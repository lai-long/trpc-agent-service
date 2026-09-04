package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/model/openai"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/tool"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
)

// DefaultRunTimeout bounds one model run (design 5.2.2: 60s).
const DefaultRunTimeout = 60 * time.Second

// ModelError marks a failure of the model call itself (timeout, LLM API
// error, empty response), as opposed to a platform infrastructure failure
// (session store, queue). The guardrail degrades a ModelError to a busy
// reply; any other error keeps the message pending for redelivery (5.2.2).
type ModelError struct{ Err error }

func (e *ModelError) Error() string { return e.Err.Error() }
func (e *ModelError) Unwrap() error { return e.Err }

// RunnerProcessor processes messages with the tRPC-Agent-Go runner.Runner.
// Session persistence is delegated to the framework's session.Service.
type RunnerProcessor struct {
	runner  runner.Runner
	model   string
	timeout time.Duration
	retries int
}

// RunnerConfig is the minimal parameter set for assembling a Runner.
// APIKey must be resolved via a SecretResolver before being passed in; this
// package never touches refs or files.
type RunnerConfig struct {
	AppName        string // Runner app name, the first segment of the framework session.Key
	BaseURL        string // OpenAI-compatible endpoint
	APIKey         string // resolved model key in plaintext (never log it)
	ModelName      string
	Instruction    string   // system prompt from agent_app.config
	Temperature    *float64 // nil means the model default
	SessionService session.Service
	// Timeout bounds one run and Retries is the number of re-attempts after a
	// failed run (design 5.2.2: 60s deadline, retry once, then degrade).
	// Zero values default to 60s / 1.
	Timeout time.Duration
	Retries int
	// Tools and ToolCallbacks wire the platform tool registry and the
	// guardrail's dangerous-tool interception into the agent.
	Tools         []tool.Tool
	ToolCallbacks *tool.Callbacks
	// MemoryService, when set, is injected into invocations (the memory tools
	// resolve it from context) and its tool set joins the agent's tools.
	MemoryService memory.Service
	// Knowledge, when set, gives the agent a knowledge search tool scoped by
	// KnowledgeFilter (tenant/app metadata isolation).
	Knowledge       knowledge.Knowledge
	KnowledgeFilter map[string]any
	// ArtifactService, when set, is wired onto the runner for artifact
	// storage (design: Artifact = S3).
	ArtifactService artifact.Service
}

// NewRunnerProcessor assembles llmagent + runner with non-streaming
// generation: the full reply is returned in one piece.
func NewRunnerProcessor(cfg RunnerConfig) *RunnerProcessor {
	m := openai.New(cfg.ModelName,
		openai.WithBaseURL(cfg.BaseURL),
		openai.WithAPIKey(cfg.APIKey),
	)
	tools := cfg.Tools
	if cfg.MemoryService != nil {
		tools = append(append([]tool.Tool{}, tools...), cfg.MemoryService.Tools()...)
	}
	genConfig := model.GenerationConfig{Stream: false, Temperature: cfg.Temperature}
	opts := []llmagent.Option{
		llmagent.WithModel(m),
		llmagent.WithGenerationConfig(genConfig),
		// Prepend the session summary when one exists (PG backend compresses
		// old events into the summary table); a no-op for sessions without.
		llmagent.WithAddSessionSummary(true),
	}
	if cfg.Instruction != "" {
		opts = append(opts, llmagent.WithInstruction(cfg.Instruction))
	}
	if len(tools) > 0 {
		opts = append(opts, llmagent.WithTools(tools))
	}
	if cfg.ToolCallbacks != nil {
		opts = append(opts, llmagent.WithToolCallbacks(cfg.ToolCallbacks))
	}
	if cfg.Knowledge != nil {
		opts = append(opts, llmagent.WithKnowledge(cfg.Knowledge))
		if len(cfg.KnowledgeFilter) > 0 {
			opts = append(opts, llmagent.WithKnowledgeFilter(cfg.KnowledgeFilter))
		}
	}
	llm := llmagent.New("assistant", opts...)
	runnerOpts := []runner.Option{runner.WithSessionService(cfg.SessionService)}
	if cfg.MemoryService != nil {
		runnerOpts = append(runnerOpts, runner.WithMemoryService(cfg.MemoryService))
	}
	if cfg.ArtifactService != nil {
		runnerOpts = append(runnerOpts, runner.WithArtifactService(cfg.ArtifactService))
	}
	r := runner.NewRunner(cfg.AppName, llm, runnerOpts...)
	return newRunnerProcessor(r, cfg.ModelName, cfg.Timeout, cfg.Retries)
}

// newRunnerProcessor wraps an existing runner; tests use it to inject fakes.
func newRunnerProcessor(r runner.Runner, modelName string, timeout time.Duration, retries int) *RunnerProcessor {
	if timeout <= 0 {
		timeout = DefaultRunTimeout
	}
	if retries < 0 {
		retries = 0
	}
	return &RunnerProcessor{runner: r, model: modelName, timeout: timeout, retries: retries}
}

// Process implements Processor: run with a per-run deadline, retrying a
// failed run up to cfg.Retries times (design 5.2.2: retry once, then the
// guardrail degrades to a busy reply). Model-side failures come back as
// *ModelError; infrastructure failures (runner.Run itself refusing the run)
// are returned raw so the worker leaves the message pending for redelivery.
//
// The event channel must be consumed until closed, otherwise framework-side
// goroutines block and leak.
func (p *RunnerProcessor) Process(ctx context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
	out := channels.OutboundMessage{
		Channel:    msg.Channel,
		MsgID:      msg.MsgID,
		SessionKey: msg.SessionKey,
		UserID:     msg.UserID,
		ChatID:     msg.ChatID,
		BindingID:  msg.BindingID,
		TenantID:   msg.TenantID,
		TraceID:    msg.TraceID,
		Model:      p.model,
		ReceivedAt: msg.ReceivedAt,
	}

	var lastErr error
	for attempt := 0; attempt <= p.retries; attempt++ {
		if attempt > 0 {
			plog.Warnf("retrying run (attempt %d/%d, session=%s): %v",
				attempt+1, p.retries+1, msg.SessionKey, lastErr)
		}
		reply, usage, err := p.runOnce(ctx, msg)
		out.PromptTokens += usage.prompt
		out.CompletionTokens += usage.completion
		if err == nil {
			out.Text = reply
			if channels.LooksMarkdown(reply) {
				out.TextType = channels.TextTypeMarkdown
			}
			zap.L().Debug("runner replied",
				zap.String(plog.FieldSessionKey, msg.SessionKey),
				zap.String(plog.FieldTraceID, msg.TraceID),
				zap.Int("reply_len", len(out.Text)))
			return out, nil
		}
		lastErr = err
		var infra *infraError
		if errors.As(err, &infra) {
			return out, infra.Err
		}
		if ctx.Err() != nil {
			break // shutting down: no further attempts
		}
	}
	return out, &ModelError{Err: lastErr}
}

// infraError wraps failures of runner.Run itself (session store down, etc.)
// to keep them off the ModelError degradation path.
type infraError struct{ Err error }

func (e *infraError) Error() string { return e.Err.Error() }
func (e *infraError) Unwrap() error { return e.Err }

// tokenUsage accumulates the LLM token usage observed on the event stream.
type tokenUsage struct{ prompt, completion int }

// runOnce executes a single run under a fresh deadline and drains the event
// channel to close (also after cancellation — the framework shuts its
// goroutines down on ctx cancel and then closes the channel).
func (p *RunnerProcessor) runOnce(ctx context.Context, msg channels.InboundMessage) (string, tokenUsage, error) {
	var usage tokenUsage
	runCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	events, err := p.runner.Run(runCtx, msg.UserID, msg.SessionKey, model.NewUserMessage(msg.Text))
	if err != nil {
		return "", usage, &infraError{Err: fmt.Errorf("runner run: %w", err)}
	}

	var reply strings.Builder
	var runErr error
	for evt := range events { // ranging until close satisfies the drain requirement
		if evt.Error != nil {
			plog.Warnf("runner event error: %s", evt.Error.Message)
			runErr = errors.New(evt.Error.Message)
			continue
		}
		if evt.Response != nil && evt.Response.Usage != nil {
			usage.prompt += evt.Response.Usage.PromptTokens
			usage.completion += evt.Response.Usage.CompletionTokens
			metrics.TokensTotal.Add(ctx, int64(evt.Response.Usage.PromptTokens), tokenAttr(msg.TenantID, "prompt"))
			metrics.TokensTotal.Add(ctx, int64(evt.Response.Usage.CompletionTokens), tokenAttr(msg.TenantID, "completion"))
		}
		if evt.IsFinalResponse() && evt.Response != nil && len(evt.Response.Choices) > 0 {
			reply.WriteString(evt.Response.Choices[0].Message.Content)
		}
	}
	if reply.Len() > 0 {
		return reply.String(), usage, nil
	}
	if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
		return "", usage, fmt.Errorf("model timeout after %s: %w", p.timeout, context.DeadlineExceeded)
	}
	if runErr != nil {
		return "", usage, runErr
	}
	return "", usage, errors.New("runner produced no final response")
}

// Close shuts down the underlying runner (call at process exit).
func (p *RunnerProcessor) Close() error { return p.runner.Close() }

// tokenAttr tags token usage by tenant and kind (prompt / completion).
func tokenAttr(tenantID, kind string) otelmetric.MeasurementOption {
	return otelmetric.WithAttributes(
		attribute.String("tenant_id", tenantID),
		attribute.String("kind", kind),
	)
}
