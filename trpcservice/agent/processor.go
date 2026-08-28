package agent

import (
	"context"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/model/openai"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
)

// RunnerProcessor processes messages with the tRPC-Agent-Go runner.Runner.
// Session persistence is delegated to the framework's session.Service.
type RunnerProcessor struct {
	runner runner.Runner
}

// RunnerConfig is the minimal parameter set for assembling a Runner.
// APIKey must be resolved via a SecretResolver before being passed in; this
// package never touches refs or files.
type RunnerConfig struct {
	AppName        string // Runner app name, the first segment of the framework session.Key
	BaseURL        string // OpenAI-compatible endpoint
	APIKey         string // resolved model key in plaintext (never log it)
	ModelName      string
	SessionService session.Service
}

// NewRunnerProcessor assembles llmagent + runner with non-streaming
// generation: the full reply is returned in one piece.
func NewRunnerProcessor(cfg RunnerConfig) *RunnerProcessor {
	m := openai.New(cfg.ModelName,
		openai.WithBaseURL(cfg.BaseURL),
		openai.WithAPIKey(cfg.APIKey),
	)
	llm := llmagent.New("assistant",
		llmagent.WithModel(m),
		llmagent.WithGenerationConfig(model.GenerationConfig{Stream: false}),
	)
	r := runner.NewRunner(cfg.AppName, llm, runner.WithSessionService(cfg.SessionService))
	return &RunnerProcessor{runner: r}
}

// Process implements Processor.
// The event channel must be consumed until closed, otherwise framework-side
// goroutines block and leak.
func (p *RunnerProcessor) Process(ctx context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
	out := channels.OutboundMessage{
		Channel:    msg.Channel,
		MsgID:      msg.MsgID,
		SessionKey: msg.SessionKey,
		UserID:     msg.UserID,
		ChatID:     msg.ChatID,
		TenantID:   msg.TenantID,
		TraceID:    msg.TraceID,
	}

	events, err := p.runner.Run(ctx, msg.UserID, msg.SessionKey, model.NewUserMessage(msg.Text))
	if err != nil {
		return out, fmt.Errorf("runner run: %w", err)
	}

	var reply strings.Builder
	for evt := range events { // ranging until close satisfies the drain requirement
		if evt.Error != nil {
			plog.Warnf("runner event error: %s", evt.Error.Message)
			continue
		}
		if evt.Response != nil && evt.Response.Usage != nil {
			metrics.TokensTotal.Add(ctx, int64(evt.Response.Usage.PromptTokens), tokenAttr("prompt"))
			metrics.TokensTotal.Add(ctx, int64(evt.Response.Usage.CompletionTokens), tokenAttr("completion"))
		}
		if evt.IsFinalResponse() && evt.Response != nil && len(evt.Response.Choices) > 0 {
			reply.WriteString(evt.Response.Choices[0].Message.Content)
		}
	}
	if reply.Len() == 0 {
		return out, fmt.Errorf("runner produced no final response")
	}

	out.Text = reply.String()
	zap.L().Debug("runner replied",
		zap.String(plog.FieldSessionKey, msg.SessionKey),
		zap.String(plog.FieldTraceID, msg.TraceID),
		zap.Int("reply_len", len(out.Text)))
	return out, nil
}

// Close shuts down the underlying runner (call at process exit).
func (p *RunnerProcessor) Close() error { return p.runner.Close() }

// tokenAttr tags token usage by kind (prompt / completion).
func tokenAttr(kind string) otelmetric.MeasurementOption {
	return otelmetric.WithAttributes(attribute.String("kind", kind))
}
