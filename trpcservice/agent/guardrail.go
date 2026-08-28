package agent

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

// auditLogger is the audit sink used by the guardrail (*storage.Auditor
// satisfies it). A small interface keeps the guardrail testable without PG.
type auditLogger interface {
	LogAsync(ev storage.AuditEvent)
	LogSync(ctx context.Context, ev storage.AuditEvent) error
}

// auditDecision describes a guardrail decision to audit for one message. An
// empty decision means "nothing to audit" (e.g. a non-requester's answer
// attempt in a group chat).
type auditDecision struct {
	decision  string // allow / deny / review / review_timeout
	errorType string
	toolName  string
	latencyMs int
}

// InputChecker screens an inbound message; denied=true blocks processing.
type InputChecker func(ctx context.Context, msg channels.InboundMessage) (reason string, denied bool)

// OutputChecker post-processes a reply (desensitization), returning the text
// to send.
type OutputChecker func(ctx context.Context, msg channels.InboundMessage, reply string) string

// DefaultBlockedWords is the demo input denylist. Tenant-level lists arrive
// with tenant.tool_policy once the Admin API lands.
var DefaultBlockedWords = []string{"赌博", "毒品", "枪支"}

// Guarded wraps a Processor with the guardrail chain (design 4.3):
//
//	approval answer → input checks → inner processor → output checks
//	→ approval confirmation composition
//
// The guardrail owns message-level auditing: routine messages get an async
// allow event, guardrail decisions (deny / review / review_timeout /
// dangerous-tool execution) are written synchronously — the compliance red
// line is that critical decisions are never lost.
type Guarded struct {
	Inner    Processor
	Approver *Approver   // nil disables tool-approval handling
	Auditor  auditLogger // nil disables auditing
	Input    []InputChecker
	Output   []OutputChecker
}

// Process implements Processor.
func (g *Guarded) Process(ctx context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
	// 1. A pending approval consumes confirm/reject answers before anything
	//    else runs.
	if g.Approver != nil {
		handled, out, dec, err := g.Approver.Answer(ctx, msg)
		if err != nil {
			return channels.OutboundMessage{}, err
		}
		if handled {
			g.syncAudit(msg, dec)
			return out, nil
		}
	}

	// 2. Input checks.
	for _, check := range g.Input {
		reason, denied := check(ctx, msg)
		if denied {
			plog.Warnf("input denied (session=%s): %s", msg.SessionKey, reason)
			g.syncAudit(msg, auditDecision{decision: "deny", errorType: "sensitive_input"})
			out := replyShell(msg)
			out.Text = "抱歉，您的消息包含受限内容，已被拦截。"
			return out, nil
		}
	}

	// 3. Inner processing (Runner or echo fallback). Routine allow audit,
	//    async lane; process errors ride the same event as error_type.
	started := time.Now()
	out, err := g.Inner.Process(ctx, msg)
	g.asyncAudit(msg, started, err)

	// Drain the interception signal even on error: a redelivery regenerates
	// it, and a stale signal must not leak into an unrelated run.
	var sig Signal
	var signaled bool
	if g.Approver != nil {
		sig, signaled = g.Approver.TakeSignal(msg.SessionKey)
	}
	if err != nil {
		return out, err
	}

	// 4. Output checks (desensitization).
	for _, check := range g.Output {
		out.Text = check(ctx, msg, out.Text)
	}

	// 5. A dangerous call was intercepted during the run: audit the decision
	//    with full tenant context and replace the LLM reply with the
	//    deterministic confirmation/conflict/timeout notice.
	if signaled {
		g.syncAudit(msg, signalDecision(sig))
		out.Text = signalReply(sig)
	}
	return out, nil
}

// SensitiveWordInput denies messages containing any of the words.
func SensitiveWordInput(words []string) InputChecker {
	return func(_ context.Context, msg channels.InboundMessage) (string, bool) {
		for _, w := range words {
			if w != "" && strings.Contains(msg.Text, w) {
				return w, true
			}
		}
		return "", false
	}
}

// redactRules are the default output desensitization patterns: phone numbers,
// ID card numbers and email addresses are masked before a reply leaves the
// platform.
var redactRules = []*regexp.Regexp{
	regexp.MustCompile(`1[3-9]\d{9}`),             // 手机号
	regexp.MustCompile(`\d{17}[\dXx]`),            // 身份证号
	regexp.MustCompile(`[\w.+-]+@[\w-]+\.[\w.]+`), // 邮箱
}

// RedactOutput masks sensitive patterns (phone / ID card / email) in replies.
func RedactOutput() OutputChecker {
	return func(_ context.Context, msg channels.InboundMessage, reply string) string {
		masked := reply
		for _, re := range redactRules {
			masked = re.ReplaceAllString(masked, "***")
		}
		if masked != reply {
			plog.Debugf("output redacted (session=%s)", msg.SessionKey)
		}
		return masked
	}
}

// signalDecision maps an interception signal to its audit event. Re-hitting
// an already-pending call (Fresh=false) audits nothing: the review was
// recorded when the pending was first created.
func signalDecision(sig Signal) auditDecision {
	switch sig.Kind {
	case "created":
		if !sig.Fresh {
			return auditDecision{}
		}
		return auditDecision{decision: "review", toolName: sig.ToolName}
	case "conflict":
		return auditDecision{decision: "deny", errorType: "approval_conflict", toolName: sig.ToolName}
	case "timeout":
		return auditDecision{decision: "review_timeout", toolName: sig.ToolName}
	}
	return auditDecision{}
}

// signalReply composes the deterministic user-facing notice for an
// interception, replacing whatever the LLM said.
func signalReply(sig Signal) string {
	switch sig.Kind {
	case "created":
		remaining := time.Until(sig.Deadline).Round(time.Second)
		return fmt.Sprintf("⚠️ 检测到危险操作，待您确认：\n• 工具：%s\n• 参数：%s\n请在 %s 内回复「确认」执行，或回复「拒绝」取消。",
			sig.ToolName, sig.Args, remaining)
	case "conflict":
		return fmt.Sprintf("当前已有待确认的操作（工具：%s），请先回复「确认」或「拒绝」完成该审批，再发起新的危险操作。", sig.Pending)
	case "timeout":
		return fmt.Sprintf("之前待确认的操作（工具：%s）已超时作废，如需执行请重新发起。", sig.ToolName)
	}
	return ""
}

// asyncAudit records the routine (allow) decision for a processed message,
// off the request path. Messages that bypassed tenant routing (dev fallback)
// are filed under the zero UUID.
func (g *Guarded) asyncAudit(msg channels.InboundMessage, started time.Time, processErr error) {
	if g.Auditor == nil {
		return
	}
	ev := storage.AuditEvent{
		TenantID:  tenantOrZero(msg.TenantID),
		Channel:   msg.Channel,
		UserID:    msg.UserID,
		Decision:  "allow",
		LatencyMs: int(time.Since(started).Milliseconds()),
		TraceID:   msg.TraceID,
	}
	if processErr != nil {
		ev.ErrorType = "process_error"
	}
	g.Auditor.LogAsync(ev)
}

// syncAudit writes a critical guardrail decision synchronously: deny /
// review / review_timeout / dangerous-tool execution must not be lost, even
// at the cost of milliseconds of latency (compliance red line).
func (g *Guarded) syncAudit(msg channels.InboundMessage, dec auditDecision) {
	if g.Auditor == nil || dec.decision == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := g.Auditor.LogSync(ctx, storage.AuditEvent{
		TenantID:  tenantOrZero(msg.TenantID),
		Channel:   msg.Channel,
		UserID:    msg.UserID,
		ToolName:  dec.toolName,
		Decision:  dec.decision,
		ErrorType: dec.errorType,
		LatencyMs: dec.latencyMs,
		TraceID:   msg.TraceID,
	})
	if err != nil {
		plog.Errorf("sync audit %s failed: %v", dec.decision, err)
	}
}

func tenantOrZero(tenantID string) string {
	if tenantID == "" {
		return "00000000-0000-0000-0000-000000000000"
	}
	return tenantID
}
