package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	tagent "trpc.group/trpc-go/trpc-agent-go/agent"
	ttool "trpc.group/trpc-go/trpc-agent-go/tool"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool"
)

// DefaultApprovalTimeout is how long a user has to confirm a dangerous tool
// call (design 5.3.3: 5 minutes, tenant-configurable once tenant policies are
// wired).
const DefaultApprovalTimeout = 5 * time.Minute

// approvalKeyGrace extends the Redis key TTL beyond the deadline so a late
// answer still finds the record and gets audited as review_timeout instead of
// silently vanishing.
const approvalKeyGrace = 5 * time.Minute

// PendingApproval is a dangerous tool call waiting for in-band user
// confirmation. It is stored in Redis (key approval:{session_key}) rather
// than in framework session.state: same node-shared durability, but with a
// native TTL and readable without loading the whole session.
type PendingApproval struct {
	CallID    string          `json:"call_id"`
	ToolName  string          `json:"tool_name"`
	Arguments json.RawMessage `json:"arguments"`
	Requester string          `json:"requester"` // user_id whose message triggered the call
	Deadline  time.Time       `json:"deadline"`
}

// Signal describes what the tool-call interception did during a run, so the
// guardrail can compose the user-facing reply and audit with full message
// context (tenant_id) afterwards. Signals are in-process: the BeforeTool
// callback and the guardrail wrapping the same run always live on the same
// node, and the session lock serializes runs per session.
type Signal struct {
	Kind     string // "created" | "conflict" | "timeout"
	ToolName string // the pending tool (created/timeout) or the blocked one (conflict)
	Pending  string // for conflicts: the tool already waiting
	Args     string // argument summary for display
	Deadline time.Time
	Fresh    bool // created only: false when a redelivery re-hit the same call
}

// Approver implements the dangerous-tool approval chain of design 5.3.3:
// intercept (BeforeTool) → pending record → user answer matching (Answer) →
// release or reject. Auditing of the review/deny/review_timeout decisions is
// left to the guardrail that owns message context; the Approver reports via
// signals and returns audited decisions only in Answer.
type Approver struct {
	rdb     *redis.Client
	tools   *tool.Registry
	timeout time.Duration

	mu      sync.Mutex
	signals map[string]Signal // session_key → signal of the current run
}

// NewApprover creates an Approver. A nil rdb disables the approval store;
// dangerous calls are then blocked outright instead of pended.
func NewApprover(rdb *redis.Client, tools *tool.Registry, timeout time.Duration) *Approver {
	if timeout <= 0 {
		timeout = DefaultApprovalTimeout
	}
	return &Approver{rdb: rdb, tools: tools, timeout: timeout, signals: make(map[string]Signal)}
}

func approvalKey(sessionKey string) string { return "approval:" + sessionKey }

// blockedResult is the synthetic tool result for an intercepted call: it
// tells the model the call did not execute, so the LLM reply (used only as a
// fallback when the guardrail's deterministic confirmation is unavailable)
// does not claim success.
func blockedResult(reason string) *ttool.BeforeToolResult {
	return &ttool.BeforeToolResult{
		CustomResult: "blocked: " + reason,
	}
}

// BeforeTool is registered as a framework BeforeTool callback. Non-dangerous
// tools pass through untouched. Dangerous calls never execute here; they are
// pended (or rejected on conflict) and short-circuited with a synthetic
// result.
func (a *Approver) BeforeTool(ctx context.Context, args *ttool.BeforeToolArgs) (*ttool.BeforeToolResult, error) {
	if a == nil || args == nil || !a.tools.IsDangerous(args.ToolName) {
		return nil, nil
	}
	if a.rdb == nil {
		return blockedResult("审批存储不可用，危险操作被拒绝"), nil
	}

	inv, ok := tagent.InvocationFromContext(ctx)
	if !ok || inv == nil || inv.Session == nil {
		// Cannot scope an approval without knowing the session: block hard.
		return blockedResult("无法确认会话上下文，危险操作被拒绝"), nil
	}
	sessionKey := inv.Session.ID

	existing, err := a.pending(ctx, sessionKey)
	if err != nil {
		return blockedResult("审批存储读取失败，危险操作被拒绝"), nil
	}

	now := time.Now()
	if existing != nil && now.After(existing.Deadline) {
		// Lazy timeout detection: audit review_timeout via the signal and
		// drop the stale record. The user must re-issue the operation.
		_ = a.delete(ctx, sessionKey)
		a.setSignal(sessionKey, Signal{Kind: "timeout", ToolName: existing.ToolName})
		return blockedResult("上一个待确认操作已超时作废，请重新向用户说明并等待其再次发起"), nil
	}
	if existing != nil {
		if existing.ToolName == args.ToolName && bytes.Equal(existing.Arguments, args.Arguments) {
			// The same call re-attempted (message redelivery or model retry):
			// reuse the pending record — no duplicate review audit, the
			// confirmation message gets re-sent and deduplicated downstream.
			a.setSignal(sessionKey, Signal{
				Kind: "created", ToolName: existing.ToolName,
				Args: summarizeArgs(existing.Arguments), Deadline: existing.Deadline,
			})
			return blockedResult("该操作已在等待用户确认"), nil
		}
		// One pending approval per session at a time (design 5.3.3 rule 5):
		// reject the new call, keep the original pending.
		a.setSignal(sessionKey, Signal{
			Kind: "conflict", ToolName: args.ToolName, Pending: existing.ToolName,
		})
		return blockedResult("当前已有待确认的操作，请先完成该审批"), nil
	}

	p := PendingApproval{
		CallID:    args.ToolCallID,
		ToolName:  args.ToolName,
		Arguments: append(json.RawMessage(nil), args.Arguments...),
		Requester: inv.Session.UserID,
		Deadline:  now.Add(a.timeout),
	}
	if err := a.set(ctx, sessionKey, p); err != nil {
		return blockedResult("审批存储写入失败，危险操作被拒绝"), nil
	}
	a.setSignal(sessionKey, Signal{
		Kind: "created", ToolName: p.ToolName, Args: summarizeArgs(p.Arguments),
		Deadline: p.Deadline, Fresh: true,
	})
	return blockedResult("该操作需要用户在对话中确认后才能执行"), nil
}

// Answer checks whether msg is a confirmation/rejection of the session's
// pending approval. handled=false means the message is not an answer (or no
// approval is pending) and normal processing should continue — non-answer
// messages never disturb a pending approval (design 5.3.3 rule 3).
//
// The audit event for consumed answers is emitted synchronously by the caller
// via the returned decision; Answer itself only reports what happened.
func (a *Approver) Answer(ctx context.Context, msg channels.InboundMessage) (handled bool, out channels.OutboundMessage, dec auditDecision, err error) {
	if a == nil || a.rdb == nil {
		return false, channels.OutboundMessage{}, dec, nil
	}
	p, err := a.pending(ctx, msg.SessionKey)
	if err != nil {
		return false, channels.OutboundMessage{}, dec, fmt.Errorf("read pending approval: %w", err)
	}
	if p == nil {
		return false, channels.OutboundMessage{}, dec, nil
	}

	// Exact match only (design 5.3.3 rule 2): an answer-shaped message either
	// closes the approval or is processed as normal chat — nothing in between.
	text := strings.TrimSpace(msg.Text)
	if text != answerConfirm && text != answerReject && text != answerCancel {
		return false, channels.OutboundMessage{}, dec, nil
	}

	reply := replyShell(msg)
	// Group chats: only the original requester may confirm (design 5.3.3
	// rule 6). The pending record is left untouched.
	if msg.ChatID != "" && msg.UserID != p.Requester {
		reply.Text = "仅操作发起人可以确认或拒绝该操作。"
		return true, reply, dec, nil
	}

	if time.Now().After(p.Deadline) {
		_ = a.delete(ctx, msg.SessionKey)
		reply.Text = fmt.Sprintf("操作 %s 的确认已超时作废，如需执行请重新发起。", p.ToolName)
		return true, reply, auditDecision{decision: "review_timeout", toolName: p.ToolName}, nil
	}

	// Consume the record BEFORE any execution: a crash after this point loses
	// the confirmation rather than risking a double execution of a dangerous
	// tool.
	if err := a.delete(ctx, msg.SessionKey); err != nil {
		return true, channels.OutboundMessage{}, dec, fmt.Errorf("consume pending approval: %w", err)
	}

	if text == answerReject || text == answerCancel {
		reply.Text = fmt.Sprintf("已取消操作 %s。", p.ToolName)
		return true, reply, auditDecision{decision: "deny", errorType: "user_rejected", toolName: p.ToolName}, nil
	}

	started := time.Now()
	result, callErr := a.tools.Call(ctx, p.ToolName, p.Arguments)
	dec = auditDecision{
		decision: "allow", toolName: p.ToolName,
		latencyMs: int(time.Since(started).Milliseconds()),
	}
	if callErr != nil {
		dec.errorType = "tool_error"
		reply.Text = fmt.Sprintf("操作 %s 执行失败：%v", p.ToolName, callErr)
		return true, reply, dec, nil
	}
	reply.Text = fmt.Sprintf("✅ 操作 %s 已执行。\n结果：%s", p.ToolName, summarizeResult(result))
	return true, reply, dec, nil
}

// TakeSignal returns and clears the interception signal of the session's
// current run.
func (a *Approver) TakeSignal(sessionKey string) (Signal, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	s, ok := a.signals[sessionKey]
	delete(a.signals, sessionKey)
	return s, ok
}

func (a *Approver) setSignal(sessionKey string, s Signal) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.signals[sessionKey] = s
}

func (a *Approver) pending(ctx context.Context, sessionKey string) (*PendingApproval, error) {
	data, err := a.rdb.Get(ctx, approvalKey(sessionKey)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get approval: %w", err)
	}
	var p PendingApproval
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, fmt.Errorf("decode approval: %w", err)
	}
	return &p, nil
}

func (a *Approver) set(ctx context.Context, sessionKey string, p PendingApproval) error {
	data, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("encode approval: %w", err)
	}
	if err := a.rdb.Set(ctx, approvalKey(sessionKey), data, a.timeout+approvalKeyGrace).Err(); err != nil {
		return fmt.Errorf("set approval: %w", err)
	}
	return nil
}

func (a *Approver) delete(ctx context.Context, sessionKey string) error {
	if err := a.rdb.Del(ctx, approvalKey(sessionKey)).Err(); err != nil {
		return fmt.Errorf("delete approval: %w", err)
	}
	return nil
}

// Answer texts the user may send to close a pending approval (exact match
// after the channel normalized the text; design 5.3.3 rule 2).
const (
	answerConfirm = "确认"
	answerReject  = "拒绝"
	answerCancel  = "取消"
)

// replyShell builds an OutboundMessage routed back to the sender of msg.
func replyShell(msg channels.InboundMessage) channels.OutboundMessage {
	return channels.OutboundMessage{
		Channel:    msg.Channel,
		MsgID:      msg.MsgID,
		SessionKey: msg.SessionKey,
		UserID:     msg.UserID,
		ChatID:     msg.ChatID,
		TenantID:   msg.TenantID,
		TraceID:    msg.TraceID,
	}
}

// summarizeArgs caps the argument summary so the confirmation message stays
// well under IM length limits.
func summarizeArgs(args json.RawMessage) string {
	const max = 200
	s := string(args)
	if len(s) > max {
		s = s[:max] + "…"
	}
	return s
}

func summarizeResult(result any) string {
	const max = 500
	data, err := json.Marshal(result)
	if err != nil {
		return fmt.Sprintf("%v", result)
	}
	if len(data) > max {
		return string(data[:max]) + "…"
	}
	return string(data)
}
