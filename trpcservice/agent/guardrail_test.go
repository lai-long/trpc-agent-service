package agent

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	ttool "trpc.group/trpc-go/trpc-agent-go/tool"
	"trpc.group/trpc-go/trpc-agent-go/tool/function"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool"
)

// fakeAuditor captures audit events without PG.
type fakeAuditor struct {
	mu     sync.Mutex
	async  []storage.AuditEvent
	synced []storage.AuditEvent
}

func (f *fakeAuditor) LogAsync(ev storage.AuditEvent) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.async = append(f.async, ev)
}

func (f *fakeAuditor) LogSync(_ context.Context, ev storage.AuditEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.synced = append(f.synced, ev)
	return nil
}

func (f *fakeAuditor) syncDecisions() []storage.AuditEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]storage.AuditEvent(nil), f.synced...)
}

func (f *fakeAuditor) asyncDecisions() []storage.AuditEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]storage.AuditEvent(nil), f.async...)
}

func testMsg(text string) channels.InboundMessage {
	return channels.InboundMessage{
		Channel: "mock", MsgID: "m1", SessionKey: "dm:mock:u1", UserID: "u1",
		Text: text, TenantID: "t1", TraceID: "trace-1",
	}
}

// testRegistry builds two dangerous tools (op_a / op_b) and one safe tool.
func testRegistry() *tool.Registry {
	mk := func(name string) ttool.Tool {
		return function.NewFunctionTool(
			func(_ context.Context, in struct {
				X string `json:"x"`
			}) (map[string]string, error) {
				return map[string]string{"tool": name, "x": in.X}, nil
			},
			function.WithName(name), function.WithDescription("test tool "+name))
	}
	return tool.NewRegistry(
		tool.Tool{Tool: mk("op_a"), Dangerous: true},
		tool.Tool{Tool: mk("op_b"), Dangerous: true},
		tool.Tool{Tool: mk("safe")},
	)
}

func TestGuardedInputDeny(t *testing.T) {
	aud := &fakeAuditor{}
	g := &Guarded{
		Inner:   EchoProcessor{},
		Auditor: aud,
		Input:   []InputChecker{SensitiveWordInput([]string{"赌博"})},
	}
	out, err := g.Process(context.Background(), testMsg("来赌博吧"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.Text, "受限内容") {
		t.Fatalf("want denial reply, got %q", out.Text)
	}
	evs := aud.syncDecisions()
	if len(evs) != 1 || evs[0].Decision != "deny" || evs[0].ErrorType != "sensitive_input" {
		t.Fatalf("want sync deny/sensitive_input, got %+v", evs)
	}
	if evs[0].TenantID != "t1" {
		t.Fatalf("tenant not propagated to audit: %+v", evs[0])
	}
	if n := len(aud.asyncDecisions()); n != 0 {
		t.Fatalf("denied message must not get an allow event, got %d", n)
	}
}

func TestGuardedOutputRedact(t *testing.T) {
	g := &Guarded{
		Inner:  EchoProcessor{},
		Output: []OutputChecker{RedactOutput()},
	}
	out, err := g.Process(context.Background(), testMsg("打我电话 13800138000"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.Text, "13800138000") {
		t.Fatalf("phone number not redacted: %q", out.Text)
	}
}

func TestGuardedPassthroughAuditsAllow(t *testing.T) {
	aud := &fakeAuditor{}
	g := &Guarded{Inner: EchoProcessor{}, Auditor: aud}
	out, err := g.Process(context.Background(), testMsg("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if out.Text != "echo: hello" {
		t.Fatalf("unexpected reply: %q", out.Text)
	}
	evs := aud.asyncDecisions()
	if len(evs) != 1 || evs[0].Decision != "allow" || evs[0].TenantID != "t1" {
		t.Fatalf("want async allow with tenant, got %+v", evs)
	}
}

func TestGuardedSignalReplacesReply(t *testing.T) {
	aud := &fakeAuditor{}
	ap := NewApprover(nil, testRegistry(), 0) // nil rdb: store disabled, signals work
	ap.setSignal("dm:mock:u1", Signal{
		Kind: "created", ToolName: "op_a", Args: `{"x":"1"}`,
		Deadline: time.Now().Add(5 * time.Minute), Fresh: true,
	})
	g := &Guarded{Inner: EchoProcessor{}, Approver: ap, Auditor: aud}
	out, err := g.Process(context.Background(), testMsg("执行操作"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.Text, "危险操作") || !strings.Contains(out.Text, "op_a") {
		t.Fatalf("want confirmation notice, got %q", out.Text)
	}
	evs := aud.syncDecisions()
	if len(evs) != 1 || evs[0].Decision != "review" || evs[0].ToolName != "op_a" {
		t.Fatalf("want sync review for op_a, got %+v", evs)
	}
}

// failProcessor always fails with the given error.
type failProcessor struct{ err error }

func (f failProcessor) Process(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
	return channels.OutboundMessage{}, f.err
}

func TestGuardedModelTimeoutDegradesToBusyReply(t *testing.T) {
	aud := &fakeAuditor{}
	g := &Guarded{
		Inner:   failProcessor{err: &ModelError{Err: context.DeadlineExceeded}},
		Auditor: aud,
	}
	out, err := g.Process(context.Background(), testMsg("hello"))
	if err != nil {
		t.Fatalf("model failures degrade to a reply, got err %v", err)
	}
	if out.Text != degradedReply {
		t.Fatalf("want busy reply, got %q", out.Text)
	}
	// The degraded reply must keep the routing fields for the sender.
	if out.SessionKey != "dm:mock:u1" || out.UserID != "u1" {
		t.Fatalf("routing fields lost: %+v", out)
	}
	evs := aud.asyncDecisions()
	if len(evs) != 1 || evs[0].Decision != "allow" || evs[0].ErrorType != "model_timeout" {
		t.Fatalf("want async allow/model_timeout, got %+v", evs)
	}
}

func TestGuardedModelErrorDegradesToBusyReply(t *testing.T) {
	aud := &fakeAuditor{}
	g := &Guarded{
		Inner:   failProcessor{err: &ModelError{Err: errors.New("boom")}},
		Auditor: aud,
	}
	out, err := g.Process(context.Background(), testMsg("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if out.Text != degradedReply {
		t.Fatalf("want busy reply, got %q", out.Text)
	}
	evs := aud.asyncDecisions()
	if len(evs) != 1 || evs[0].ErrorType != "model_error" {
		t.Fatalf("want model_error audit, got %+v", evs)
	}
}

func TestGuardedInfraErrorPropagates(t *testing.T) {
	// Non-model failures keep the message pending for redelivery: the error
	// propagates to the worker, which does not ack (design 5.2.2).
	aud := &fakeAuditor{}
	g := &Guarded{
		Inner:   failProcessor{err: errors.New("pg down")},
		Auditor: aud,
	}
	_, err := g.Process(context.Background(), testMsg("hello"))
	if err == nil {
		t.Fatal("infra errors must propagate")
	}
	evs := aud.asyncDecisions()
	if len(evs) != 1 || evs[0].ErrorType != "process_error" {
		t.Fatalf("want process_error audit, got %+v", evs)
	}
}

func TestGuardedModelErrorWithPendingSignalDeliversConfirmation(t *testing.T) {
	// A dangerous call was intercepted during the failed run: the pending
	// approval is real, so the confirmation notice wins over the busy reply.
	aud := &fakeAuditor{}
	ap := NewApprover(nil, testRegistry(), 0)
	ap.setSignal("dm:mock:u1", Signal{
		Kind: "created", ToolName: "op_a", Args: `{"x":"1"}`,
		Deadline: time.Now().Add(5 * time.Minute), Fresh: true,
	})
	g := &Guarded{
		Inner:    failProcessor{err: &ModelError{Err: errors.New("boom")}},
		Approver: ap,
		Auditor:  aud,
	}
	out, err := g.Process(context.Background(), testMsg("执行操作"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.Text, "危险操作") || !strings.Contains(out.Text, "op_a") {
		t.Fatalf("want confirmation notice, got %q", out.Text)
	}
	evs := aud.syncDecisions()
	if len(evs) != 1 || evs[0].Decision != "review" {
		t.Fatalf("want sync review, got %+v", evs)
	}
}

// usageProcessor returns a fixed reply with token usage, as RunnerProcessor
// does after a real run.
type usageProcessor struct{}

func (usageProcessor) Process(_ context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
	out := channels.OutboundMessage{
		Channel: msg.Channel, MsgID: msg.MsgID, SessionKey: msg.SessionKey,
		UserID: msg.UserID, ChatID: msg.ChatID, TenantID: msg.TenantID,
		Text: "ok", PromptTokens: 1000, CompletionTokens: 500, Model: "m-test",
	}
	return out, nil
}

func TestGuardedAuditCarriesUsageAndCost(t *testing.T) {
	SetModelPricing(map[string][2]float64{"m-test": {1.0, 2.0}}) // $1/$2 per 1M tokens
	t.Cleanup(func() { SetModelPricing(nil) })

	aud := &fakeAuditor{}
	g := &Guarded{Inner: usageProcessor{}, Auditor: aud}
	if _, err := g.Process(context.Background(), testMsg("hello")); err != nil {
		t.Fatal(err)
	}
	evs := aud.asyncDecisions()
	if len(evs) != 1 {
		t.Fatalf("want 1 audit event, got %d", len(evs))
	}
	ev := evs[0]
	if ev.PromptTokens != 1000 || ev.CompletionTokens != 500 {
		t.Fatalf("token usage missing from audit: %+v", ev)
	}
	// 1000*$1 + 500*$2 per 1M = $0.002
	if ev.Cost < 0.0019 || ev.Cost > 0.0021 {
		t.Fatalf("cost miscalculated: %+v", ev)
	}
}

// A recall event is audited (sync, with the recalled message id in detail)
// and marked in session state; it never reaches the inner processor and the
// reply is empty (the worker acks without an outbound hop).
func TestGuardedRecall(t *testing.T) {
	aud := &fakeAuditor{}
	marked := map[string][]byte{}
	g := &Guarded{
		Inner:   EchoProcessor{},
		Auditor: aud,
		StateMark: func(_ context.Context, msg channels.InboundMessage, key string, value []byte) error {
			marked[key] = value
			return nil
		},
	}
	msg := testMsg("")
	msg.Type = channels.TypeRecall
	msg.MsgID = "recall:m-42"
	out, err := g.Process(context.Background(), msg)
	if err != nil {
		t.Fatal(err)
	}
	if out.Text != "" {
		t.Fatalf("recall must not produce a reply, got %q", out.Text)
	}
	evs := aud.syncDecisions()
	if len(evs) != 1 || evs[0].Decision != "recall" {
		t.Fatalf("want sync recall audit, got %+v", evs)
	}
	if !strings.Contains(string(evs[0].Detail), "m-42") {
		t.Fatalf("audit detail must carry the recalled message id: %s", evs[0].Detail)
	}
	if _, ok := marked["recalled:m-42"]; !ok {
		t.Fatalf("session state not marked: %v", marked)
	}
	if n := len(aud.asyncDecisions()); n != 0 {
		t.Fatalf("recall must not get a routine allow event, got %d", n)
	}
}
