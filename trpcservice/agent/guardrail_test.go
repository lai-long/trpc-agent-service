package agent

import (
	"context"
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
