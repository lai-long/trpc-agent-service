package agent

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	tagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/session"
	ttool "trpc.group/trpc-go/trpc-agent-go/tool"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

// approverForTest connects to the compose Redis (localhost:6380) and returns
// an Approver plus cleanup; skips the test when Redis is unreachable.
func approverForTest(t *testing.T) (*Approver, *redis.Client) {
	t.Helper()
	ctx := context.Background()
	rdb, err := storage.NewRedis(ctx, "localhost:6380")
	if err != nil {
		t.Skipf("redis unavailable (%v), skipping integration test", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return NewApprover(rdb, testRegistry(), time.Minute), rdb
}

func invocationCtx(sessionKey, userID string) context.Context {
	inv := &tagent.Invocation{Session: &session.Session{ID: sessionKey, UserID: userID}}
	return tagent.NewInvocationContext(context.Background(), inv)
}

func toolArgs(callID, toolName, jsonArgs string) *ttool.BeforeToolArgs {
	return &ttool.BeforeToolArgs{
		ToolCallID: callID,
		ToolName:   toolName,
		Arguments:  []byte(jsonArgs),
	}
}

func TestBeforeToolPendsDangerousCall(t *testing.T) {
	ap, rdb := approverForTest(t)
	sessionKey := "test:approval:" + t.Name()
	t.Cleanup(func() { rdb.Del(context.Background(), approvalKey(sessionKey)) })

	// Safe tools pass through untouched.
	res, err := ap.BeforeTool(invocationCtx(sessionKey, "u1"), toolArgs("c0", "safe", `{}`))
	if err != nil || res != nil {
		t.Fatalf("safe tool must pass through, got res=%v err=%v", res, err)
	}

	// A dangerous call is pended and short-circuited, not executed.
	res, err = ap.BeforeTool(invocationCtx(sessionKey, "u1"), toolArgs("c1", "op_a", `{"x":"1"}`))
	if err != nil {
		t.Fatal(err)
	}
	if res == nil || res.CustomResult == nil {
		t.Fatal("dangerous call must be short-circuited with a synthetic result")
	}
	p, err := ap.pending(context.Background(), sessionKey)
	if err != nil || p == nil {
		t.Fatalf("pending approval not stored: %v %+v", err, p)
	}
	if p.ToolName != "op_a" || p.Requester != "u1" || p.CallID != "c1" {
		t.Fatalf("unexpected pending: %+v", p)
	}
	sig, ok := ap.TakeSignal(sessionKey)
	if !ok || sig.Kind != "created" || !sig.Fresh {
		t.Fatalf("want fresh created signal, got %+v (ok=%v)", sig, ok)
	}

	// Redelivery re-hitting the SAME call reuses the pending record without a
	// fresh signal (no duplicate review audit downstream).
	res, err = ap.BeforeTool(invocationCtx(sessionKey, "u1"), toolArgs("c2", "op_a", `{"x":"1"}`))
	if err != nil || res == nil {
		t.Fatalf("re-attempt must be blocked, got res=%v err=%v", res, err)
	}
	sig, ok = ap.TakeSignal(sessionKey)
	if !ok || sig.Kind != "created" || sig.Fresh {
		t.Fatalf("want non-fresh created signal for re-attempt, got %+v", sig)
	}

	// A DIFFERENT dangerous call while one is pending conflicts (rule 5).
	res, err = ap.BeforeTool(invocationCtx(sessionKey, "u1"), toolArgs("c3", "op_b", `{"x":"2"}`))
	if err != nil || res == nil {
		t.Fatalf("conflicting call must be blocked, got res=%v err=%v", res, err)
	}
	sig, ok = ap.TakeSignal(sessionKey)
	if !ok || sig.Kind != "conflict" || sig.Pending != "op_a" {
		t.Fatalf("want conflict signal naming the pending tool, got %+v", sig)
	}
}

func TestAnswerConfirmExecutesTool(t *testing.T) {
	ap, rdb := approverForTest(t)
	sessionKey := "test:approval:" + t.Name()
	t.Cleanup(func() { rdb.Del(context.Background(), approvalKey(sessionKey)) })
	ctx := context.Background()

	if _, err := ap.BeforeTool(invocationCtx(sessionKey, "u1"), toolArgs("c1", "op_a", `{"x":"1"}`)); err != nil {
		t.Fatal(err)
	}

	msg := testMsg("确认")
	msg.SessionKey = sessionKey
	handled, out, dec, err := ap.Answer(ctx, msg)
	if err != nil || !handled {
		t.Fatalf("confirm must be handled, got handled=%v err=%v", handled, err)
	}
	if dec.decision != "allow" || dec.toolName != "op_a" {
		t.Fatalf("want allow/op_a decision, got %+v", dec)
	}
	if !strings.Contains(out.Text, "已执行") || !strings.Contains(out.Text, "op_a") {
		t.Fatalf("want execution result, got %q", out.Text)
	}
	// Pending consumed: a second confirm is not an answer anymore.
	handled, _, _, err = ap.Answer(ctx, msg)
	if err != nil || handled {
		t.Fatalf("second confirm must not be handled, got handled=%v err=%v", handled, err)
	}
}

func TestAnswerReject(t *testing.T) {
	ap, rdb := approverForTest(t)
	sessionKey := "test:approval:" + t.Name()
	t.Cleanup(func() { rdb.Del(context.Background(), approvalKey(sessionKey)) })
	ctx := context.Background()

	if _, err := ap.BeforeTool(invocationCtx(sessionKey, "u1"), toolArgs("c1", "op_a", `{"x":"1"}`)); err != nil {
		t.Fatal(err)
	}
	msg := testMsg("拒绝")
	msg.SessionKey = sessionKey
	handled, out, dec, err := ap.Answer(ctx, msg)
	if err != nil || !handled {
		t.Fatalf("reject must be handled, got handled=%v err=%v", handled, err)
	}
	if dec.decision != "deny" || dec.errorType != "user_rejected" {
		t.Fatalf("want deny/user_rejected, got %+v", dec)
	}
	if !strings.Contains(out.Text, "已取消") {
		t.Fatalf("want cancellation notice, got %q", out.Text)
	}
}

func TestAnswerTimeout(t *testing.T) {
	ap, rdb := approverForTest(t)
	sessionKey := "test:approval:" + t.Name()
	t.Cleanup(func() { rdb.Del(context.Background(), approvalKey(sessionKey)) })

	// Seed an already-expired pending record.
	if err := ap.set(context.Background(), sessionKey, PendingApproval{
		CallID: "c1", ToolName: "op_a", Arguments: []byte(`{"x":"1"}`),
		Requester: "u1", Deadline: time.Now().Add(-time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	msg := testMsg("确认")
	msg.SessionKey = sessionKey
	handled, out, dec, err := ap.Answer(context.Background(), msg)
	if err != nil || !handled {
		t.Fatalf("late confirm must be handled, got handled=%v err=%v", handled, err)
	}
	if dec.decision != "review_timeout" {
		t.Fatalf("want review_timeout, got %+v", dec)
	}
	if !strings.Contains(out.Text, "超时") {
		t.Fatalf("want timeout notice, got %q", out.Text)
	}
}

func TestAnswerGroupOnlyRequester(t *testing.T) {
	ap, rdb := approverForTest(t)
	sessionKey := "test:approval:" + t.Name()
	t.Cleanup(func() { rdb.Del(context.Background(), approvalKey(sessionKey)) })
	ctx := context.Background()

	if _, err := ap.BeforeTool(invocationCtx(sessionKey, "u1"), toolArgs("c1", "op_a", `{"x":"1"}`)); err != nil {
		t.Fatal(err)
	}

	// Another group member's confirm does not consume the approval.
	msg := testMsg("确认")
	msg.SessionKey = sessionKey
	msg.ChatID = "room1"
	msg.UserID = "u2"
	handled, out, _, err := ap.Answer(ctx, msg)
	if err != nil || !handled {
		t.Fatalf("non-requester answer must be handled (with a notice), got handled=%v err=%v", handled, err)
	}
	if !strings.Contains(out.Text, "发起人") {
		t.Fatalf("want requester-only notice, got %q", out.Text)
	}
	p, _ := ap.pending(ctx, sessionKey)
	if p == nil {
		t.Fatal("non-requester answer must not consume the pending approval")
	}

	// The original requester's confirm does.
	msg.UserID = "u1"
	handled, _, dec, err := ap.Answer(ctx, msg)
	if err != nil || !handled || dec.decision != "allow" {
		t.Fatalf("requester confirm must execute, got handled=%v dec=%+v err=%v", handled, dec, err)
	}
}

func TestNonAnswerDoesNotDisturbPending(t *testing.T) {
	ap, rdb := approverForTest(t)
	sessionKey := "test:approval:" + t.Name()
	t.Cleanup(func() { rdb.Del(context.Background(), approvalKey(sessionKey)) })
	ctx := context.Background()

	if _, err := ap.BeforeTool(invocationCtx(sessionKey, "u1"), toolArgs("c1", "op_a", `{"x":"1"}`)); err != nil {
		t.Fatal(err)
	}
	msg := testMsg("今天天气怎么样")
	msg.SessionKey = sessionKey
	handled, _, _, err := ap.Answer(ctx, msg)
	if err != nil || handled {
		t.Fatalf("non-answer must pass through, got handled=%v err=%v", handled, err)
	}
	p, _ := ap.pending(ctx, sessionKey)
	if p == nil {
		t.Fatal("non-answer message must not disturb the pending approval")
	}
}
