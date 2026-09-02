package channels_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

// countingChannel records how many times Send was called.
type countingChannel struct {
	mu    sync.Mutex
	calls int
}

func (c *countingChannel) Name() string                                        { return "counting" }
func (c *countingChannel) RegisterRoutes(_ *http.ServeMux, _ channels.Handler) {}
func (c *countingChannel) Send(context.Context, channels.OutboundMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	return nil
}
func (c *countingChannel) Calls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

// A redelivered outbound message (sent but un-acked in a previous attempt)
// must not be sent twice.
func TestSenderOutboundIdempotency(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb, err := storage.NewRedis(ctx, "localhost:6380")
	if err != nil {
		t.Skipf("redis unavailable (%v), skipping integration test", err)
	}
	// Cleanups run LIFO; register Close first so the Del cleanup runs while
	// the client is still open.
	t.Cleanup(func() { _ = rdb.Close() })

	stream := storage.NewStream(rdb)
	outbound := "test:sent:out:" + t.Name()
	sentKey := fmt.Sprintf("sent:counting:dup-msg-%s", t.Name())
	t.Cleanup(func() { rdb.Del(context.Background(), outbound, sentKey) })
	if err := stream.EnsureGroup(ctx, outbound, "senders"); err != nil {
		t.Fatal(err)
	}

	ch := &countingChannel{}
	sender := &channels.Sender{
		Stream:   stream,
		Sent:     storage.NewSentMarker(rdb),
		Channels: map[string]channels.Channel{ch.Name(): ch},
		Name:     "test-s", InStream: outbound,
	}
	go func() { _ = sender.Run(ctx) }()

	out := channels.OutboundMessage{
		Channel: "counting", MsgID: "dup-msg-" + t.Name(),
		SessionKey: "dm:counting:u1", UserID: "u1", Text: "reply",
	}
	payload, _ := json.Marshal(out)

	// Same payload twice: simulates "send succeeded, ack crashed" redelivery.
	if _, err := stream.Add(ctx, outbound, payload); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return ch.Calls() == 1 })

	// At this point the first delivery is acked. A duplicate entry (same msg)
	// arrives — the sent: key must block a second send.
	if _, err := stream.Add(ctx, outbound, payload); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		sent, err := storage.NewSentMarker(rdb).IsSent(ctx, "counting", out.MsgID)
		return err == nil && sent
	})
	time.Sleep(500 * time.Millisecond) // give a buggy duplicate a chance to fire
	if got := ch.Calls(); got != 1 {
		t.Fatalf("Send called %d times for the same message, want 1", got)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("condition not met within 5s")
}

// An exhausted send bucket re-queues the message instead of dropping it
// (design 5.3.2: 超限在 Stream 内排队不丢弃).
func TestSenderRateLimitedRequeues(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb, err := storage.NewRedis(ctx, "localhost:6380")
	if err != nil {
		t.Skipf("redis unavailable (%v), skipping integration test", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })

	stream := storage.NewStream(rdb)
	outbound := "test:rl:out:" + t.Name()
	t.Cleanup(func() { rdb.Del(context.Background(), outbound) })
	t.Cleanup(func() { rdb.Del(context.Background(), "ratelimit:send:counting:t-rl") })
	if err := stream.EnsureGroup(ctx, outbound, "senders"); err != nil {
		t.Fatal(err)
	}

	ch := &countingChannel{}
	sender := &channels.Sender{
		Stream:   stream,
		Channels: map[string]channels.Channel{"counting": ch},
		Name:     "test-rl",
		InStream: outbound,
		// burst=1 with a near-zero refill and a short wait: the first message
		// sends, the second spins 200ms and is re-queued.
		Limiter: storage.NewLimiter(rdb), SendQPS: 0.001, SendBurst: 1, SendWait: 200 * time.Millisecond,
	}
	go func() { _ = sender.Run(ctx) }()

	mk := func(id string) []byte {
		payload, _ := json.Marshal(channels.OutboundMessage{
			Channel: "counting", MsgID: id, SessionKey: "dm:counting:u1",
			UserID: "u1", Text: "hi", TenantID: "t-rl",
		})
		return payload
	}
	if _, err := stream.Add(ctx, outbound, mk("rl-a")); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Add(ctx, outbound, mk("rl-b")); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && ch.Calls() < 1 {
		time.Sleep(50 * time.Millisecond)
	}
	if ch.Calls() != 1 {
		t.Fatalf("want exactly 1 send (burst), got %d", ch.Calls())
	}
	// The rate-limited message must still be in the stream (re-queued copy or
	// pending original), not dropped.
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		n, err := stream.Len(ctx, outbound)
		if err != nil {
			t.Fatal(err)
		}
		if n >= 1 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("rate-limited message vanished from the stream")
}

// A tenant send override (rate_policy send_qps/send_burst) replaces the
// platform default bucket shape (design 5.3.2 按 {channel}:{tenant} 限速).
func TestSenderTenantRateOverride(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb, err := storage.NewRedis(ctx, "localhost:6380")
	if err != nil {
		t.Skipf("redis unavailable (%v), skipping integration test", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })

	stream := storage.NewStream(rdb)
	outbound := "test:rl-tenant:out:" + t.Name()
	t.Cleanup(func() { rdb.Del(context.Background(), outbound) })
	t.Cleanup(func() {
		rdb.Del(context.Background(), "ratelimit:send:counting:t-fast", "ratelimit:send:counting:t-slow")
	})
	if err := stream.EnsureGroup(ctx, outbound, "senders"); err != nil {
		t.Fatal(err)
	}

	ch := &countingChannel{}
	sender := &channels.Sender{
		Stream: stream, Channels: map[string]channels.Channel{"counting": ch},
		Name: "test-rl-tenant", InStream: outbound,
		// Platform default is slow (1 token / 100s); the tenant override is fast.
		Limiter: storage.NewLimiter(rdb), SendQPS: 0.01, SendBurst: 1, SendWait: 300 * time.Millisecond,
		SendPolicyFor: func(_ context.Context, tenantID string) (float64, int, bool) {
			if tenantID == "t-fast" {
				return 100, 10, true
			}
			return 0, 0, false
		},
	}
	go func() { _ = sender.Run(ctx) }()

	mk := func(id, tenant string) []byte {
		payload, _ := json.Marshal(channels.OutboundMessage{
			Channel: "counting", MsgID: id, SessionKey: "dm:counting:u1",
			UserID: "u1", Text: "hi", TenantID: tenant,
		})
		return payload
	}
	// Two sends each: t-fast's override lets both through quickly; t-slow's
	// second one waits for the default bucket and gets re-queued.
	for _, m := range [][2]string{{"fast-a", "t-fast"}, {"fast-b", "t-fast"}, {"slow-a", "t-slow"}, {"slow-b", "t-slow"}} {
		if _, err := stream.Add(ctx, outbound, mk(m[0], m[1])); err != nil {
			t.Fatal(err)
		}
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ch.Calls() >= 3 { // fast-a, fast-b, slow-a
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("want 3 sends (override tenant not throttled), got %d", ch.Calls())
}
