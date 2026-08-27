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
