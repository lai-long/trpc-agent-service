package agent_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/mock"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

// End to end: inbound enqueue → Worker(echo) → outbound enqueue → Sender →
// mock inbox. Needs the Redis from compose (localhost:6380); skips when
// unreachable.
func TestWorkerSenderRoundTrip(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb, err := storage.NewRedis(ctx, "localhost:6380")
	if err != nil {
		t.Skipf("redis unavailable (%v), skipping integration test", err)
	}
	defer func() { _ = rdb.Close() }()

	stream := storage.NewStream(rdb)
	if err := stream.EnsureGroup(ctx, storage.StreamInbound, "workers"); err != nil {
		t.Fatal(err)
	}
	if err := stream.EnsureGroup(ctx, storage.StreamOutbound, "senders"); err != nil {
		t.Fatal(err)
	}

	mockCh := mock.New()
	worker := &agent.Worker{Stream: stream, Processor: agent.EchoProcessor{}, Name: "test-w"}
	sender := &channels.Sender{
		Stream:   stream,
		Channels: map[string]channels.Channel{mockCh.Name(): mockCh},
		Name:     "test-s",
	}
	go func() { _ = worker.Run(ctx) }()
	go func() { _ = sender.Run(ctx) }()

	in := channels.InboundMessage{
		Channel:    "mock",
		MsgID:      "test-msg-1",
		SessionKey: channels.SessionKey("mock", "u1", ""),
		UserID:     "u1",
		Text:       "hello worker",
		TraceID:    "trace-1",
	}
	payload, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Add(ctx, storage.StreamInbound, payload); err != nil {
		t.Fatal(err)
	}

	// Poll the mock inbox for up to 5 seconds.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, m := range mockCh.Sent() {
			if m.TraceID == "trace-1" {
				if m.Text != "echo: hello worker" {
					t.Fatalf("unexpected reply: %q", m.Text)
				}
				if m.SessionKey != "dm:mock:u1" {
					t.Fatalf("unexpected session key: %q", m.SessionKey)
				}
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("reply did not arrive in mock inbox within 5s")
}
