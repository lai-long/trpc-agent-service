package agent_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/mock"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

// fastWorker builds a Worker on dedicated streams with reaping tuned for tests.
func fastWorker(stream *storage.Stream, p agent.Processor, inbound, outbound string) *agent.Worker {
	return &agent.Worker{
		Stream: stream, Processor: p, Name: "test-reaper",
		InStream: inbound, OutStream: outbound,
		ReapInterval: 100 * time.Millisecond,
		MaxIdle:      time.Millisecond,
		MaxAttempts:  3,
	}
}

// A pending message whose consumer vanished is taken over and processed.
func TestWorkerTakesOverOrphanedMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb, err := storage.NewRedis(ctx, "localhost:6380")
	if err != nil {
		t.Skipf("redis unavailable (%v), skipping integration test", err)
	}
	defer func() { _ = rdb.Close() }()

	stream := storage.NewStream(rdb)
	inbound := "test:reap:in:" + t.Name()
	outbound := "test:reap:out:" + t.Name()
	t.Cleanup(func() { rdb.Del(context.Background(), inbound, outbound) })
	if err := stream.EnsureGroup(ctx, inbound, "workers"); err != nil {
		t.Fatal(err)
	}
	if err := stream.EnsureGroup(ctx, outbound, "senders"); err != nil {
		t.Fatal(err)
	}

	in := channels.InboundMessage{
		Channel: "mock", MsgID: "orphan-1", SessionKey: "dm:mock:u9",
		UserID: "u9", Text: "rescue me", TraceID: "trace-orphan",
	}
	payload, _ := json.Marshal(in)
	if _, err := stream.Add(ctx, inbound, payload); err != nil {
		t.Fatal(err)
	}

	// A consumer reads the message and crashes before Ack (stays pending).
	if _, err := stream.Read(ctx, inbound, "workers", "dead-worker", 1, time.Second); err != nil {
		t.Fatal(err)
	}

	mockCh := mock.New()
	worker := fastWorker(stream, agent.EchoProcessor{}, inbound, outbound)
	sender := &channels.Sender{
		Stream:   stream,
		Channels: map[string]channels.Channel{mockCh.Name(): mockCh},
		Name:     "test-s", InStream: outbound,
	}
	go func() { _ = worker.Run(ctx) }()
	go func() { _ = sender.Run(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, m := range mockCh.Sent() {
			if m.TraceID == "trace-orphan" {
				return // taken over and delivered
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("orphaned pending message was not taken over within 5s")
}

// failProcessor always fails, driving the message into the dead-letter stream.
type failProcessor struct{}

func (failProcessor) Process(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
	return channels.OutboundMessage{}, errors.New("always fails")
}

// A message that fails past MaxAttempts lands in stream:deadletter.
func TestWorkerDeadLettersPoisonMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb, err := storage.NewRedis(ctx, "localhost:6380")
	if err != nil {
		t.Skipf("redis unavailable (%v), skipping integration test", err)
	}
	defer func() { _ = rdb.Close() }()

	stream := storage.NewStream(rdb)
	inbound := "test:dlq:in:" + t.Name()
	deadletter := "test:dlq:dead:" + t.Name()
	t.Cleanup(func() { rdb.Del(context.Background(), inbound, deadletter) })
	if err := stream.EnsureGroup(ctx, inbound, "workers"); err != nil {
		t.Fatal(err)
	}

	in := channels.InboundMessage{
		Channel: "mock", MsgID: "poison-1", SessionKey: "dm:mock:u10",
		UserID: "u10", Text: "boom", TraceID: "trace-poison",
	}
	payload, _ := json.Marshal(in)
	if _, err := stream.Add(ctx, inbound, payload); err != nil {
		t.Fatal(err)
	}

	worker := fastWorker(stream, failProcessor{}, inbound, "test:dlq:out:"+t.Name())
	go func() { _ = worker.Run(ctx) }()

	// The platform dead-letter stream is shared; count entries matching this
	// test's message before and after to stay robust against leftovers.
	countDead := func() int {
		msgs, err := rdb.XRange(ctx, storage.StreamDeadletter, "-", "+").Result()
		if err != nil {
			return 0
		}
		n := 0
		for _, m := range msgs {
			if body, ok := m.Values["payload"].(string); ok &&
				strings.Contains(body, "poison-1") && strings.Contains(body, inbound) {
				n++
			}
		}
		return n
	}
	before := countDead()

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if countDead() > before {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("poison message was not dead-lettered within 8s")
}
