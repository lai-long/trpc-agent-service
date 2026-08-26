// Package agent owns agent definitions and Runner assembly.
//
// The Worker consumes stream:inbound, processes each message through a
// Processor, and produces replies to stream:outbound.
package agent

import (
	"context"
	"encoding/json"

	"time"

	"go.uber.org/zap"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

// Processor handles one inbound message and produces a reply.
// Runner-backed implementations must consume the framework event channel to
// completion, and drain it after context cancellation, to avoid goroutine
// leaks.
type Processor interface {
	Process(ctx context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error)
}

// EchoProcessor echoes the input verbatim. It exercises the pipeline end to
// end without LLM access and serves as the fallback when no model key is
// configured.
type EchoProcessor struct{}

// Process implements Processor.
func (EchoProcessor) Process(_ context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
	return channels.OutboundMessage{
		Channel:    msg.Channel,
		SessionKey: msg.SessionKey,
		UserID:     msg.UserID,
		ChatID:     msg.ChatID,
		Text:       "echo: " + msg.Text,
		TraceID:    msg.TraceID,
	}, nil
}

// Worker consumes stream:inbound as part of consumer group "workers": on
// success the reply is enqueued to stream:outbound and the message is Acked;
// on failure the message stays pending for a surviving node to take over via
// XCLAIM (a crash loses no messages).
type Worker struct {
	Stream    *storage.Stream
	Processor Processor
	Name      string // consumer name identifying pending ownership (e.g. hostname-pid)
}

// Run consumes until ctx is canceled; a nil return means a clean shutdown.
func (w *Worker) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}

		msgs, err := w.Stream.Read(ctx, storage.StreamInbound, "workers", w.Name, 10, 2*time.Second)
		if err != nil {
			if ctx.Err() != nil {
				return nil // read error during shutdown — exit cleanly
			}
			// Redis briefly unavailable: back off and retry instead of
			// killing the worker.
			plog.Warnf("worker %s read inbound: %v", w.Name, err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Second):
			}
			continue
		}

		for _, m := range msgs {
			w.handle(ctx, m)
		}
	}
}

func (w *Worker) handle(ctx context.Context, m storage.Message) {
	var msg channels.InboundMessage
	if err := json.Unmarshal(m.Payload, &msg); err != nil {
		// Poison message (can never be unmarshaled): Ack and drop it so it
		// cannot block the queue with endless redeliveries.
		plog.Errorf("worker %s drop poison message %s: %v", w.Name, m.ID, err)
		_ = w.Stream.Ack(ctx, storage.StreamInbound, "workers", m.ID)
		return
	}

	out, err := w.Processor.Process(ctx, msg)
	if err != nil {
		// No Ack: leave it pending for redelivery. Redelivery produces
		// duplicate events, deduplicated by the (session_id, event_seq)
		// unique constraint.
		plog.Errorf("worker %s process %s failed: %v", w.Name, m.ID, err)
		return
	}

	payload, err := json.Marshal(out)
	if err != nil {
		plog.Errorf("worker %s marshal outbound: %v", w.Name, err)
		return
	}
	if _, err := w.Stream.Add(ctx, storage.StreamOutbound, payload); err != nil {
		plog.Errorf("worker %s enqueue outbound: %v", w.Name, err)
		return
	}

	// Ack the inbound message only after the reply is enqueued; redelivery
	// within the crash window is covered by outbound idempotency (sent: key).
	if err := w.Stream.Ack(ctx, storage.StreamInbound, "workers", m.ID); err != nil {
		plog.Warnf("worker %s ack %s: %v", w.Name, m.ID, err)
	}
	zap.L().Debug("message processed",
		zap.String(plog.FieldSessionKey, msg.SessionKey),
		zap.String(plog.FieldTraceID, msg.TraceID))
}
