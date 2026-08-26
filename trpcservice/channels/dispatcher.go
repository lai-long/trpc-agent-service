package channels

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"

	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

var senderTracer = otel.Tracer("trpc-agent-service/sender")

// Sender consumes the outbound stream as part of consumer group "senders" and
// dispatches each message to the Send of its Channel. A message is Acked
// only after a successful send; failures stay pending for retry.
type Sender struct {
	Stream   *storage.Stream
	Channels map[string]Channel // channel name → channel implementation
	Name     string             // consumer name

	InStream string // stream to consume; empty means storage.StreamOutbound
}

func (s *Sender) inStream() string {
	if s.InStream != "" {
		return s.InStream
	}
	return storage.StreamOutbound
}

// Run consumes until ctx is canceled; a nil return means a clean shutdown.
func (s *Sender) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}

		msgs, err := s.Stream.Read(ctx, s.inStream(), "senders", s.Name, 10, 2*time.Second)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			plog.Warnf("sender %s read outbound: %v", s.Name, err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Second):
			}
			continue
		}

		for _, m := range msgs {
			s.handle(ctx, m)
		}
	}
}

func (s *Sender) handle(ctx context.Context, m storage.Message) {
	var msg OutboundMessage
	if err := json.Unmarshal(m.Payload, &msg); err != nil {
		plog.Errorf("sender %s drop poison message %s: %v", s.Name, m.ID, err)
		_ = s.Stream.Ack(ctx, s.inStream(), "senders", m.ID)
		return
	}

	// Continue the message trace across the outbound Stream boundary.
	ctx = metrics.ExtractTraceparent(ctx, propagation.MapCarrier{"traceparent": msg.TraceParent})
	_, span := senderTracer.Start(ctx, "sender.send")
	defer span.End()
	span.SetAttributes(
		attribute.String("channel", msg.Channel),
		attribute.String("session_key", msg.SessionKey),
	)

	ch, ok := s.Channels[msg.Channel]
	if !ok {
		// An unknown channel is a configuration error, not a retryable
		// failure: Ack, drop and alert.
		plog.Errorf("sender %s: no channel named %q, drop %s", s.Name, msg.Channel, m.ID)
		_ = s.Stream.Ack(ctx, s.inStream(), "senders", m.ID)
		return
	}

	if err := ch.Send(ctx, msg); err != nil {
		// No Ack: leave it pending for retry.
		plog.Errorf("sender %s send via %s failed: %v", s.Name, msg.Channel, err)
		span.RecordError(err)
		return
	}
	if err := s.Stream.Ack(ctx, s.inStream(), "senders", m.ID); err != nil {
		plog.Warnf("sender %s ack %s: %v", s.Name, m.ID, err)
	}
	zap.L().Debug("outbound delivered",
		zap.String(plog.FieldChannel, msg.Channel),
		zap.String(plog.FieldSessionKey, msg.SessionKey),
		zap.String(plog.FieldTraceID, msg.TraceID))
}

// String identifies the sender in logs.
func (s *Sender) String() string { return fmt.Sprintf("sender(%s)", s.Name) }
