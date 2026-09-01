package channels

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"

	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

var senderTracer = otel.Tracer("trpc-agent-service/sender")

func sendAttr(msg OutboundMessage, result string) otelmetric.MeasurementOption {
	return otelmetric.WithAttributes(
		attribute.String("channel", msg.Channel),
		attribute.String("tenant_id", msg.TenantID),
		attribute.String("result", result),
	)
}

// rateLimitedAttr tags re-queue events caused by send pacing.
func rateLimitedAttr(msg OutboundMessage) otelmetric.AddOption {
	return otelmetric.WithAttributes(
		attribute.String("channel", msg.Channel),
		attribute.String("tenant_id", msg.TenantID),
	)
}

// Sender consumes the outbound stream as part of consumer group "senders" and
// dispatches each message to the Send of its Channel.
//
// Per-message protocol (outbound idempotency): check the sent: key first and
// skip already-delivered replies; send; mark sent; only then Ack. A crash
// before Ack redelivers the message, but the sent: key blocks a duplicate
// push to the user. Failures stay pending for retry.
//
// Sends are paced per {channel, tenant} token bucket (design 5.3.2, IM
// proactive-send rate limits): when the bucket is exhausted the message is
// re-queued instead of dropped, so rate limiting delays but never loses.
type Sender struct {
	Stream   *storage.Stream
	Sent     *storage.SentMarker // nil disables outbound idempotency
	Channels map[string]Channel  // channel name → channel implementation
	Name     string              // consumer name

	// Limiter paces sends per {channel}:{tenant_id}; nil disables pacing.
	// SendQPS/SendBurst are the platform-level bucket shape; SendWait bounds
	// how long a message spins for a token before being re-queued.
	Limiter   *storage.Limiter
	SendQPS   float64
	SendBurst int
	SendWait  time.Duration

	InStream string // stream to consume; empty means storage.StreamOutbound
}

func (s *Sender) inStream() string {
	if s.InStream != "" {
		return s.InStream
	}
	return storage.StreamOutbound
}

func (s *Sender) sendQPS() float64 {
	if s.SendQPS > 0 {
		return s.SendQPS
	}
	return 20
}

func (s *Sender) sendBurst() int {
	if s.SendBurst > 0 {
		return s.SendBurst
	}
	return 40
}

func (s *Sender) sendWait() time.Duration {
	if s.SendWait > 0 {
		return s.SendWait
	}
	return 30 * time.Second
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
	ctx, span := senderTracer.Start(ctx, "sender.send")
	defer span.End()
	span.SetAttributes(
		attribute.String("channel", msg.Channel),
		attribute.String("session_key", msg.SessionKey),
	)

	// Already delivered (sent but un-acked in a previous life): skip the send.
	if s.Sent != nil && msg.MsgID != "" {
		sent, err := s.Sent.IsSent(ctx, msg.Channel, msg.MsgID)
		if err != nil {
			plog.Warnf("sender %s check sent %s: %v", s.Name, m.ID, err)
			return // Redis hiccup: leave pending, retry later
		}
		if sent {
			plog.Infof("sender %s skip already-sent reply for msg %s", s.Name, msg.MsgID)
			metrics.OutboundTotal.Add(ctx, 1, sendAttr(msg, "skipped_duplicate"))
			_ = s.Stream.Ack(ctx, s.inStream(), "senders", m.ID)
			return
		}
	}

	ch, ok := s.Channels[msg.Channel]
	if !ok {
		// An unknown channel is a configuration error, not a retryable
		// failure: Ack, drop and alert.
		plog.Errorf("sender %s: no channel named %q, drop %s", s.Name, msg.Channel, m.ID)
		_ = s.Stream.Ack(ctx, s.inStream(), "senders", m.ID)
		return
	}

	// Pace the send on the {channel, tenant} bucket (design 5.3.2). On
	// exhaustion the message is re-queued (new entry, original acked) rather
	// than dropped: the copy retries when the bucket has refilled.
	if s.Limiter != nil {
		scope := "send:" + msg.Channel + ":" + msg.TenantID
		ok, err := s.Limiter.WaitAllow(ctx, scope, s.sendQPS(), s.sendBurst(), s.sendWait())
		if err != nil {
			plog.Warnf("sender %s rate limit check %s: %v", s.Name, m.ID, err)
			return // Redis hiccup: leave pending, retry later
		}
		if !ok {
			if _, err := s.Stream.Add(ctx, s.inStream(), m.Payload); err != nil {
				plog.Errorf("sender %s re-queue %s: %v", s.Name, m.ID, err)
				return // stays pending
			}
			metrics.SendRateLimitedTotal.Add(ctx, 1, rateLimitedAttr(msg))
			plog.Warnf("sender %s re-queued %s: send bucket %s exhausted", s.Name, m.ID, scope)
			_ = s.Stream.Ack(ctx, s.inStream(), "senders", m.ID)
			return
		}
	}

	if err := ch.Send(ctx, msg); err != nil {
		// No Ack: leave it pending for retry.
		metrics.OutboundTotal.Add(ctx, 1, sendAttr(msg, "error"))
		plog.Errorf("sender %s send via %s failed: %v", s.Name, msg.Channel, err)
		span.RecordError(err)
		return
	}
	metrics.OutboundTotal.Add(ctx, 1, sendAttr(msg, "ok"))
	if s.Sent != nil && msg.MsgID != "" {
		if err := s.Sent.MarkSent(ctx, msg.Channel, msg.MsgID, ""); err != nil {
			plog.Warnf("sender %s mark sent %s: %v", s.Name, m.ID, err)
		}
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
