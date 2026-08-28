// Package web provides the Admin API and the Gateway HTTP entry points.
package web

import (
	"context"
	"encoding/json"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

var tracer = otel.Tracer("trpc-agent-service/gateway")

// EnqueueHandler implements channels.Handler as the Gateway's inbound core
// (sync ack + async consume): a message is written to stream:inbound and
// acknowledged immediately; the reply is produced asynchronously by a Worker
// and delivered via stream:outbound.
//
// An empty OutboundMessage.Text means "accepted, reply follows
// asynchronously" (see the channels.Handler contract).
//
// Before enqueueing, the handler resolves tenant routing (webhook_path →
// channel_binding → tenant + app) and stamps tenant_id / app_id onto the
// message; unknown or inactive routes are rejected. Per-tenant token-bucket
// rate limiting plugs in next, between routing and dedup.
type EnqueueHandler struct {
	Stream *storage.Stream
	Dedup  *storage.Deduper
	// Routes resolves webhook_path to tenant/app; nil disables tenant routing
	// (single-tenant dev fallback, messages carry an empty tenant_id).
	Routes *tenant.Resolver

	InStream string // empty means storage.StreamInbound
}

func (h EnqueueHandler) inStream() string {
	if h.InStream != "" {
		return h.InStream
	}
	return storage.StreamInbound
}

// Handle implements channels.Handler.
//
// Starts the root span of the message trace and stamps the message with the
// real trace ID + traceparent, so the Worker continues the same trace after
// the async Stream hop.
func (h EnqueueHandler) Handle(ctx context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
	// Tenant routing first: a message with no active route is rejected before
	// consuming dedup keys or queue space.
	if h.Routes != nil {
		route, err := h.Routes.Resolve(ctx, msg.WebhookPath)
		if err != nil {
			return channels.OutboundMessage{}, fmt.Errorf("tenant route: %w", err)
		}
		msg.TenantID = route.Tenant.ID
		msg.AppID = route.App.ID
	}

	// Inbound idempotency gate: first arrival passes, duplicates get
	// ErrDuplicate so the channel layer answers 200 and the IM stops
	// redelivering.
	if h.Dedup != nil {
		first, err := h.Dedup.Check(ctx, msg.Channel, msg.MsgID)
		if err != nil {
			return channels.OutboundMessage{}, fmt.Errorf("dedup check: %w", err)
		}
		if !first {
			metrics.DedupDroppedTotal.Add(ctx, 1, chAttr(msg))
			return channels.OutboundMessage{}, channels.ErrDuplicate
		}
	}

	ctx, span := tracer.Start(ctx, "gateway.enqueue")
	defer span.End()
	span.SetAttributes(
		attribute.String("channel", msg.Channel),
		attribute.String("tenant_id", msg.TenantID),
		attribute.String("session_key", msg.SessionKey),
		attribute.String("user_id", msg.UserID),
	)

	msg.TraceID = span.SpanContext().TraceID().String()
	carrier := propagation.MapCarrier{}
	metrics.InjectTraceparent(ctx, carrier)
	msg.TraceParent = carrier.Get("traceparent")

	payload, err := json.Marshal(msg)
	if err != nil {
		return channels.OutboundMessage{}, fmt.Errorf("marshal inbound message: %w", err)
	}
	if _, err := h.Stream.Add(ctx, h.inStream(), payload); err != nil {
		// Return the error so the channel layer replies 5xx and the IM
		// retries later.
		return channels.OutboundMessage{}, fmt.Errorf("enqueue inbound: %w", err)
	}
	metrics.InboundTotal.Add(ctx, 1, chAttr(msg))
	return channels.OutboundMessage{}, nil
}

func chAttr(msg channels.InboundMessage) otelmetric.AddOption {
	return otelmetric.WithAttributes(
		attribute.String("channel", msg.Channel),
		attribute.String("tenant_id", msg.TenantID),
	)
}
