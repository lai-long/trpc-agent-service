// Package metrics owns observability for the platform: tracing init and
// shared instrumentation helpers.
//
// Tracing is delegated to the framework's telemetry package: atrace.Start
// installs a global OTLP tracer provider and activates the framework's own
// spans (Runner / Tool / Model / Session), so platform spans and framework
// spans land in the same trace. The endpoint is configured through the
// standard OTEL_EXPORTER_OTLP_ENDPOINT env var.
//
// Platform-side spans cover the parts the framework cannot see: the IM
// callback, the Stream enqueue/consume boundary, and outbound delivery.
// The trace crosses the async Stream boundary via a W3C traceparent field
// carried in the message payload.
package metrics

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	atrace "trpc.group/trpc-go/trpc-agent-go/telemetry/trace"
)

// InitTracing initializes the global tracer provider and the W3C propagator.
// The returned shutdown flushes pending spans; call it before process exit.
func InitTracing(ctx context.Context) (shutdown func() error, err error) {
	clean, err := atrace.Start(ctx)
	if err != nil {
		return nil, fmt.Errorf("start tracing: %w", err)
	}
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	return clean, nil
}

// InjectTraceparent writes the current span context from ctx into carrier.
func InjectTraceparent(ctx context.Context, carrier propagation.MapCarrier) {
	otel.GetTextMapPropagator().Inject(ctx, carrier)
}

// ExtractTraceparent restores the remote span context from carrier into ctx.
func ExtractTraceparent(ctx context.Context, carrier propagation.MapCarrier) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}
