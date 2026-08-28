// Package agent owns agent definitions and Runner assembly.
//
// The Worker consumes stream:inbound, processes each message through a
// Processor, and produces replies to stream:outbound.
package agent

import (
	"context"
	"encoding/json"

	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

var tracer = otel.Tracer("trpc-agent-service/worker")

func processAttr(msg channels.InboundMessage) otelmetric.MeasurementOption {
	return otelmetric.WithAttributes(
		attribute.String("channel", msg.Channel),
		attribute.String("tenant_id", msg.TenantID),
	)
}

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
		MsgID:      msg.MsgID,
		SessionKey: msg.SessionKey,
		UserID:     msg.UserID,
		ChatID:     msg.ChatID,
		Text:       "echo: " + msg.Text,
		TenantID:   msg.TenantID,
		TraceID:    msg.TraceID,
	}, nil
}

// Worker consumes the inbound stream as part of consumer group "workers": on
// success the reply is enqueued to the outbound stream and the message is
// Acked; on failure the message stays pending for a surviving node to take
// over via XCLAIM (a crash loses no messages).
//
// Message-level auditing is owned by the guardrail (the Guarded processor),
// which is the only place that knows the decision behind each reply.
type Worker struct {
	Stream    *storage.Stream
	Lock      *storage.Lock // nil disables session locking (single-replica dev)
	Processor Processor
	Name      string // consumer name identifying pending ownership (e.g. hostname-pid)

	InStream  string // empty means storage.StreamInbound
	OutStream string // empty means storage.StreamOutbound

	// ReapInterval is how often pending messages are scanned for takeover.
	// MaxIdle is how long a message must stay pending before it counts as
	// orphaned; it must exceed the p95 processing time, or healthy in-flight
	// work would be double-processed. MaxAttempts caps redeliveries before a
	// message is dead-lettered.
	ReapInterval time.Duration
	MaxIdle      time.Duration
	MaxAttempts  int64

	// LockTTL is the session lock lease, renewed by the watchdog every
	// TTL/3 while processing runs. LockWait is how long a message spins for
	// the lock before being re-queued.
	LockTTL  time.Duration
	LockWait time.Duration
}

func (w *Worker) reapInterval() time.Duration {
	if w.ReapInterval > 0 {
		return w.ReapInterval
	}
	return 30 * time.Second
}

func (w *Worker) maxIdle() time.Duration {
	if w.MaxIdle > 0 {
		return w.MaxIdle
	}
	return 10 * time.Minute
}

func (w *Worker) maxAttempts() int64 {
	if w.MaxAttempts > 0 {
		return w.MaxAttempts
	}
	return 5
}

func (w *Worker) lockTTL() time.Duration {
	if w.LockTTL > 0 {
		return w.LockTTL
	}
	return 10 * time.Second
}

func (w *Worker) lockWait() time.Duration {
	if w.LockWait > 0 {
		return w.LockWait
	}
	return 15 * time.Second
}

func (w *Worker) inStream() string {
	if w.InStream != "" {
		return w.InStream
	}
	return storage.StreamInbound
}

func (w *Worker) outStream() string {
	if w.OutStream != "" {
		return w.OutStream
	}
	return storage.StreamOutbound
}

// Run consumes until ctx is canceled; a nil return means a clean shutdown.
// Every reapInterval it also takes over pending messages orphaned by crashed
// consumers (XCLAIM semantics via XAUTOCLAIM).
func (w *Worker) Run(ctx context.Context) error {
	lastReap := time.Now()
	for {
		if ctx.Err() != nil {
			return nil
		}

		if time.Since(lastReap) >= w.reapInterval() {
			w.reap(ctx)
			lastReap = time.Now()
		}

		msgs, err := w.Stream.Read(ctx, w.inStream(), "workers", w.Name, 10, 2*time.Second)
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
		_ = w.Stream.Ack(ctx, w.inStream(), "workers", m.ID)
		return
	}

	// Continue the trace across the async Stream boundary.
	ctx = metrics.ExtractTraceparent(ctx, propagation.MapCarrier{"traceparent": msg.TraceParent})
	ctx, span := tracer.Start(ctx, "worker.process")
	defer span.End()
	span.SetAttributes(
		attribute.String("channel", msg.Channel),
		attribute.String("tenant_id", msg.TenantID),
		attribute.String("session_key", msg.SessionKey),
	)
	started := time.Now()

	// Session lock: serialize concurrent processing of the same session
	// across replicas. Deferred calls run LIFO: the watchdog stops first,
	// then the lock is released.
	if w.Lock != nil {
		owner, stopWatchdog, ok := w.acquireSession(ctx, m, msg.SessionKey)
		if !ok {
			return // re-queued or shutting down
		}
		defer func() {
			releaseCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := w.Lock.Release(releaseCtx, msg.SessionKey, owner); err != nil {
				plog.Warnf("worker %s release lock %s: %v", w.Name, msg.SessionKey, err)
			}
		}()
		defer stopWatchdog()
	}

	out, err := w.Processor.Process(ctx, msg)
	metrics.ProcessDuration.Record(ctx, float64(time.Since(started).Milliseconds()), processAttr(msg))
	if err != nil {
		// No Ack: leave it pending for redelivery. Redelivery produces
		// duplicate events, deduplicated by the (session_id, event_seq)
		// unique constraint.
		metrics.ProcessErrorTotal.Add(ctx, 1, processAttr(msg))
		plog.Errorf("worker %s process %s failed: %v", w.Name, m.ID, err)
		span.RecordError(err)
		return
	}

	// Carry the worker span context into the outbound message so the send
	// span joins this trace as a child of worker.process.
	carrier := propagation.MapCarrier{}
	metrics.InjectTraceparent(ctx, carrier)
	out.TraceParent = carrier.Get("traceparent")

	payload, err := json.Marshal(out)
	if err != nil {
		plog.Errorf("worker %s marshal outbound: %v", w.Name, err)
		return
	}
	if _, err := w.Stream.Add(ctx, w.outStream(), payload); err != nil {
		plog.Errorf("worker %s enqueue outbound: %v", w.Name, err)
		return
	}

	// Ack the inbound message only after the reply is enqueued; redelivery
	// within the crash window is covered by outbound idempotency (sent: key).
	if err := w.Stream.Ack(ctx, w.inStream(), "workers", m.ID); err != nil {
		plog.Warnf("worker %s ack %s: %v", w.Name, m.ID, err)
	}
	zap.L().Debug("message processed",
		zap.String(plog.FieldSessionKey, msg.SessionKey),
		zap.String(plog.FieldTraceID, msg.TraceID))
}

// reap takes over pending messages idle longer than maxIdle (their consumers
// crashed) and reprocesses them. A message that keeps failing past
// maxAttempts is dead-lettered so it cannot loop forever.
func (w *Worker) reap(ctx context.Context) {
	if err := w.Stream.EnsureGroup(ctx, w.inStream(), "workers"); err != nil {
		plog.Warnf("worker %s ensure group before reap: %v", w.Name, err)
		return
	}
	msgs, err := w.Stream.AutoClaim(ctx, w.inStream(), "workers", w.Name, w.maxIdle(), 50)
	if err != nil {
		plog.Warnf("worker %s autoclaim: %v", w.Name, err)
		return
	}
	for _, m := range msgs {
		attempts, err := w.Stream.Attempts(ctx, w.inStream(), m.ID)
		if err != nil {
			plog.Warnf("worker %s count attempts %s: %v", w.Name, m.ID, err)
			continue
		}
		if attempts > w.maxAttempts() {
			plog.Errorf("worker %s dead-letters %s after %d attempts", w.Name, m.ID, attempts)
			if err := w.Stream.DeadLetter(ctx, w.inStream(), "workers", m); err != nil {
				plog.Errorf("worker %s deadletter %s: %v", w.Name, m.ID, err)
			}
			continue
		}
		plog.Infof("worker %s takes over %s (attempt %d)", w.Name, m.ID, attempts)
		w.handle(ctx, m)
	}
}

// acquireSession spins for the session lock until lockWait. On timeout the
// message is re-queued (as a new entry) and the original acked, so a busy
// session delays the message instead of failing it. The returned stop ends
// the renewal watchdog.
func (w *Worker) acquireSession(ctx context.Context, m storage.Message, sessionKey string) (owner string, stop func(), ok bool) {
	owner = w.Name + ":" + m.ID
	deadline := time.Now().Add(w.lockWait())
	for {
		acquired, err := w.Lock.TryAcquire(ctx, sessionKey, owner, w.lockTTL())
		if err != nil {
			plog.Warnf("worker %s acquire lock %s: %v", w.Name, sessionKey, err)
		}
		if acquired {
			return owner, w.startLockWatchdog(ctx, sessionKey, owner), true
		}
		if time.Now().After(deadline) {
			if _, err := w.Stream.Add(ctx, w.inStream(), m.Payload); err != nil {
				plog.Errorf("worker %s re-queue %s: %v", w.Name, m.ID, err)
				return "", nil, false // stays pending, the reaper will retry
			}
			if err := w.Stream.Ack(ctx, w.inStream(), "workers", m.ID); err != nil {
				plog.Warnf("worker %s ack after re-queue %s: %v", w.Name, m.ID, err)
			}
			plog.Infof("worker %s re-queued %s: session %s is busy", w.Name, m.ID, sessionKey)
			return "", nil, false
		}
		select {
		case <-ctx.Done():
			return "", nil, false
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// startLockWatchdog renews the session lock every TTL/3 so long tool calls
// and slow generations cannot outlive the lease. It exits when ctx is
// canceled, the lock is lost, or the returned stop is called.
func (w *Worker) startLockWatchdog(ctx context.Context, sessionKey, owner string) (stop func()) {
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(w.lockTTL() / 3)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				ok, err := w.Lock.Extend(ctx, sessionKey, owner, w.lockTTL())
				if err != nil || !ok {
					plog.Warnf("worker %s lost session lock %s (extended=%v, err=%v)", w.Name, sessionKey, ok, err)
					return
				}
			}
		}
	}()
	return func() { close(done) }
}
