package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
)

// AuditEvent is one row of the audit_log table.
type AuditEvent struct {
	TenantID         string // tenant UUID; zero UUID until the tenant module lands
	Channel          string
	UserID           string
	SessionID        string // session UUID; empty means NULL (session_key is not a UUID)
	AgentName        string
	ToolName         string
	Decision         string // allow / deny / review
	LatencyMs        int
	ErrorType        string
	Cost             float64
	PromptTokens     int
	CompletionTokens int
	TraceID          string
	CreatedAt        time.Time
}

const (
	auditBatchSize     = 100             // flush the buffer at this many events
	auditFlushInterval = time.Second     // or this often, whichever comes first
	auditQueueSize     = 10000           // events beyond this are dropped with a warning
	auditSyncTimeout   = 3 * time.Second // deny/review writes get this much slack
)

// Auditor writes audit events to PG in two lanes: routine allow events are
// buffered and flushed asynchronously in batches (off the request path);
// critical decisions (deny / review) are written synchronously — they must
// not be lost, even at the cost of milliseconds of latency.
type Auditor struct {
	pool   *pgxpool.Pool
	ch     chan AuditEvent
	cancel context.CancelFunc
	done   chan struct{}
}

// NewAuditor creates an Auditor on an established pool.
func NewAuditor(pool *pgxpool.Pool) *Auditor {
	return &Auditor{
		pool: pool,
		ch:   make(chan AuditEvent, auditQueueSize),
		done: make(chan struct{}),
	}
}

// Start runs the background flush loop until Close.
func (a *Auditor) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	a.cancel = cancel
	go func() {
		defer close(a.done)
		ticker := time.NewTicker(auditFlushInterval)
		defer ticker.Stop()
		buf := make([]AuditEvent, 0, auditBatchSize)
		for {
			select {
			case ev := <-a.ch:
				buf = append(buf, ev)
				if len(buf) >= auditBatchSize {
					a.flush(buf)
					buf = buf[:0]
				}
			case <-ticker.C:
				if len(buf) > 0 {
					a.flush(buf)
					buf = buf[:0]
				}
			case <-ctx.Done():
				if len(buf) > 0 {
					a.flush(buf)
				}
				return
			}
		}
	}()
}

// Close stops the flush loop and waits for the remaining buffer to flush.
func (a *Auditor) Close() {
	a.cancel()
	<-a.done
}

// LogAsync buffers a routine event; the queue full case drops the event with
// a warning rather than blocking the request path.
func (a *Auditor) LogAsync(ev AuditEvent) {
	if ev.CreatedAt.IsZero() {
		ev.CreatedAt = time.Now()
	}
	select {
	case a.ch <- ev:
	default:
		plog.Warnf("audit queue full, dropping %s event (trace=%s)", ev.Decision, ev.TraceID)
	}
}

// LogSync writes a critical decision (deny / review) immediately.
func (a *Auditor) LogSync(ctx context.Context, ev AuditEvent) error {
	if ev.CreatedAt.IsZero() {
		ev.CreatedAt = time.Now()
	}
	ctx, cancel := context.WithTimeout(ctx, auditSyncTimeout)
	defer cancel()
	return a.insert(ctx, []AuditEvent{ev})
}

func (a *Auditor) flush(buf []AuditEvent) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.insert(ctx, buf); err != nil {
		plog.Errorf("audit batch flush (%d events): %v", len(buf), err)
	}
}

func (a *Auditor) insert(ctx context.Context, events []AuditEvent) error {
	const q = `INSERT INTO audit_log
		(tenant_id, channel, user_id, session_id, agent_name, tool_name,
		 decision, latency_ms, error_type, cost, prompt_tokens, completion_tokens,
		 trace_id, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`
	for _, ev := range events {
		var sessionID *string
		if ev.SessionID != "" {
			sessionID = &ev.SessionID
		}
		if _, err := a.pool.Exec(ctx, q,
			ev.TenantID, ev.Channel, ev.UserID, sessionID, ev.AgentName, ev.ToolName,
			ev.Decision, ev.LatencyMs, ev.ErrorType, ev.Cost, ev.PromptTokens,
			ev.CompletionTokens, ev.TraceID, ev.CreatedAt,
		); err != nil {
			return fmt.Errorf("insert audit_log: %w", err)
		}
	}
	return nil
}
