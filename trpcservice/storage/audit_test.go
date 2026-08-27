package storage

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// zeroTenant stands in for a real tenant id until the tenant module lands.
const zeroTenant = "00000000-0000-0000-0000-000000000000"

func TestAuditorAsyncBatchAndSync(t *testing.T) {
	pool, err := NewPG(context.Background(), testPGDSN)
	if err != nil {
		t.Skipf("postgres unavailable (%v), skipping integration test", err)
	}
	ctx := context.Background()
	tag := fmt.Sprintf("audit-test-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM audit_log WHERE trace_id LIKE $1", tag+"%")
		pool.Close()
	})

	a := NewAuditor(pool)
	a.Start()

	// Async lane: buffered, flushed by the 1s ticker.
	for i := 0; i < 3; i++ {
		a.LogAsync(AuditEvent{
			TenantID: zeroTenant, Channel: "mock", UserID: "u1",
			Decision: "allow", TraceID: fmt.Sprintf("%s-async-%d", tag, i),
		})
	}

	// Sync lane (deny/review): visible immediately.
	if err := a.LogSync(ctx, AuditEvent{
		TenantID: zeroTenant, Channel: "mock", UserID: "u1",
		Decision: "deny", ErrorType: "budget_exceeded", TraceID: tag + "-sync",
	}); err != nil {
		t.Fatal(err)
	}

	count := func() int {
		var n int
		if err := pool.QueryRow(ctx,
			"SELECT count(*) FROM audit_log WHERE trace_id LIKE $1", tag+"%").Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	if n := count(); n != 1 {
		t.Fatalf("sync deny should be visible immediately, got %d", n)
	}

	// Async events land after the flush tick.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && count() != 4 {
		time.Sleep(100 * time.Millisecond)
	}
	if n := count(); n != 4 {
		t.Fatalf("expected 4 audit rows after flush, got %d", n)
	}

	// Close flushes whatever is still buffered.
	a.LogAsync(AuditEvent{
		TenantID: zeroTenant, Channel: "mock", Decision: "allow", TraceID: tag + "-tail",
	})
	a.Close()
	if n := count(); n != 5 {
		t.Fatalf("expected 5 audit rows after close-flush, got %d", n)
	}
}
