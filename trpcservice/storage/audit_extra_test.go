package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

// A batch the database rejects is retried, then dropped loudly: the drop must
// land in the Dropped counter (the audit trail has holes — someone must look).
func TestAuditorFlushDropsAfterRetries(t *testing.T) {
	pool, err := NewPG(context.Background(), testPGDSN)
	if err != nil {
		t.Skipf("postgres unavailable (%v), skipping integration test", err)
	}
	a := NewAuditor(pool)
	pool.Close() // every insert attempt now fails

	a.flush([]AuditEvent{{
		TenantID: zeroTenant, Channel: "mock", UserID: "u1",
		Decision: "allow", TraceID: "flush-drop",
	}})
	if got := a.Dropped(); got != 1 {
		t.Fatalf("the rejected batch must be counted as dropped, got %d", got)
	}
}

// The change-audit columns survive a round trip: a non-empty SessionID and
// Detail are written (NULL otherwise).
func TestAuditorSyncWritesSessionIDAndDetail(t *testing.T) {
	pool, err := NewPG(context.Background(), testPGDSN)
	if err != nil {
		t.Skipf("postgres unavailable (%v), skipping integration test", err)
	}
	ctx := context.Background()
	tag := fmt.Sprintf("audit-detail-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM audit_log WHERE trace_id = $1", tag)
		pool.Close()
	})

	// A syntactically valid, unique uuid for the session_id column.
	sessUUID := fmt.Sprintf("00000000-0000-0000-0000-%012d", time.Now().UnixNano()%1e12)
	a := NewAuditor(pool)
	if err := a.LogSync(ctx, AuditEvent{
		TenantID: zeroTenant, Channel: "mock", UserID: "u1", SessionID: sessUUID,
		Decision: "review", Detail: json.RawMessage(`{"before":1,"after":2}`),
		TraceID: tag,
	}); err != nil {
		t.Fatal(err)
	}

	var sid *string
	var detail []byte
	if err := pool.QueryRow(ctx,
		`SELECT session_id, detail FROM audit_log WHERE trace_id = $1`, tag).Scan(&sid, &detail); err != nil {
		t.Fatal(err)
	}
	if sid == nil || *sid != sessUUID {
		t.Fatalf("session_id must persist, got %v", sid)
	}
	var decoded map[string]int
	if err := json.Unmarshal(detail, &decoded); err != nil {
		t.Fatalf("detail must be valid json, got %s: %v", detail, err)
	}
	if decoded["before"] != 1 || decoded["after"] != 2 {
		t.Fatalf("detail payload mismatch: %v", decoded)
	}

	// A zero uuid stands in for "no session": it must persist as NULL.
	tagNull := tag + "-null"
	if err := a.LogSync(ctx, AuditEvent{
		TenantID: zeroTenant, Channel: "mock", Decision: "allow", TraceID: tagNull,
	}); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT session_id, detail FROM audit_log WHERE trace_id = $1`, tagNull).Scan(&sid, &detail); err != nil {
		t.Fatal(err)
	}
	if sid != nil {
		t.Fatalf("empty SessionID must persist as NULL, got %v", *sid)
	}
	if len(detail) != 0 {
		t.Fatalf("empty Detail must persist as NULL, got %s", detail)
	}
}
