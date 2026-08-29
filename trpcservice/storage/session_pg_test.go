package storage_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

// Test fixtures use their own tenant/app rows so tests are independent of the
// demo seed. Needs the PG from compose; skips when unreachable.
const (
	testTenantID = "00000000-0000-0000-0000-0000000000aa"
	testAppID    = "00000000-0000-0000-0000-0000000001aa"
)

func pgSessionService(t *testing.T) (*storage.PGSessionService, *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	pool, err := storage.NewPG(ctx, "postgres://trpc:trpc-dev-only@localhost:5432/trpc?sslmode=disable")
	if err != nil {
		t.Skipf("postgres unavailable (%v), skipping integration test", err)
	}
	t.Cleanup(func() { pool.Close() })

	if _, err := pool.Exec(ctx,
		`INSERT INTO tenant (id, name, status) VALUES ($1, 'pg-session-test', 'active') ON CONFLICT (id) DO NOTHING`,
		testTenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO agent_app (id, tenant_id, name, agent_type, config, version, status)
		 VALUES ($1, $2, 'pg-session-test', 'llm', '{}', 1, 'published') ON CONFLICT DO NOTHING`,
		testAppID, testTenantID); err != nil {
		t.Fatal(err)
	}
	return storage.NewPGSessionService(pool), pool
}

func testKey(suffix string) session.Key {
	return session.Key{AppName: testAppID, UserID: "u-pg", SessionID: "dm:mock:pg-" + suffix}
}

func textEvent(id, author, content string) *event.Event {
	return &event.Event{
		ID:       id,
		Author:   author,
		Response: &model.Response{Choices: []model.Choice{{Message: model.Message{Role: model.RoleUser, Content: content}}}},
	}
}

func cleanupSession(t *testing.T, pool *pgxpool.Pool, key session.Key) {
	t.Helper()
	ctx := context.Background()
	_, _ = pool.Exec(ctx, `DELETE FROM session_event WHERE session_id IN
		(SELECT id FROM session WHERE app_id=$1 AND session_key=$2)`, key.AppName, key.SessionID)
	_, _ = pool.Exec(ctx, `DELETE FROM session WHERE app_id=$1 AND session_key=$2`, key.AppName, key.SessionID)
}

func TestPGSessionEventJournalAndSnapshot(t *testing.T) {
	svc, pool := pgSessionService(t)
	ctx := context.Background()
	key := testKey(t.Name())
	cleanupSession(t, pool, key)
	t.Cleanup(func() { cleanupSession(t, pool, key) })

	sess, err := svc.CreateSession(ctx, key, session.StateMap{})
	if err != nil {
		t.Fatal(err)
	}

	// Append two events; the second carries a state delta.
	if err := svc.AppendEvent(ctx, sess, textEvent("e1", "user", "你好")); err != nil {
		t.Fatal(err)
	}
	e2 := textEvent("e2", "assistant", "你好！")
	e2.StateDelta = map[string][]byte{"mood": []byte(`"happy"`)}
	if err := svc.AppendEvent(ctx, sess, e2); err != nil {
		t.Fatal(err)
	}

	// Journal: two events with ordered seqs.
	var seqs []int64
	rows, err := pool.Query(ctx,
		`SELECT event_seq FROM session_event se JOIN session s ON s.id = se.session_id
		 WHERE s.app_id=$1 AND s.session_key=$2 ORDER BY event_seq`, key.AppName, key.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			t.Fatal(err)
		}
		seqs = append(seqs, seq)
	}
	rows.Close()
	if len(seqs) != 2 || seqs[0] != 1 || seqs[1] != 2 {
		t.Fatalf("want event_seq [1 2], got %v", seqs)
	}

	// Snapshot + replay: GetSession returns the state delta and both events.
	got, err := svc.GetSession(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || len(got.Events) != 2 {
		t.Fatalf("want 2 replayed events, got %+v", got)
	}
	if got.Events[0].ID != "e1" || got.Events[1].ID != "e2" {
		t.Fatalf("events out of order: %s %s", got.Events[0].ID, got.Events[1].ID)
	}
	if string(got.State["mood"]) != `"happy"` {
		t.Fatalf("snapshot missing state delta: %v", got.State)
	}

	// The (session_id, event_seq) unique constraint is the DB-level backstop:
	// a direct duplicate insert must be rejected.
	_, err = pool.Exec(ctx,
		`INSERT INTO session_event (session_id, event_seq, event)
		 SELECT session_id, event_seq, event FROM session_event se
		 JOIN session s ON s.id = se.session_id
		 WHERE s.app_id=$1 AND s.session_key=$2 AND event_seq=1`,
		key.AppName, key.SessionID)
	if err == nil {
		t.Fatal("duplicate (session_id, event_seq) insert must fail")
	}

	// EventNum window: only the last event.
	limited, err := svc.GetSession(ctx, key, session.WithEventNum(1))
	if err != nil {
		t.Fatal(err)
	}
	if len(limited.Events) != 1 || limited.Events[0].ID != "e2" {
		t.Fatalf("WithEventNum(1) mismatch: %+v", limited.Events)
	}
}

func TestPGSessionUpdateStateAndDelete(t *testing.T) {
	svc, pool := pgSessionService(t)
	ctx := context.Background()
	key := testKey(t.Name())
	cleanupSession(t, pool, key)
	t.Cleanup(func() { cleanupSession(t, pool, key) })

	if _, err := svc.CreateSession(ctx, key, session.StateMap{"a": []byte(`1`)}); err != nil {
		t.Fatal(err)
	}
	// Merge one key, delete another (nil value), reject scoped keys.
	if err := svc.UpdateSessionState(ctx, key, session.StateMap{"b": []byte(`2`), "a": nil}); err != nil {
		t.Fatal(err)
	}
	got, err := svc.GetSession(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got.State["a"]; ok {
		t.Fatal("nil value must delete the key")
	}
	if string(got.State["b"]) != `2` {
		t.Fatalf("merge failed: %v", got.State)
	}
	if err := svc.UpdateSessionState(ctx, key, session.StateMap{session.StateAppPrefix + "x": []byte(`1`)}); err == nil {
		t.Fatal("app: prefixed keys must be rejected")
	}

	if err := svc.DeleteSession(ctx, key); err != nil {
		t.Fatal(err)
	}
	got, err = svc.GetSession(ctx, key)
	if err != nil || got != nil {
		t.Fatalf("deleted session must return nil, got %+v, err %v", got, err)
	}
	var events int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM session_event se JOIN session s ON s.id=se.session_id
		 WHERE s.app_id=$1 AND s.session_key=$2`, key.AppName, key.SessionID).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 0 {
		t.Fatalf("events must be deleted with the session, got %d", events)
	}
}

func TestPGSessionUnsupportedScopes(t *testing.T) {
	svc, _ := pgSessionService(t)
	ctx := context.Background()
	if err := svc.UpdateAppState(ctx, testAppID, session.StateMap{}); err == nil {
		t.Fatal("app state scope must be unsupported")
	}
	if _, err := svc.ListUserStates(ctx, session.UserKey{AppName: testAppID, UserID: "u"}); err == nil {
		t.Fatal("user state scope must be unsupported")
	}
}
