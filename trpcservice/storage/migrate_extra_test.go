package storage_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/session"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// uniqueUUID returns a valid, collision-free uuid for fixture rows.
func uniqueUUID(prefix string) string {
	return prefix[:8] + "-0000-0000-0000-" + fmt.Sprintf("%012x", time.Now().UnixNano()%(1<<48))
}

// A non-positive observation window falls back to the 24h default.
func TestNewMigratorDefaults(t *testing.T) {
	_, pool := pgSessionService(t)
	m := storage.NewMigrator(pool, nil, nil, 0)
	if m.ObserveWindow != 24*time.Hour {
		t.Fatalf("default observe window must be 24h, got %s", m.ObserveWindow)
	}
	if m.Interval != 5*time.Second || m.BatchSize != 50 {
		t.Fatalf("unexpected cadence defaults: %s %d", m.Interval, m.BatchSize)
	}
}

// A postgres→redis migration exercises the mirror image of the redis→postgres
// case: PG enumeration, the redis copy path (create + append the missing
// tail), the tenant session count over SQL, and the read switch with the
// invalidation broadcast. BatchSize=1 forces the multi-batch backfill. A
// dedicated tenant keeps the enumeration scoped to this test's sessions.
func TestMigratorPostgresToRedis(t *testing.T) {
	pgSvc, pool := pgSessionService(t)
	rdb := redisOrSkipForMigrate(t)
	redisSvc := redisSessionService(t)
	ctx := context.Background()

	// Dedicated tenant + app: enumeratePG pages the whole tenant, so sharing
	// the fixture tenant with other tests would make batch boundaries random.
	tenantID := uniqueUUID("fffffff0")
	appID := uniqueUUID("fffffffe")
	if _, err := pool.Exec(ctx,
		`INSERT INTO tenant (id, name, status) VALUES ($1, 'migrate-pg2redis', 'active')`, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO agent_app (id, tenant_id, name, agent_type, config, version, status)
		 VALUES ($1, $2, 'migrate-pg2redis', 'llm', '{}', 1, 'published')`, appID, tenantID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM agent_app WHERE id = $1`, appID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM tenant WHERE id = $1`, tenantID)
	})

	key1 := session.Key{AppName: appID, UserID: "u-pg", SessionID: "dm:mock:migr-1-" + t.Name()}
	key2 := session.Key{AppName: appID, UserID: "u-pg", SessionID: "dm:mock:migr-2-" + t.Name()}
	for _, key := range []sessionKeyList{{k: key1, evts: 2}, {k: key2, evts: 1}} {
		cleanupSession(t, pool, key.k)
		t.Cleanup(func() { cleanupSession(t, pool, key.k) })
		t.Cleanup(func() { _ = redisSvc.DeleteSession(context.Background(), key.k) })

		sess, err := pgSvc.CreateSession(ctx, key.k, session.StateMap{})
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < key.evts; i++ {
			if err := pgSvc.AppendEvent(ctx, sess,
				textEvent("pgm-"+key.k.SessionID[:12]+string(rune('a'+i)), "user", "迁移消息")); err != nil {
				t.Fatal(err)
			}
		}
	}

	var migID string
	if err := pool.QueryRow(ctx,
		`INSERT INTO storage_migration (tenant_id, resource, from_backend, to_backend, phase)
		 VALUES ($1, 'session', 'postgres', 'redis', 'dual_write') RETURNING id`,
		tenantID).Scan(&migID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM storage_migration WHERE id = $1`, migID)
		_, _ = pool.Exec(context.Background(), `UPDATE tenant SET storage_config = NULL WHERE id = $1`, tenantID)
	})

	m := storage.NewMigrator(pool, rdb,
		map[string]session.Service{"postgres": pgSvc, "redis": redisSvc}, 50*time.Millisecond)
	m.BatchSize = 1

	// dual_write → backfilling.
	if err := m.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	// Two backfill ticks copy one session each (batch size 1), persisting
	// progress between them.
	if err := m.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if err := m.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	// The third backfill tick finds nothing left of the two sessions: the
	// consistency check passes and the read switch flips the tenant to redis.
	if err := m.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	var phase string
	if err := pool.QueryRow(ctx,
		`SELECT phase FROM storage_migration WHERE id = $1`, migID).Scan(&phase); err != nil {
		t.Fatal(err)
	}
	if phase != tenant.PhaseObserving {
		t.Fatalf("want phase observing after the read switch, got %s", phase)
	}

	var storageCfg string
	if err := pool.QueryRow(ctx,
		`SELECT storage_config::text FROM tenant WHERE id = $1`, tenantID).Scan(&storageCfg); err != nil {
		t.Fatal(err)
	}
	if storageCfg != `{"session": {"type": "redis"}}` {
		t.Fatalf("tenant storage_config not switched: %s", storageCfg)
	}

	// Observation window is 50ms; the next tick finishes the migration.
	time.Sleep(80 * time.Millisecond)
	if err := m.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT phase FROM storage_migration WHERE id = $1`, migID).Scan(&phase); err != nil {
		t.Fatal(err)
	}
	if phase != tenant.PhaseDone {
		t.Fatalf("want phase done, got %s", phase)
	}

	// Both sessions (with their events) live on the redis target now.
	for _, want := range []sessionKeyList{{k: key1, evts: 2}, {k: key2, evts: 1}} {
		got, err := redisSvc.GetSession(ctx, want.k)
		if err != nil {
			t.Fatal(err)
		}
		if got == nil || len(got.Events) != want.evts {
			t.Fatalf("redis target session %s must carry %d events, got %+v",
				want.k.SessionID, want.evts, got)
		}
	}
}

type sessionKeyList struct {
	k    session.Key
	evts int
}

// A migration whose target backend is not wired parks the row in "failed"
// with the reason, and Run exits cleanly on cancellation.
func TestMigratorRunMarksBadBackendPairFailed(t *testing.T) {
	pgSvc, pool := pgSessionService(t)
	rdb := redisOrSkipForMigrate(t)
	ctx := context.Background()

	var migID string
	if err := pool.QueryRow(ctx,
		`INSERT INTO storage_migration (tenant_id, resource, from_backend, to_backend, phase)
		 VALUES ($1, 'session', 'postgres', 'backend-missing', 'backfilling') RETURNING id`,
		testTenantID).Scan(&migID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM storage_migration WHERE id = $1`, migID)
	})

	m := storage.NewMigrator(pool, rdb,
		map[string]session.Service{"postgres": pgSvc}, time.Hour)
	m.Interval = 20 * time.Millisecond

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		m.Run(runCtx)
		close(done)
	}()

	// The tick fails the migration; Run keeps ticking until cancel.
	deadline := time.Now().Add(5 * time.Second)
	var phase, failure string
	for {
		if err := pool.QueryRow(ctx,
			`SELECT phase, COALESCE(error, '') FROM storage_migration WHERE id = $1`,
			migID).Scan(&phase, &failure); err != nil {
			t.Fatal(err)
		}
		if phase == tenant.PhaseFailed {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("migration never failed, last phase %s", phase)
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
	if !strings.Contains(failure, "backend pair") {
		t.Fatalf("the failure reason must name the backend pair, got %q", failure)
	}
}
