package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"

	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// Migrator executes tenant backend migrations, one row of
// storage_migration at a time:
//
//	dual_write → backfilling → observing → done
//
// Dual write is live from row creation (the assembler fans writes out for
// tenants with an active migration). The Migrator drives the rest: copy the
// tenant's sessions from the old backend to the new (batched, resumable by
// cursor), run the consistency check (per-session event counts must match),
// flip tenant.storage_config (read switch), keep dual write through the
// observation window, then finish. Any failure parks the row in "failed"
// with the reason — reads never switch on a failed check.
type Migrator struct {
	pool     *pgxpool.Pool
	rdb      *redis.Client // enumerate + publish invalidation on read switch
	backends map[string]session.Service

	// ObserveWindow is how long dual write continues after the read switch.
	// Interval is the tick cadence; BatchSize caps sessions copied per tick.
	ObserveWindow time.Duration
	Interval      time.Duration
	BatchSize     int
}

// migrationProgress is the progress jsonb payload.
type migrationProgress struct {
	SessionsTotal int       `json:"sessions_total"`
	SessionsDone  int       `json:"sessions_done"`
	Cursor        string    `json:"cursor,omitempty"`     // last copied session_key (PG source pagination)
	Mismatches    []string  `json:"mismatches,omitempty"` // consistency check failures
	ObserveUntil  time.Time `json:"observe_until,omitempty"`
}

// NewMigrator creates the executor. backends maps "redis"/"postgres" to the
// process's session services (the same instances the assembler routes to).
func NewMigrator(pool *pgxpool.Pool, rdb *redis.Client, backends map[string]session.Service, observeWindow time.Duration) *Migrator {
	if observeWindow <= 0 {
		observeWindow = 24 * time.Hour
	}
	return &Migrator{
		pool: pool, rdb: rdb, backends: backends,
		ObserveWindow: observeWindow, Interval: 5 * time.Second, BatchSize: 50,
	}
}

// Run ticks until ctx is canceled.
func (m *Migrator) Run(ctx context.Context) {
	ticker := time.NewTicker(m.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.Tick(ctx); err != nil {
				plog.Errorf("migration tick: %v", err)
			}
		}
	}
}

// Tick advances every active migration one step, inside one transaction that
// claims the rows with FOR UPDATE SKIP LOCKED: replicas of the
// worker role run the same Migrator, so each tick must claim a disjoint set
// instead of two replicas double-advancing one migration (double backfill
// batches, read switches racing the consistency check). The phase/progress
// writes ride the same transaction, so a crashed tick rolls back cleanly and
// the next claimant re-advances from the last committed state. Exported for
// tests.
func (m *Migrator) Tick(ctx context.Context) error {
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration tick: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx,
		`SELECT id, tenant_id, resource, from_backend, to_backend, phase, progress
		 FROM storage_migration
		 WHERE phase NOT IN ('done', 'failed', 'aborted') ORDER BY created_at
		 FOR UPDATE SKIP LOCKED`)
	if err != nil {
		return fmt.Errorf("query migrations: %w", err)
	}
	var migs []migrationRow
	for rows.Next() {
		var r migrationRow
		var progress []byte
		if err := rows.Scan(&r.ID, &r.TenantID, &r.Resource, &r.From, &r.To, &r.Phase, &progress); err != nil {
			rows.Close()
			return fmt.Errorf("scan migration: %w", err)
		}
		if len(progress) > 0 {
			_ = json.Unmarshal(progress, &r.Progress)
		}
		migs = append(migs, r)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate migrations: %w", err)
	}
	rows.Close()

	for _, mig := range migs {
		if err := m.advance(ctx, tx, mig); err != nil {
			plog.Errorf("migration %s (%s) failed: %v", mig.ID, mig.Phase, err)
			m.fail(ctx, tx, mig, err)
		}
	}
	return tx.Commit(ctx)
}

// migrationRow is one storage_migration row.
type migrationRow struct {
	ID, TenantID, Resource, From, To, Phase string
	Progress                                migrationProgress
}

func (m *Migrator) advance(ctx context.Context, tx pgx.Tx, mig migrationRow) error {
	switch mig.Phase {
	case tenant.PhaseDualWrite:
		// Dual write has been live since the row was created; start copying.
		return m.setPhase(ctx, tx, mig.ID, tenant.PhaseBackfilling, mig.Progress)
	case tenant.PhaseBackfilling:
		return m.backfill(ctx, tx, mig)
	case tenant.PhaseObserving:
		if time.Now().After(mig.Progress.ObserveUntil) {
			plog.Infof("migration %s: observation window passed, done", mig.ID)
			return m.setPhase(ctx, tx, mig.ID, tenant.PhaseDone, mig.Progress)
		}
	}
	return nil
}

// backfill copies one batch of sessions from the source backend to the
// target; when all are copied, the consistency check gates the read switch.
// Session data moves on the pool (independent transactions per session);
// only the migration row's own phase/progress rides the claim tx.
func (m *Migrator) backfill(ctx context.Context, tx pgx.Tx, mig migrationRow) error {
	src, dst := m.backends[mig.From], m.backends[mig.To]
	if src == nil || dst == nil {
		return fmt.Errorf("backend pair %s→%s not both available", mig.From, mig.To)
	}

	sessions, err := m.enumerate(ctx, mig, mig.Progress.Cursor)
	if err != nil {
		return err
	}
	for _, key := range sessions {
		if err := m.copySession(ctx, mig, src, dst, key); err != nil {
			return fmt.Errorf("copy session %s: %w", key.SessionID, err)
		}
		mig.Progress.Cursor = key.SessionID
		mig.Progress.SessionsDone++
	}
	if len(sessions) > 0 || mig.Progress.SessionsTotal == 0 {
		// Refresh the total estimate once per batch.
		total, err := m.countTenantSessions(ctx, mig)
		if err == nil {
			mig.Progress.SessionsTotal = total
		}
	}

	if int64(len(sessions)) == int64(m.BatchSize) {
		// Probably more to come; persist progress and continue next tick.
		return m.setPhase(ctx, tx, mig.ID, tenant.PhaseBackfilling, mig.Progress)
	}

	// Everything copied: consistency check gates the read switch;
	// per-session event counts must match.
	mismatches, err := m.checkConsistency(ctx, mig, src, dst)
	if err != nil {
		return err
	}
	mig.Progress.Mismatches = mismatches
	if len(mismatches) > 0 {
		return fmt.Errorf("consistency check failed for %d sessions: %v", len(mismatches), mismatches)
	}
	return m.readSwitch(ctx, tx, mig)
}

// readSwitch points the tenant's storage_config at the new backend, notifies
// workers through the invalidation channel, and starts the observation window.
// Both row updates ride the claim tx; the broadcast is best-effort and may
// fire a beat before the commit — a worker that refreshes early just sees the
// old config and waits for the TTL.
func (m *Migrator) readSwitch(ctx context.Context, tx pgx.Tx, mig migrationRow) error {
	backendJSON, err := json.Marshal(map[string]string{"type": mig.To})
	if err != nil {
		return fmt.Errorf("encode backend config: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE tenant SET storage_config = jsonb_set(COALESCE(storage_config, '{}'), '{session}', $2::jsonb),
		 updated_at = now() WHERE id = $1`,
		mig.TenantID, backendJSON); err != nil {
		return fmt.Errorf("switch storage_config: %w", err)
	}
	mig.Progress.ObserveUntil = time.Now().Add(m.ObserveWindow)
	if err := m.setPhase(ctx, tx, mig.ID, tenant.PhaseObserving, mig.Progress); err != nil {
		return err
	}
	if err := tenant.PublishInvalidation(ctx, m.rdb); err != nil {
		plog.Warnf("migration %s: invalidation broadcast failed (TTL fallback): %v", mig.ID, err)
	}
	plog.Infof("migration %s: reads switched %s→%s for tenant %s, observing until %s",
		mig.ID, mig.From, mig.To, mig.TenantID, mig.Progress.ObserveUntil.Format(time.RFC3339))
	return nil
}

// enumerate lists one batch of the tenant's sessions on the source backend,
// after the cursor.
func (m *Migrator) enumerate(ctx context.Context, mig migrationRow, cursor string) ([]session.Key, error) {
	if mig.From == "postgres" {
		return m.enumeratePG(ctx, mig.TenantID, cursor)
	}
	return m.enumerateRedis(ctx, mig.TenantID, cursor)
}

// enumeratePG pages the session table by session_key.
func (m *Migrator) enumeratePG(ctx context.Context, tenantID, cursor string) ([]session.Key, error) {
	rows, err := m.pool.Query(ctx,
		`SELECT app_id, session_key, user_id FROM session
		 WHERE tenant_id = $1 AND session_key > $2 ORDER BY session_key LIMIT $3`,
		tenantID, cursor, m.BatchSize)
	if err != nil {
		return nil, fmt.Errorf("enumerate pg sessions: %w", err)
	}
	defer rows.Close()
	var keys []session.Key
	for rows.Next() {
		var k session.Key
		if err := rows.Scan(&k.AppName, &k.SessionID, &k.UserID); err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	return keys, rows.Err()
}

// enumerateRedis scans the framework's hashidx session keys
// (hashidx:meta:{app}:{user}:{sess}, the default storage layout of
// session/redis v1.11) for the tenant's apps. The cursor is the last copied
// session key; enumeration re-scans and skips up to it.
func (m *Migrator) enumerateRedis(ctx context.Context, tenantID, cursor string) ([]session.Key, error) {
	appIDs, err := m.tenantApps(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	var all []session.Key
	for _, appID := range appIDs {
		prefix := fmt.Sprintf("hashidx:meta:%s:", appID)
		iter := m.rdb.Scan(ctx, 0, prefix+"*", 1000).Iterator()
		for iter.Next(ctx) {
			// Strip "hashidx:meta:{app}:" → "{user}:{sess}".
			rest := iter.Val()[len(prefix):]
			end := strings.IndexByte(rest, '}')
			if !strings.HasPrefix(rest, "{") || end < 0 || len(rest) <= end+2 {
				continue // unknown key shape; never ours
			}
			all = append(all, session.Key{
				AppName: appID, UserID: rest[1:end], SessionID: rest[end+2:],
			})
		}
		if err := iter.Err(); err != nil {
			return nil, fmt.Errorf("scan redis sessions: %w", err)
		}
	}
	// Deterministic order + cursor skip.
	for i := 1; i < len(all); i++ { // insertion sort; batches are small
		for j := i; j > 0 && all[j].SessionID < all[j-1].SessionID; j-- {
			all[j], all[j-1] = all[j-1], all[j]
		}
	}
	var out []session.Key
	for _, k := range all {
		if k.SessionID > cursor {
			out = append(out, k)
		}
	}
	if len(out) > m.BatchSize {
		out = out[:m.BatchSize]
	}
	return out, nil
}

// copySession copies one session, resuming partial copies: the target's
// existing event prefix is skipped (events are append-only and ordered, and
// both targets dedupe — PG via the (session_id, event_seq) constraint, redis
// via zset member equality on identical event JSON).
func (m *Migrator) copySession(ctx context.Context, mig migrationRow, src, dst session.Service, key session.Key) error {
	srcSess, err := src.GetSession(ctx, key)
	if err != nil {
		return fmt.Errorf("read source: %w", err)
	}
	if srcSess == nil {
		return nil // vanished between enumerate and copy
	}
	dstSess, err := dst.GetSession(ctx, key)
	if err != nil {
		return fmt.Errorf("read target: %w", err)
	}
	existing := 0
	if dstSess != nil {
		existing = len(dstSess.Events)
	}

	if mig.To == "postgres" {
		return m.writeSessionToPG(ctx, mig.TenantID, srcSess)
	}
	// Redis target: create (idempotent) then append the missing tail.
	// AppendEvent mutates the carrier session's event list, so the events are
	// snapshotted first (otherwise the loop's slice grows under the iteration
	// and never terminates) and the destination's own session object is the
	// carrier.
	events := append([]event.Event(nil), srcSess.Events...)
	dstSessNew, err := dst.CreateSession(ctx, key, srcSess.State)
	if err != nil {
		return fmt.Errorf("create target session: %w", err)
	}
	for i := existing; i < len(events); i++ {
		evt := events[i]
		if err := dst.AppendEvent(ctx, dstSessNew, &evt); err != nil {
			return fmt.Errorf("append event %d: %w", i, err)
		}
	}
	return nil
}

// writeSessionToPG writes the session row + all events with their original
// order as event_seq, idempotently (ON CONFLICT DO NOTHING on both tables).
func (m *Migrator) writeSessionToPG(ctx context.Context, tenantID string, sess *session.Session) error {
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	stateJSON, err := encodeState(sess.State)
	if err != nil {
		return err
	}
	var sessID string
	err = tx.QueryRow(ctx,
		`INSERT INTO session (tenant_id, app_id, session_key, user_id, channel, state)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (app_id, session_key) DO UPDATE SET session_key = EXCLUDED.session_key
		 RETURNING id`,
		tenantID, sess.AppName, sess.ID, sess.UserID, channelOf(sess.ID), stateJSON).Scan(&sessID)
	if err != nil {
		return fmt.Errorf("upsert session: %w", err)
	}
	for i, evt := range sess.Events {
		raw, err := json.Marshal(evt)
		if err != nil {
			return fmt.Errorf("marshal event %d: %w", i, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO session_event (session_id, event_seq, event) VALUES ($1, $2, $3)
			 ON CONFLICT (session_id, event_seq) DO NOTHING`,
			sessID, i+1, raw); err != nil {
			return fmt.Errorf("insert event %d: %w", i+1, err)
		}
	}
	return tx.Commit(ctx)
}

// checkConsistency compares per-session event counts between the backends.
// Returns the mismatching session keys.
func (m *Migrator) checkConsistency(ctx context.Context, mig migrationRow, src, dst session.Service) ([]string, error) {
	keys, err := m.enumerateAll(ctx, mig)
	if err != nil {
		return nil, err
	}
	var mismatches []string
	for _, key := range keys {
		srcCount, err := countEvents(ctx, src, key)
		if err != nil {
			return nil, err
		}
		dstCount, err := countEvents(ctx, dst, key)
		if err != nil {
			return nil, err
		}
		if srcCount != dstCount {
			mismatches = append(mismatches, fmt.Sprintf("%s(%d!=%d)", key.SessionID, srcCount, dstCount))
		}
	}
	return mismatches, nil
}

// enumerateAll lists every session of the tenant (no batching): consistency
// comparison is full.
func (m *Migrator) enumerateAll(ctx context.Context, mig migrationRow) ([]session.Key, error) {
	var all []session.Key
	cursor := ""
	for {
		batch, err := m.enumerate(ctx, mig, cursor)
		if err != nil {
			return nil, err
		}
		if len(batch) == 0 {
			return all, nil
		}
		all = append(all, batch...)
		cursor = batch[len(batch)-1].SessionID
	}
}

// countEvents reads the session and counts journaled events.
func countEvents(ctx context.Context, svc session.Service, key session.Key) (int, error) {
	sess, err := svc.GetSession(ctx, key)
	if err != nil {
		return 0, err
	}
	if sess == nil {
		return 0, nil
	}
	return len(sess.Events), nil
}

func (m *Migrator) countTenantSessions(ctx context.Context, mig migrationRow) (int, error) {
	if mig.From == "postgres" {
		var n int
		if err := m.pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM session WHERE tenant_id = $1`, mig.TenantID).Scan(&n); err != nil {
			return 0, err
		}
		return n, nil
	}
	keys, err := m.enumerateRedis(ctx, mig.TenantID, "")
	if err != nil {
		return 0, err
	}
	return len(keys), nil
}

// tenantApps returns the tenant's app IDs (redis session keys are per app).
func (m *Migrator) tenantApps(ctx context.Context, tenantID string) ([]string, error) {
	rows, err := m.pool.Query(ctx, `SELECT id FROM agent_app WHERE tenant_id = $1`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (m *Migrator) setPhase(ctx context.Context, tx pgx.Tx, id, phase string, progress migrationProgress) error {
	raw, err := json.Marshal(progress)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx,
		`UPDATE storage_migration SET phase = $2, progress = $3, updated_at = now() WHERE id = $1`,
		id, phase, raw)
	return err
}

// fail parks the migration with the error; reads stay on the old backend.
// Runs on the claim tx so the marking commits (or rolls back) with the tick —
// if the tx is already aborted the exec fails and the row is retried next
// tick, the log line keeps that visible.
func (m *Migrator) fail(ctx context.Context, tx pgx.Tx, mig migrationRow, cause error) {
	if _, err := tx.Exec(ctx,
		`UPDATE storage_migration SET phase = 'failed', error = $2, updated_at = now() WHERE id = $1`,
		mig.ID, cause.Error()); err != nil {
		plog.Errorf("migration %s: fail-marking failed: %v", mig.ID, err)
	}
}
