package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// PGSessionService implements the framework session.Service over the
// platform's session / session_event / summary tables — the "event append +
// state snapshot" two-layer store of design decision 2:
//
//   - session_event is append-only; the UNIQUE (session_id, event_seq)
//     constraint keeps events ordered without duplicates and is the
//     execution-layer idempotency backstop for Stream redeliveries
//     (ON CONFLICT DO NOTHING: a late duplicate is dropped, never an error).
//   - session.state is a materialized snapshot for fast reads; the event
//     stream is the source of truth a crashed session can be replayed from.
//   - App/user-scoped state (app:/user: prefixes) is not supported by this
//     backend: the platform's agent definitions don't use those scopes.
//
// Key mapping: framework Key{AppName, UserID, SessionID} →
// session{app_id, user_id, session_key}. AppName must be the agent_app UUID
// until per-app Runner assembly lands.
type PGSessionService struct {
	pool *pgxpool.Pool

	mu       sync.Mutex
	tenantOf map[string]string // app_id → tenant_id cache
}

// NewPGSessionService creates the service on an established pool.
func NewPGSessionService(pool *pgxpool.Pool) *PGSessionService {
	return &PGSessionService{pool: pool, tenantOf: make(map[string]string)}
}

// ErrStateScopeUnsupported is returned by the app/user-scoped state methods:
// this backend stores session-scoped state only.
var ErrStateScopeUnsupported = errors.New("pg session: app/user state scopes are not supported")

// CreateSession implements session.Service. An existing session for the same
// (app_id, session_key) is returned as-is (state is not overwritten).
func (s *PGSessionService) CreateSession(ctx context.Context, key session.Key, state session.StateMap, _ ...session.Option) (*session.Session, error) {
	if err := key.CheckSessionKey(); err != nil {
		return nil, err
	}
	tenantID, err := s.tenantForApp(ctx, key.AppName)
	if err != nil {
		return nil, err
	}
	stateJSON, err := encodeState(state)
	if err != nil {
		return nil, err
	}

	sess := &session.Session{
		ID: key.SessionID, AppName: key.AppName, UserID: key.UserID,
		State: session.StateMap{},
	}
	err = s.pool.QueryRow(ctx,
		`INSERT INTO session (tenant_id, app_id, session_key, user_id, channel, state)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (app_id, session_key) DO NOTHING
		 RETURNING created_at, updated_at`,
		tenantID, key.AppName, key.SessionID, key.UserID, channelOf(key.SessionID), stateJSON,
	).Scan(&sess.CreatedAt, &sess.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		// Existing session: return it (events are loaded by GetSession).
		existing, err := s.GetSession(ctx, key)
		if err != nil {
			return nil, err
		}
		return existing, nil
	}
	if err != nil {
		return nil, fmt.Errorf("insert session: %w", err)
	}
	for k, v := range state {
		sess.State[k] = v
	}
	return sess, nil
}

// GetSession implements session.Service: loads the snapshot and replays
// events from the journal. A missing session returns (nil, nil).
func (s *PGSessionService) GetSession(ctx context.Context, key session.Key, opts ...session.Option) (*session.Session, error) {
	if err := key.CheckSessionKey(); err != nil {
		return nil, err
	}
	var (
		stateRaw         []byte
		sessID           string
		createdAt, updAt time.Time
	)
	err := s.pool.QueryRow(ctx,
		`SELECT id, state, created_at, updated_at FROM session
		 WHERE app_id = $1 AND session_key = $2`,
		key.AppName, key.SessionID,
	).Scan(&sessID, &stateRaw, &createdAt, &updAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query session: %w", err)
	}

	state, err := decodeState(stateRaw)
	if err != nil {
		return nil, err
	}
	sess := &session.Session{
		ID: key.SessionID, AppName: key.AppName, UserID: key.UserID,
		State: state, CreatedAt: createdAt, UpdatedAt: updAt,
	}
	events, err := s.loadEvents(ctx, sessID, opts...)
	if err != nil {
		return nil, err
	}
	sess.Events = events
	return sess, nil
}

// ListSessions implements session.Service.
func (s *PGSessionService) ListSessions(ctx context.Context, userKey session.UserKey, opts ...session.Option) ([]*session.Session, error) {
	if err := userKey.CheckUserKey(); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx,
		`SELECT id, session_key, created_at, updated_at FROM session
		 WHERE app_id = $1 AND user_id = $2 ORDER BY updated_at DESC`,
		userKey.AppName, userKey.UserID)
	if err != nil {
		return nil, fmt.Errorf("query sessions: %w", err)
	}
	defer rows.Close()

	onlyMeta := applySessionOpts(opts).ListSessionOnlyMeta
	var out []*session.Session
	for rows.Next() {
		var sessID, sessionKey string
		var createdAt, updatedAt time.Time
		if err := rows.Scan(&sessID, &sessionKey, &createdAt, &updatedAt); err != nil {
			return nil, fmt.Errorf("scan session: %w", err)
		}
		sess := &session.Session{
			ID: sessionKey, AppName: userKey.AppName, UserID: userKey.UserID,
			CreatedAt: createdAt, UpdatedAt: updatedAt,
		}
		if !onlyMeta {
			events, err := s.loadEvents(ctx, sessID)
			if err != nil {
				return nil, err
			}
			sess.Events = events
		}
		out = append(out, sess)
	}
	return out, rows.Err()
}

// DeleteSession implements session.Service: events and summary go first
// (FK children), then the session row.
func (s *PGSessionService) DeleteSession(ctx context.Context, key session.Key, _ ...session.Option) error {
	if err := key.CheckSessionKey(); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var sessID string
	err = tx.QueryRow(ctx,
		`DELETE FROM session WHERE app_id = $1 AND session_key = $2 RETURNING id`,
		key.AppName, key.SessionID).Scan(&sessID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil // already gone
	}
	if err != nil {
		return fmt.Errorf("delete session: %w", err)
	}
	for _, q := range []string{
		`DELETE FROM session_event WHERE session_id = $1`,
		`DELETE FROM summary WHERE session_id = $1`,
	} {
		if _, err := tx.Exec(ctx, q, sessID); err != nil {
			return fmt.Errorf("delete children: %w", err)
		}
	}
	return tx.Commit(ctx)
}

// AppendEvent implements session.Service: the canonical in-memory update runs
// first (framework semantics), then event + snapshot persist in one
// transaction. The session row is locked FOR UPDATE so concurrent appends
// serialize and event_seq stays gapless; the unique constraint is the
// last-resort backstop if the session lock is ever lost.
func (s *PGSessionService) AppendEvent(ctx context.Context, sess *session.Session, e *event.Event, opts ...session.Option) error {
	if sess == nil || e == nil {
		return errors.New("pg session: nil session or event")
	}
	key := session.Key{AppName: sess.AppName, UserID: sess.UserID, SessionID: sess.ID}
	if err := key.CheckSessionKey(); err != nil {
		return err
	}

	before := sess.GetEventCount()
	sess.UpdateUserSession(e, opts...) // append valid events + apply state delta
	journal := sess.GetEventCount() > before

	eventJSON, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	stateJSON, err := encodeState(sess.State)
	if err != nil {
		return err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	sessID, err := s.ensureSession(ctx, tx, key, stateJSON)
	if err != nil {
		return err
	}
	if journal {
		var seq int64
		if err := tx.QueryRow(ctx,
			`SELECT COALESCE(MAX(event_seq), 0) + 1 FROM session_event WHERE session_id = $1`,
			sessID).Scan(&seq); err != nil {
			return fmt.Errorf("next event_seq: %w", err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO session_event (session_id, event_seq, event) VALUES ($1, $2, $3)
			 ON CONFLICT (session_id, event_seq) DO NOTHING`,
			sessID, seq, eventJSON); err != nil {
			return fmt.Errorf("append event: %w", err)
		}
	}
	if _, err := tx.Exec(ctx,
		`UPDATE session SET state = $2, updated_at = now() WHERE id = $1`,
		sessID, stateJSON); err != nil {
		return fmt.Errorf("update snapshot: %w", err)
	}
	return tx.Commit(ctx)
}

// UpdateSessionState implements session.Service: snapshot-only merge without
// appending an event. Keys with app:/user: prefixes are rejected (scoped
// methods are unsupported); a nil value deletes the key.
func (s *PGSessionService) UpdateSessionState(ctx context.Context, key session.Key, state session.StateMap) error {
	if err := key.CheckSessionKey(); err != nil {
		return err
	}
	for k := range state {
		if strings.HasPrefix(k, session.StateAppPrefix) || strings.HasPrefix(k, session.StateUserPrefix) {
			return fmt.Errorf("pg session: key %q needs the app/user-scoped methods, which are unsupported", k)
		}
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	sessID, err := s.ensureSession(ctx, tx, key, nil)
	if err != nil {
		return err
	}
	var stateRaw []byte
	if err := tx.QueryRow(ctx, `SELECT state FROM session WHERE id = $1 FOR UPDATE`, sessID).Scan(&stateRaw); err != nil {
		return fmt.Errorf("lock snapshot: %w", err)
	}
	current, err := decodeState(stateRaw)
	if err != nil {
		return err
	}
	for k, v := range state {
		if v == nil {
			delete(current, k)
		} else {
			current[k] = v
		}
	}
	merged, err := encodeState(current)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE session SET state = $2, updated_at = now() WHERE id = $1`, sessID, merged); err != nil {
		return fmt.Errorf("update snapshot: %w", err)
	}
	return tx.Commit(ctx)
}

// CreateSessionSummary implements session.Service as a no-op: summarization
// only runs when a summarizer is configured (arrives with the Memory/Knowledge
// stage); the summary table and its covered_event_id cursor are already here.
func (s *PGSessionService) CreateSessionSummary(_ context.Context, _ *session.Session, _ string, _ bool) error {
	return nil
}

// EnqueueSummaryJob implements session.Service as a no-op (see CreateSessionSummary).
func (s *PGSessionService) EnqueueSummaryJob(_ context.Context, _ *session.Session, _ string, _ bool) error {
	return nil
}

// GetSessionSummaryText implements session.Service: reads the summary table.
func (s *PGSessionService) GetSessionSummaryText(ctx context.Context, sess *session.Session, _ ...session.SummaryOption) (string, bool) {
	if sess == nil {
		return "", false
	}
	var text string
	err := s.pool.QueryRow(ctx,
		`SELECT summary_text FROM summary s
		 JOIN session se ON se.id = s.session_id
		 WHERE se.app_id = $1 AND se.session_key = $2`,
		sess.AppName, sess.ID).Scan(&text)
	if err != nil {
		return "", false
	}
	return text, true
}

// UpdateAppState implements session.Service.
func (s *PGSessionService) UpdateAppState(context.Context, string, session.StateMap) error {
	return ErrStateScopeUnsupported
}

// DeleteAppState implements session.Service.
func (s *PGSessionService) DeleteAppState(context.Context, string, string) error {
	return ErrStateScopeUnsupported
}

// ListAppStates implements session.Service.
func (s *PGSessionService) ListAppStates(context.Context, string) (session.StateMap, error) {
	return nil, ErrStateScopeUnsupported
}

// UpdateUserState implements session.Service.
func (s *PGSessionService) UpdateUserState(context.Context, session.UserKey, session.StateMap) error {
	return ErrStateScopeUnsupported
}

// DeleteUserState implements session.Service.
func (s *PGSessionService) DeleteUserState(context.Context, session.UserKey, string) error {
	return ErrStateScopeUnsupported
}

// ListUserStates implements session.Service.
func (s *PGSessionService) ListUserStates(context.Context, session.UserKey) (session.StateMap, error) {
	return nil, ErrStateScopeUnsupported
}

// Close implements session.Service; the pool is owned by the caller.
func (s *PGSessionService) Close() error { return nil }

// ensureSession returns the internal session UUID, inserting the row when
// missing, and locks it FOR UPDATE (must run inside a transaction).
func (s *PGSessionService) ensureSession(ctx context.Context, tx pgx.Tx, key session.Key, stateJSON []byte) (string, error) {
	var sessID string
	err := tx.QueryRow(ctx,
		`SELECT id FROM session WHERE app_id = $1 AND session_key = $2 FOR UPDATE`,
		key.AppName, key.SessionID).Scan(&sessID)
	if err == nil {
		return sessID, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("lock session: %w", err)
	}

	tenantID, err := s.tenantForApp(ctx, key.AppName)
	if err != nil {
		return "", err
	}
	if stateJSON == nil {
		stateJSON = []byte(`{}`)
	}
	err = tx.QueryRow(ctx,
		`INSERT INTO session (tenant_id, app_id, session_key, user_id, channel, state)
		 VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
		tenantID, key.AppName, key.SessionID, key.UserID, channelOf(key.SessionID), stateJSON,
	).Scan(&sessID)
	if err != nil {
		return "", fmt.Errorf("insert session: %w", err)
	}
	return sessID, nil
}

// loadEvents replays the journal in event_seq order, applying the EventTime /
// EventNum options in memory (per-session volumes are modest).
func (s *PGSessionService) loadEvents(ctx context.Context, sessID string, opts ...session.Option) ([]event.Event, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT event FROM session_event WHERE session_id = $1 ORDER BY event_seq`, sessID)
	if err != nil {
		return nil, fmt.Errorf("query events: %w", err)
	}
	defer rows.Close()

	var events []event.Event
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		var e event.Event
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, fmt.Errorf("decode event: %w", err)
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	o := applySessionOpts(opts)
	if !o.EventTime.IsZero() {
		kept := events[:0]
		for _, e := range events {
			if !e.Timestamp.Before(o.EventTime) {
				kept = append(kept, e)
			}
		}
		events = kept
	}
	if o.EventNum > 0 && len(events) > o.EventNum {
		events = events[len(events)-o.EventNum:]
	}
	return events, nil
}

// tenantForApp resolves a session row's tenant_id from its agent_app, cached
// per process (app → tenant mapping changes only via deployment config).
func (s *PGSessionService) tenantForApp(ctx context.Context, appID string) (string, error) {
	s.mu.Lock()
	cached, ok := s.tenantOf[appID]
	s.mu.Unlock()
	if ok {
		return cached, nil
	}
	var tenantID string
	if err := s.pool.QueryRow(ctx,
		`SELECT tenant_id FROM agent_app WHERE id = $1`, appID).Scan(&tenantID); err != nil {
		return "", fmt.Errorf("resolve tenant for app %s: %w", appID, err)
	}
	s.mu.Lock()
	s.tenantOf[appID] = tenantID
	s.mu.Unlock()
	return tenantID, nil
}

// channelOf extracts the channel segment from a session key
// (dm:{channel}:{user} / group:{channel}:{chat}); "unknown" when malformed.
func channelOf(sessionKey string) string {
	parts := strings.SplitN(sessionKey, ":", 3)
	if len(parts) >= 2 {
		return parts[1]
	}
	return "unknown"
}

// applySessionOpts evaluates the framework's functional options.
func applySessionOpts(opts []session.Option) *session.Options {
	o := &session.Options{}
	for _, fn := range opts {
		if fn != nil {
			fn(o)
		}
	}
	return o
}

// encodeState serializes the state snapshot; values are raw JSON by framework
// convention, so they embed cleanly into the jsonb column.
func encodeState(state session.StateMap) ([]byte, error) {
	raw := make(map[string]json.RawMessage, len(state))
	for k, v := range state {
		if len(v) == 0 {
			continue
		}
		raw[k] = json.RawMessage(v)
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("marshal state: %w", err)
	}
	return data, nil
}

func decodeState(data []byte) (session.StateMap, error) {
	out := session.StateMap{}
	if len(data) == 0 {
		return out, nil
	}
	raw := make(map[string]json.RawMessage)
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("unmarshal state: %w", err)
	}
	for k, v := range raw {
		out[k] = append([]byte(nil), v...)
	}
	return out, nil
}
