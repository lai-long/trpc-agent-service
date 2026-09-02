package storage

import (
	"context"

	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"

	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
)

// FanoutSessionService dual-writes two session backends during a storage
// migration (design 5.2.6 双写): reads and summary queries go to the primary
// (the authoritative side of the current phase); writes go to both, with
// secondary failures logged rather than propagated — the backfill plus the
// consistency check re-cover a missed secondary write before the read switch.
type FanoutSessionService struct {
	Primary   session.Service // authoritative reads
	Secondary session.Service // best-effort shadow writes
}

var _ session.Service = (*FanoutSessionService)(nil)

func (f *FanoutSessionService) CreateSession(ctx context.Context, key session.Key, state session.StateMap, opts ...session.Option) (*session.Session, error) {
	_, err := f.Secondary.CreateSession(ctx, key, state, opts...)
	f.shadow("create_session", err)
	return f.Primary.CreateSession(ctx, key, state, opts...)
}

func (f *FanoutSessionService) GetSession(ctx context.Context, key session.Key, opts ...session.Option) (*session.Session, error) {
	return f.Primary.GetSession(ctx, key, opts...)
}

func (f *FanoutSessionService) ListSessions(ctx context.Context, userKey session.UserKey, opts ...session.Option) ([]*session.Session, error) {
	return f.Primary.ListSessions(ctx, userKey, opts...)
}

func (f *FanoutSessionService) DeleteSession(ctx context.Context, key session.Key, opts ...session.Option) error {
	f.shadow("delete_session", f.Secondary.DeleteSession(ctx, key, opts...))
	return f.Primary.DeleteSession(ctx, key, opts...)
}

func (f *FanoutSessionService) UpdateAppState(ctx context.Context, appName string, state session.StateMap) error {
	f.shadow("update_app_state", f.Secondary.UpdateAppState(ctx, appName, state))
	return f.Primary.UpdateAppState(ctx, appName, state)
}

func (f *FanoutSessionService) DeleteAppState(ctx context.Context, appName string, key string) error {
	f.shadow("delete_app_state", f.Secondary.DeleteAppState(ctx, appName, key))
	return f.Primary.DeleteAppState(ctx, appName, key)
}

func (f *FanoutSessionService) ListAppStates(ctx context.Context, appName string) (session.StateMap, error) {
	return f.Primary.ListAppStates(ctx, appName)
}

func (f *FanoutSessionService) UpdateUserState(ctx context.Context, userKey session.UserKey, state session.StateMap) error {
	f.shadow("update_user_state", f.Secondary.UpdateUserState(ctx, userKey, state))
	return f.Primary.UpdateUserState(ctx, userKey, state)
}

func (f *FanoutSessionService) ListUserStates(ctx context.Context, userKey session.UserKey) (session.StateMap, error) {
	return f.Primary.ListUserStates(ctx, userKey)
}

func (f *FanoutSessionService) DeleteUserState(ctx context.Context, userKey session.UserKey, key string) error {
	f.shadow("delete_user_state", f.Secondary.DeleteUserState(ctx, userKey, key))
	return f.Primary.DeleteUserState(ctx, userKey, key)
}

func (f *FanoutSessionService) UpdateSessionState(ctx context.Context, key session.Key, state session.StateMap) error {
	f.shadow("update_session_state", f.Secondary.UpdateSessionState(ctx, key, state))
	return f.Primary.UpdateSessionState(ctx, key, state)
}

func (f *FanoutSessionService) AppendEvent(ctx context.Context, sess *session.Session, e *event.Event, opts ...session.Option) error {
	// Shadow first: a slow secondary must not delay the authoritative write.
	// The secondary gets a Clone — AppendEvent mutates the carrier's event
	// list (UpdateUserSession), and sharing the pointer would append every
	// event twice into the caller's in-flight session.
	f.shadow("append_event", f.Secondary.AppendEvent(ctx, sess.Clone(), e, opts...))
	return f.Primary.AppendEvent(ctx, sess, e, opts...)
}

func (f *FanoutSessionService) CreateSessionSummary(ctx context.Context, sess *session.Session, filterKey string, force bool) error {
	f.shadow("create_summary", f.Secondary.CreateSessionSummary(ctx, sess, filterKey, force))
	return f.Primary.CreateSessionSummary(ctx, sess, filterKey, force)
}

func (f *FanoutSessionService) EnqueueSummaryJob(ctx context.Context, sess *session.Session, filterKey string, force bool) error {
	f.shadow("enqueue_summary", f.Secondary.EnqueueSummaryJob(ctx, sess, filterKey, force))
	return f.Primary.EnqueueSummaryJob(ctx, sess, filterKey, force)
}

func (f *FanoutSessionService) GetSessionSummaryText(ctx context.Context, sess *session.Session, opts ...session.SummaryOption) (string, bool) {
	return f.Primary.GetSessionSummaryText(ctx, sess, opts...)
}

// Close is a no-op: the wrapped services are shared and closed by their owner.
func (f *FanoutSessionService) Close() error { return nil }

// shadow logs a secondary write failure; the backfill and the pre-switch
// consistency check are the compensation path (design 5.2.6 双写半边失败).
func (f *FanoutSessionService) shadow(op string, err error) {
	if err != nil {
		plog.Warnf("migration shadow write %s failed (consistency check will catch it): %v", op, err)
	}
}
