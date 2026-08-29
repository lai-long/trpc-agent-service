package tenant

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
)

// InvalidationChannel is the Redis pub/sub channel carrying config
// invalidation notifications (design 5.2.3): the Admin API publishes after
// tenant/app/binding writes and publish/rollback; Resolvers drop their cache
// on receipt so changes take effect in seconds. The cache TTL remains the
// fallback when a notification is lost.
const InvalidationChannel = "tenant:invalidate"

// PublishInvalidation notifies all Resolver instances to drop their cache.
func PublishInvalidation(ctx context.Context, rdb *redis.Client) error {
	if err := rdb.Publish(ctx, InvalidationChannel, "1").Err(); err != nil {
		return fmt.Errorf("publish invalidation: %w", err)
	}
	return nil
}

// DefaultCacheTTL bounds how long a snapshot is served before reloading.
// The publish/rollback pub/sub invalidation arrives with the Admin API
// (design 5.2.3); until then this TTL is the only refresh mechanism.
const DefaultCacheTTL = 30 * time.Second

var (
	// ErrUnknownBinding means no channel_binding row serves the webhook path.
	ErrUnknownBinding = errors.New("unknown channel binding")
	// ErrInactive means the binding, its tenant or its app is disabled.
	ErrInactive = errors.New("tenant route inactive")
)

// Route is the resolution of one webhook path: which tenant and agent app a
// callback belongs to.
type Route struct {
	Tenant  Tenant
	App     AgentApp
	Binding ChannelBinding
}

// Resolver answers webhook_path → Route lookups from a cached snapshot of
// the tenant tables, refreshed every TTL. A failed reload keeps serving the
// previous snapshot (stale beats down); only a failed first load errors.
type Resolver struct {
	store Store
	ttl   time.Duration

	mu       sync.RWMutex
	tenants  map[string]Tenant
	apps     map[string]AgentApp
	bindings map[string]ChannelBinding // by webhook_path
	loadedAt time.Time
	loaded   bool
}

// NewResolver creates a Resolver with the default cache TTL.
func NewResolver(store Store) *Resolver {
	return NewResolverWithTTL(store, DefaultCacheTTL)
}

// NewResolverWithTTL creates a Resolver with an explicit cache TTL.
func NewResolverWithTTL(store Store, ttl time.Duration) *Resolver {
	return &Resolver{store: store, ttl: ttl}
}

// Resolve maps a webhook path to its tenant route, refreshing the cache when
// stale. Unknown or inactive routes are rejected: the callback gets an error
// reply and the IM may redeliver, but the message never consumes queue space.
func (r *Resolver) Resolve(ctx context.Context, webhookPath string) (Route, error) {
	if err := r.refresh(ctx); err != nil {
		return Route{}, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	b, ok := r.bindings[webhookPath]
	if !ok {
		return Route{}, fmt.Errorf("%w: %s", ErrUnknownBinding, webhookPath)
	}
	if b.Status != StatusActive {
		return Route{}, fmt.Errorf("%w: binding %s status %q", ErrInactive, b.ID, b.Status)
	}
	t, ok := r.tenants[b.TenantID]
	if !ok {
		return Route{}, fmt.Errorf("%w: binding %s references missing tenant %s",
			ErrUnknownBinding, b.ID, b.TenantID)
	}
	if t.Status != StatusActive {
		return Route{}, fmt.Errorf("%w: tenant %s status %q", ErrInactive, t.ID, t.Status)
	}
	app, ok := r.apps[b.AppID]
	if !ok {
		return Route{}, fmt.Errorf("%w: binding %s references missing app %s",
			ErrUnknownBinding, b.ID, b.AppID)
	}
	if app.Status == StatusDisabled {
		return Route{}, fmt.Errorf("%w: app %s disabled", ErrInactive, app.ID)
	}
	return Route{Tenant: t, App: app, Binding: b}, nil
}

// Invalidate drops the cached snapshot; the next Resolve reloads.
func (r *Resolver) Invalidate() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.loaded = false
}

// WatchInvalidations subscribes to the invalidation channel until ctx is
// canceled; each notification drops the cache so a publish/rollback takes
// effect within seconds instead of at TTL expiry.
func (r *Resolver) WatchInvalidations(ctx context.Context, rdb *redis.Client) {
	go func() {
		sub := rdb.Subscribe(ctx, InvalidationChannel)
		defer func() { _ = sub.Close() }()
		ch := sub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-ch:
				if !ok {
					return
				}
				r.Invalidate()
			}
		}
	}()
}

// refresh reloads the snapshot when the cache is stale. Concurrent refreshes
// are serialized by the write lock; the double-check after acquiring it
// keeps a burst of callbacks from reloading more than once.
func (r *Resolver) refresh(ctx context.Context) error {
	r.mu.RLock()
	fresh := r.loaded && time.Since(r.loadedAt) < r.ttl
	r.mu.RUnlock()
	if fresh {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.loaded && time.Since(r.loadedAt) < r.ttl {
		return nil
	}
	d, err := r.store.LoadAll(ctx)
	if err != nil {
		if r.loaded {
			plog.Warnf("tenant reload failed, serving snapshot from %s: %v",
				r.loadedAt.Format(time.RFC3339), err)
			return nil
		}
		return fmt.Errorf("load tenants: %w", err)
	}

	tenants := make(map[string]Tenant, len(d.Tenants))
	for _, t := range d.Tenants {
		tenants[t.ID] = t
	}
	apps := make(map[string]AgentApp, len(d.Apps))
	for _, a := range d.Apps {
		apps[a.ID] = a
	}
	bindings := make(map[string]ChannelBinding, len(d.Bindings))
	for _, b := range d.Bindings {
		bindings[b.WebhookPath] = b
	}
	r.tenants, r.apps, r.bindings = tenants, apps, bindings
	r.loadedAt = time.Now()
	r.loaded = true
	return nil
}
