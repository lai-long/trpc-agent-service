package storage

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

// appTenantResolver resolves a session/memory row's tenant_id from its
// agent_app id, cached per process (the app → tenant mapping changes only
// through the Admin API, and the resolver cache there is short-lived).
type appTenantResolver struct {
	pool *pgxpool.Pool

	mu    sync.Mutex
	cache map[string]string
}

func newAppTenantResolver(pool *pgxpool.Pool) *appTenantResolver {
	return &appTenantResolver{pool: pool, cache: make(map[string]string)}
}

func (r *appTenantResolver) resolve(ctx context.Context, appID string) (string, error) {
	r.mu.Lock()
	cached, ok := r.cache[appID]
	r.mu.Unlock()
	if ok {
		return cached, nil
	}
	var tenantID string
	if err := r.pool.QueryRow(ctx,
		`SELECT tenant_id FROM agent_app WHERE id = $1`, appID).Scan(&tenantID); err != nil {
		return "", fmt.Errorf("resolve tenant for app %s: %w", appID, err)
	}
	r.mu.Lock()
	r.cache[appID] = tenantID
	r.mu.Unlock()
	return tenantID, nil
}
