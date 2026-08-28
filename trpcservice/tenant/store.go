package tenant

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Store loads the full tenant data set. The data volume is small (design
// target ~50 tenants), so the Resolver caches a full snapshot instead of
// querying per request.
type Store interface {
	LoadAll(ctx context.Context) (Data, error)
}

// Data is one load of the three routing tables.
type Data struct {
	Tenants  []Tenant
	Apps     []AgentApp
	Bindings []ChannelBinding
}

// PGStore implements Store on a PostgreSQL pool.
type PGStore struct {
	pool *pgxpool.Pool
}

// NewPGStore creates a PGStore on an established pool.
func NewPGStore(pool *pgxpool.Pool) *PGStore {
	return &PGStore{pool: pool}
}

// LoadAll implements Store.
func (s *PGStore) LoadAll(ctx context.Context) (Data, error) {
	var d Data
	if err := s.loadTenants(ctx, &d); err != nil {
		return Data{}, err
	}
	if err := s.loadApps(ctx, &d); err != nil {
		return Data{}, err
	}
	if err := s.loadBindings(ctx, &d); err != nil {
		return Data{}, err
	}
	return d, nil
}

func (s *PGStore) loadTenants(ctx context.Context, d *Data) error {
	rows, err := s.pool.Query(ctx,
		`SELECT id, name, model_config, tool_policy, audit_policy, storage_config, status FROM tenant`)
	if err != nil {
		return fmt.Errorf("query tenant: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var t Tenant
		if err := rows.Scan(&t.ID, &t.Name, &t.ModelConfig, &t.ToolPolicy,
			&t.AuditPolicy, &t.StorageConfig, &t.Status); err != nil {
			return fmt.Errorf("scan tenant: %w", err)
		}
		d.Tenants = append(d.Tenants, t)
	}
	return rows.Err()
}

func (s *PGStore) loadApps(ctx context.Context, d *Data) error {
	rows, err := s.pool.Query(ctx,
		`SELECT id, tenant_id, name, agent_type, config, version, status FROM agent_app`)
	if err != nil {
		return fmt.Errorf("query agent_app: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var a AgentApp
		if err := rows.Scan(&a.ID, &a.TenantID, &a.Name, &a.AgentType,
			&a.Config, &a.Version, &a.Status); err != nil {
			return fmt.Errorf("scan agent_app: %w", err)
		}
		d.Apps = append(d.Apps, a)
	}
	return rows.Err()
}

func (s *PGStore) loadBindings(ctx context.Context, d *Data) error {
	rows, err := s.pool.Query(ctx,
		`SELECT id, tenant_id, channel, app_id, webhook_path, token_ref, aeskey_ref, config, status
		 FROM channel_binding`)
	if err != nil {
		return fmt.Errorf("query channel_binding: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var b ChannelBinding
		// token_ref / aeskey_ref are nullable (pgx cannot scan NULL into *string).
		var tokenRef, aeskeyRef sql.NullString
		if err := rows.Scan(&b.ID, &b.TenantID, &b.Channel, &b.AppID, &b.WebhookPath,
			&tokenRef, &aeskeyRef, &b.Config, &b.Status); err != nil {
			return fmt.Errorf("scan channel_binding: %w", err)
		}
		b.TokenRef, b.AESKeyRef = tokenRef.String, aeskeyRef.String
		d.Bindings = append(d.Bindings, b)
	}
	return rows.Err()
}
