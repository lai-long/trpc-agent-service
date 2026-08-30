package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	memtool "trpc.group/trpc-go/trpc-agent-go/memory/tool"
	"trpc.group/trpc-go/trpc-agent-go/session"
	ttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

// PGMemoryService implements the framework memory.Service over the
// memory_item table (design 5.1.3):
//
//   - two levels: app_id set = app-private, NULL = tenant-shared; reads merge
//     both levels, writes default to app-private;
//   - soft delete (deleted_at) backs the "user asks to be forgotten"
//     compliance scenario;
//   - the PG row is the single source of truth: a committed write is visible
//     to every worker (no local cache);
//   - search is keyword ILIKE for now; semantic recall plugs into the
//     embedding_id column when the embedder stage lands (vectors live in the
//     knowledge store, memory content stays here).
type PGMemoryService struct {
	pool    *pgxpool.Pool
	tenants *appTenantResolver
}

// NewPGMemoryService creates the service on an established pool.
func NewPGMemoryService(pool *pgxpool.Pool) *PGMemoryService {
	return &PGMemoryService{pool: pool, tenants: newAppTenantResolver(pool)}
}

// ReadMemories implements memory.Reader: app-private + tenant-shared, active
// rows only, most recently updated first.
func (s *PGMemoryService) ReadMemories(ctx context.Context, userKey memory.UserKey, limit int) ([]*memory.Entry, error) {
	if limit <= 0 {
		limit = 10
	}
	tenantID, err := s.tenants.resolve(ctx, userKey.AppName)
	if err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx,
		`SELECT id, content, topics, created_at, updated_at FROM memory_item
		 WHERE tenant_id = $1 AND user_id = $2 AND (app_id = $3 OR app_id IS NULL)
		   AND deleted_at IS NULL
		 ORDER BY updated_at DESC LIMIT $4`,
		tenantID, userKey.UserID, userKey.AppName, limit)
	if err != nil {
		return nil, fmt.Errorf("read memories: %w", err)
	}
	defer rows.Close()
	return scanEntries(rows, userKey)
}

// SearchMemories implements memory.Reader: keyword match (ILIKE) over the
// merged scope. An empty query degrades to ReadMemories.
func (s *PGMemoryService) SearchMemories(ctx context.Context, userKey memory.UserKey, query string, _ ...memory.SearchOption) ([]*memory.Entry, error) {
	if query == "" {
		return s.ReadMemories(ctx, userKey, 10)
	}
	tenantID, err := s.tenants.resolve(ctx, userKey.AppName)
	if err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx,
		`SELECT id, content, topics, created_at, updated_at FROM memory_item
		 WHERE tenant_id = $1 AND user_id = $2 AND (app_id = $3 OR app_id IS NULL)
		   AND deleted_at IS NULL AND content ILIKE '%' || $4 || '%'
		 ORDER BY updated_at DESC LIMIT 10`,
		tenantID, userKey.UserID, userKey.AppName, query)
	if err != nil {
		return nil, fmt.Errorf("search memories: %w", err)
	}
	defer rows.Close()
	return scanEntries(rows, userKey)
}

// AddMemory implements memory.Service: idempotent by (scope, content) — an
// active duplicate just touches updated_at, a matching tombstone is
// reactivated, otherwise a new app-private row is inserted.
func (s *PGMemoryService) AddMemory(ctx context.Context, userKey memory.UserKey, mem string, topics []string, _ ...memory.AddOption) error {
	if mem == "" {
		return errors.New("pg memory: empty content")
	}
	tenantID, err := s.tenants.resolve(ctx, userKey.AppName)
	if err != nil {
		return err
	}
	topicsJSON, err := json.Marshal(topics)
	if err != nil {
		return err
	}

	tag, err := s.pool.Exec(ctx,
		`UPDATE memory_item SET updated_at = now()
		 WHERE tenant_id=$1 AND app_id=$2 AND user_id=$3 AND content=$4 AND deleted_at IS NULL`,
		tenantID, userKey.AppName, userKey.UserID, mem)
	if err != nil {
		return fmt.Errorf("add memory (touch): %w", err)
	}
	if tag.RowsAffected() > 0 {
		return nil // active duplicate: idempotent no-op
	}
	tag, err = s.pool.Exec(ctx,
		`UPDATE memory_item SET deleted_at = NULL, topics = $5, updated_at = now()
		 WHERE tenant_id=$1 AND app_id=$2 AND user_id=$3 AND content=$4 AND deleted_at IS NOT NULL`,
		tenantID, userKey.AppName, userKey.UserID, mem, topicsJSON)
	if err != nil {
		return fmt.Errorf("add memory (revive): %w", err)
	}
	if tag.RowsAffected() > 0 {
		return nil // tombstone reactivated
	}
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO memory_item (tenant_id, app_id, user_id, content, topics)
		 VALUES ($1, $2, $3, $4, $5)`,
		tenantID, userKey.AppName, userKey.UserID, mem, topicsJSON); err != nil {
		return fmt.Errorf("add memory (insert): %w", err)
	}
	return nil
}

// UpdateMemory implements memory.Service: updates an active row; a matching
// tombstone is reactivated with the new content; a missing row is an error
// (the memory_update tool always targets IDs returned by search/load).
func (s *PGMemoryService) UpdateMemory(ctx context.Context, memoryKey memory.Key, mem string, topics []string, _ ...memory.UpdateOption) error {
	if err := memoryKey.CheckMemoryKey(); err != nil {
		return err
	}
	topicsJSON, err := json.Marshal(topics)
	if err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx,
		`UPDATE memory_item SET content=$3, topics=$4, updated_at=now()
		 WHERE id=$1 AND user_id=$2 AND deleted_at IS NULL`,
		memoryKey.MemoryID, memoryKey.UserID, mem, topicsJSON)
	if err != nil {
		return fmt.Errorf("update memory: %w", err)
	}
	if tag.RowsAffected() > 0 {
		return nil
	}
	tag, err = s.pool.Exec(ctx,
		`UPDATE memory_item SET content=$3, topics=$4, deleted_at=NULL, updated_at=now()
		 WHERE id=$1 AND user_id=$2 AND deleted_at IS NOT NULL`,
		memoryKey.MemoryID, memoryKey.UserID, mem, topicsJSON)
	if err != nil {
		return fmt.Errorf("update memory (revive): %w", err)
	}
	if tag.RowsAffected() > 0 {
		return nil
	}
	return fmt.Errorf("memory %s not found", memoryKey.MemoryID)
}

// DeleteMemory implements memory.Service: soft delete (compliance scenario).
func (s *PGMemoryService) DeleteMemory(ctx context.Context, memoryKey memory.Key) error {
	if err := memoryKey.CheckMemoryKey(); err != nil {
		return err
	}
	if _, err := s.pool.Exec(ctx,
		`UPDATE memory_item SET deleted_at = now() WHERE id=$1 AND user_id=$2 AND deleted_at IS NULL`,
		memoryKey.MemoryID, memoryKey.UserID); err != nil {
		return fmt.Errorf("delete memory: %w", err)
	}
	return nil
}

// ClearMemories implements memory.Service: soft-deletes all active memories
// of the user within the app scope (tenant-shared rows are untouched — they
// belong to the tenant, not the user's app session).
func (s *PGMemoryService) ClearMemories(ctx context.Context, userKey memory.UserKey) error {
	if _, err := s.pool.Exec(ctx,
		`UPDATE memory_item SET deleted_at = now()
		 WHERE app_id=$1 AND user_id=$2 AND deleted_at IS NULL`,
		userKey.AppName, userKey.UserID); err != nil {
		return fmt.Errorf("clear memories: %w", err)
	}
	return nil
}

// Tools implements memory.Service: the standard memory tool set. The tools
// resolve the service and app/user identity from the invocation context at
// call time (the runner injects them), so they are service-agnostic.
func (s *PGMemoryService) Tools() []ttool.Tool {
	tools := []ttool.Tool{
		memtool.NewAddTool(),
		memtool.NewSearchTool(),
		memtool.NewLoadTool(),
		memtool.NewUpdateTool(),
		memtool.NewDeleteTool(),
		memtool.NewClearTool(),
	}
	return tools
}

// EnqueueAutoMemoryJob implements memory.Service as a no-op: automatic memory
// extraction needs a model-backed extractor, which arrives with the
// summarizer stage. The runner tolerates the no-op.
func (s *PGMemoryService) EnqueueAutoMemoryJob(_ context.Context, _ *session.Session) error {
	return nil
}

// Close implements memory.Service; the pool is owned by the caller.
func (s *PGMemoryService) Close() error { return nil }

func scanEntries(rows pgx.Rows, userKey memory.UserKey) ([]*memory.Entry, error) {
	var out []*memory.Entry
	for rows.Next() {
		var (
			id, content          string
			topicsRaw            []byte
			createdAt, updatedAt time.Time
		)
		if err := rows.Scan(&id, &content, &topicsRaw, &createdAt, &updatedAt); err != nil {
			return nil, fmt.Errorf("scan memory: %w", err)
		}
		var topics []string
		if len(topicsRaw) > 0 {
			if err := json.Unmarshal(topicsRaw, &topics); err != nil {
				return nil, fmt.Errorf("decode topics: %w", err)
			}
		}
		out = append(out, &memory.Entry{
			ID: id, AppName: userKey.AppName, UserID: userKey.UserID,
			Memory:    &memory.Memory{Memory: content, Topics: topics},
			CreatedAt: createdAt, UpdatedAt: updatedAt,
		})
	}
	return out, rows.Err()
}
