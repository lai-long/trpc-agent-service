package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// AdminAPI serves the management endpoints: tenant/app CRUD,
// publish/rollback with atomic switching, channel binding management,
// storage-migration control and audit queries. It is intended for
// internal networks only; authentication is a bearer token
// (TRPC_ADMIN_TOKEN) — empty means dev mode (no auth).
type AdminAPI struct {
	pool    *pgxpool.Pool
	auditor *storage.Auditor // nil disables write-op auditing
	rdb     *redis.Client    // nil disables invalidation broadcasts
	token   string

	// Knowledge ingests documents into the shared knowledge store (nil when
	// no embedder is configured; the ingestion endpoint then answers 503).
	Knowledge *knowledge.BuiltinKnowledge
	// DefaultSessionBackend is the platform default session backend; a
	// migration's from_backend is the tenant's override or this default.
	DefaultSessionBackend string
	// ModelHosts is the platform-level allowlist of model endpoint hosts
	// (config.Config.ModelHostAllowlist). Tenant and app configs may only
	// point their runner at these hosts: conversation content flows to the
	// model endpoint, so the choice is platform policy. Nil
	// falls back to agent.DefaultModelHosts.
	ModelHosts []string
}

// NewAdminAPI creates the API. The bearer token empty means dev mode.
func NewAdminAPI(pool *pgxpool.Pool, auditor *storage.Auditor, rdb *redis.Client, token string) *AdminAPI {
	return &AdminAPI{pool: pool, auditor: auditor, rdb: rdb, token: token}
}

// RegisterRoutes mounts the admin routes.
func (a *AdminAPI) RegisterRoutes(mux *http.ServeMux) {
	handle := func(pattern string, h http.HandlerFunc) {
		mux.Handle(pattern, a.auth(h))
	}
	handle("POST /admin/tenants", a.createTenant)
	handle("GET /admin/tenants", a.listTenants)
	handle("GET /admin/tenants/{id}", a.getTenant)
	handle("PATCH /admin/tenants/{id}", a.updateTenant)
	handle("POST /admin/tenants/{id}/apps", a.createApp)
	handle("GET /admin/tenants/{id}/apps", a.listApps)
	handle("PATCH /admin/tenants/{id}/apps/{app}", a.updateApp)
	handle("POST /admin/apps/{id}/publish", a.publishApp)
	handle("POST /admin/apps/{id}/rollback", a.rollbackApp)
	handle("POST /admin/apps/{id}/bindings", a.createBinding)
	handle("GET /admin/apps/{id}/bindings", a.listBindings)
	handle("DELETE /admin/apps/{id}/bindings/{binding}", a.deleteBinding)
	handle("POST /admin/apps/{id}/knowledge/documents", a.addKnowledgeDocument)
	handle("POST /admin/tenants/{id}/storage-migrations", a.createMigration)
	handle("GET /admin/storage-migrations/{id}", a.getMigration)
	handle("GET /admin/audit", a.queryAudit)
}

// auth enforces the bearer token unless running in dev mode (token unset).
func (a *AdminAPI) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if a.token != "" && r.Header.Get("Authorization") != "Bearer "+a.token {
			writeError(w, http.StatusUnauthorized, "missing or invalid admin token")
			return
		}
		next(w, r)
	}
}

// validateModelConfig enforces the platform model-endpoint allowlist on
// tenant.model_config. Every write path that can set base_url goes through
// this helper or validateAppConfig, so none of them becomes a bypass.
func (a *AdminAPI) validateModelConfig(w http.ResponseWriter, raw json.RawMessage) bool {
	if err := agent.ValidateModelConfig(raw, a.ModelHosts); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return false
	}
	return true
}

// validateAppConfig is validateModelConfig for agent_app.config, where the
// model spec nests under "model".
func (a *AdminAPI) validateAppConfig(w http.ResponseWriter, raw json.RawMessage) bool {
	if err := agent.ValidateAppConfig(raw, a.ModelHosts); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return false
	}
	return true
}

// ---------------------------------------------------------------------------
// Tenants
// ---------------------------------------------------------------------------

func (a *AdminAPI) createTenant(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Name            string          `json:"name"`
		ModelConfig     json.RawMessage `json:"model_config"`
		ToolPolicy      json.RawMessage `json:"tool_policy"`
		AuditPolicy     json.RawMessage `json:"audit_policy"`
		GuardrailPolicy json.RawMessage `json:"guardrail_policy"`
		RatePolicy      json.RawMessage `json:"rate_policy"`
		StorageConfig   json.RawMessage `json:"storage_config"`
	}
	if !decodeBody(w, r, &in) {
		return
	}
	if in.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	if !a.validateModelConfig(w, in.ModelConfig) {
		return
	}
	var id string
	err := a.pool.QueryRow(r.Context(),
		`INSERT INTO tenant (name, model_config, tool_policy, audit_policy, guardrail_policy, rate_policy, storage_config)
		 VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id`,
		in.Name, rawOrNil(in.ModelConfig), rawOrNil(in.ToolPolicy),
		rawOrNil(in.AuditPolicy), rawOrNil(in.GuardrailPolicy), rawOrNil(in.RatePolicy), rawOrNil(in.StorageConfig)).Scan(&id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	a.afterWrite(r, "create_tenant", id, nil, in)
	writeJSON(w, http.StatusCreated, map[string]any{"id": id})
}

func (a *AdminAPI) listTenants(w http.ResponseWriter, r *http.Request) {
	rows, err := a.pool.Query(r.Context(),
		`SELECT id, name, status, created_at, updated_at FROM tenant ORDER BY created_at DESC`)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id, name, status string
		var createdAt, updatedAt time.Time
		if err := rows.Scan(&id, &name, &status, &createdAt, &updatedAt); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		out = append(out, map[string]any{
			"id": id, "name": name, "status": status,
			"created_at": createdAt, "updated_at": updatedAt,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (a *AdminAPI) getTenant(w http.ResponseWriter, r *http.Request) {
	var (
		name, status                                               string
		modelCfg, toolPol, auditPol, guardPol, ratePol, storageCfg []byte
		createdAt, updatedAt                                       time.Time
	)
	err := a.pool.QueryRow(r.Context(),
		`SELECT name, status, model_config, tool_policy, audit_policy, guardrail_policy, rate_policy, storage_config, created_at, updated_at
		 FROM tenant WHERE id = $1`, r.PathValue("id"),
	).Scan(&name, &status, &modelCfg, &toolPol, &auditPol, &guardPol, &ratePol, &storageCfg, &createdAt, &updatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "tenant not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id": r.PathValue("id"), "name": name, "status": status,
		"model_config": jsonOrNull(modelCfg), "tool_policy": jsonOrNull(toolPol),
		"audit_policy": jsonOrNull(auditPol), "guardrail_policy": jsonOrNull(guardPol), "rate_policy": jsonOrNull(ratePol),
		"storage_config": jsonOrNull(storageCfg),
		"created_at":     createdAt, "updated_at": updatedAt,
	})
}

func (a *AdminAPI) updateTenant(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Name            *string         `json:"name"`
		Status          *string         `json:"status"`
		ModelConfig     json.RawMessage `json:"model_config"`
		ToolPolicy      json.RawMessage `json:"tool_policy"`
		AuditPolicy     json.RawMessage `json:"audit_policy"`
		GuardrailPolicy json.RawMessage `json:"guardrail_policy"`
		RatePolicy      json.RawMessage `json:"rate_policy"`
		StorageConfig   json.RawMessage `json:"storage_config"`
	}
	if !decodeBody(w, r, &in) {
		return
	}
	sets, args := []string{}, []any{r.PathValue("id")}
	add := func(clause string, v any) {
		args = append(args, v)
		sets = append(sets, fmt.Sprintf(clause, len(args)))
	}
	if in.Name != nil {
		add("name = $%d", *in.Name)
	}
	if in.Status != nil {
		if *in.Status != "active" && *in.Status != "disabled" {
			writeError(w, http.StatusBadRequest, "status must be active or disabled")
			return
		}
		add("status = $%d", *in.Status)
	}
	if in.ModelConfig != nil {
		if !a.validateModelConfig(w, in.ModelConfig) {
			return
		}
		add("model_config = $%d", []byte(in.ModelConfig))
	}
	if in.ToolPolicy != nil {
		add("tool_policy = $%d", []byte(in.ToolPolicy))
	}
	if in.AuditPolicy != nil {
		add("audit_policy = $%d", []byte(in.AuditPolicy))
	}
	if in.GuardrailPolicy != nil {
		add("guardrail_policy = $%d", []byte(in.GuardrailPolicy))
	}
	if in.RatePolicy != nil {
		add("rate_policy = $%d", []byte(in.RatePolicy))
	}
	if in.StorageConfig != nil {
		// storage_config changes must go through the migration flow;
		// live switching is rejected here.
		writeError(w, http.StatusConflict,
			"storage_config cannot be changed directly; use the migration flow")
		return
	}
	if len(sets) == 0 {
		writeError(w, http.StatusBadRequest, "nothing to update")
		return
	}
	before := a.rowJSON(r.Context(), "tenant", "id", r.PathValue("id"))
	tag, err := a.pool.Exec(r.Context(),
		fmt.Sprintf(`UPDATE tenant SET %s, updated_at = now() WHERE id = $1`, strings.Join(sets, ", ")),
		args...)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if tag.RowsAffected() == 0 {
		writeError(w, http.StatusNotFound, "tenant not found")
		return
	}
	a.afterWrite(r, "update_tenant", r.PathValue("id"), before, in)
	writeJSON(w, http.StatusOK, map[string]any{"updated": true})
}

// ---------------------------------------------------------------------------
// Apps
// ---------------------------------------------------------------------------

func (a *AdminAPI) createApp(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Name      string          `json:"name"`
		AgentType string          `json:"agent_type"`
		Config    json.RawMessage `json:"config"`
	}
	if !decodeBody(w, r, &in) {
		return
	}
	if in.Name == "" || in.AgentType == "" || len(in.Config) == 0 {
		writeError(w, http.StatusBadRequest, "name, agent_type and config are required")
		return
	}
	if !a.validateAppConfig(w, in.Config) {
		return
	}
	tenantID := r.PathValue("id")
	var id string
	var version int
	err := a.pool.QueryRow(r.Context(),
		`INSERT INTO agent_app (tenant_id, name, agent_type, config, version, status)
		 SELECT $1::uuid, $2::varchar, $3::varchar, $4::jsonb, COALESCE(MAX(version), 0) + 1, 'draft'
		 FROM agent_app WHERE tenant_id = $1::uuid AND name = $2::varchar
		 RETURNING id, version`,
		tenantID, in.Name, in.AgentType, []byte(in.Config)).Scan(&id, &version)
	if err != nil {
		// Concurrent creates race on (tenant_id, name, version): ask the
		// caller to retry instead of a 500.
		if strings.Contains(err.Error(), "uk_agent_app_version") {
			writeError(w, http.StatusConflict, "concurrent create raced the version; retry")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	a.afterWrite(r, "create_app", tenantID, nil, map[string]any{
		"id": id, "version": version, "name": in.Name, "agent_type": in.AgentType, "config": in.Config,
	})
	writeJSON(w, http.StatusCreated, map[string]any{"id": id, "version": version, "status": "draft"})
}

func (a *AdminAPI) listApps(w http.ResponseWriter, r *http.Request) {
	rows, err := a.pool.Query(r.Context(),
		`SELECT id, name, agent_type, version, status, updated_at FROM agent_app
		 WHERE tenant_id = $1 ORDER BY name, version DESC`, r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id, name, agentType, status string
		var version int
		var updatedAt time.Time
		if err := rows.Scan(&id, &name, &agentType, &version, &status, &updatedAt); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		out = append(out, map[string]any{
			"id": id, "name": name, "agent_type": agentType,
			"version": version, "status": status, "updated_at": updatedAt,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

// updateApp edits a draft's config; published versions are immutable
// snapshots so rollback stays a pure status switch.
func (a *AdminAPI) updateApp(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Config json.RawMessage `json:"config"`
	}
	if !decodeBody(w, r, &in) {
		return
	}
	if len(in.Config) == 0 {
		writeError(w, http.StatusBadRequest, "config is required")
		return
	}
	if !a.validateAppConfig(w, in.Config) {
		return
	}
	before := a.rowJSON(r.Context(), "agent_app", "id", r.PathValue("app"))
	tag, err := a.pool.Exec(r.Context(),
		`UPDATE agent_app SET config = $3, updated_at = now()
		 WHERE id = $1 AND tenant_id = $2 AND status = 'draft'`,
		r.PathValue("app"), r.PathValue("id"), []byte(in.Config))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if tag.RowsAffected() == 0 {
		writeError(w, http.StatusConflict, "app not found or not in draft status")
		return
	}
	a.afterWrite(r, "update_app", r.PathValue("id"), before, in)
	writeJSON(w, http.StatusOK, map[string]any{"updated": true})
}

// publishApp atomically switches the (tenant, name) published version: the
// previously published version leaves published status and the target takes
// it, guarded by the partial unique index.
func (a *AdminAPI) publishApp(w http.ResponseWriter, r *http.Request) {
	before := a.rowJSON(r.Context(), "agent_app", "id", r.PathValue("id"))
	tenantID, err := a.publish(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, errStatus(err), err.Error())
		return
	}
	a.afterWrite(r, "publish_app", tenantID, before, map[string]any{"published": r.PathValue("id")})
	writeJSON(w, http.StatusOK, map[string]any{"published": r.PathValue("id")})
}

// rollbackApp re-publishes a historical version: the requested one, or the
// highest version below the currently published one.
func (a *AdminAPI) rollbackApp(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Version int `json:"version"`
	}
	// An empty body is allowed: roll back to the previous version.
	_ = json.NewDecoder(r.Body).Decode(&in)

	ctx := r.Context()
	appID := r.PathValue("id")
	var tenantID, name string
	var version int
	err := a.pool.QueryRow(ctx,
		`SELECT tenant_id, name, version FROM agent_app WHERE id = $1`, appID).
		Scan(&tenantID, &name, &version)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "app not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	target := in.Version
	if target == 0 {
		err = a.pool.QueryRow(ctx,
			`SELECT COALESCE(MAX(version), 0) FROM agent_app
			 WHERE tenant_id = $1 AND name = $2 AND version < $3`,
			tenantID, name, version).Scan(&target)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}
	if target == 0 || target == version {
		writeError(w, http.StatusConflict, "no earlier version to roll back to")
		return
	}
	var targetID string
	err = a.pool.QueryRow(ctx,
		`SELECT id FROM agent_app WHERE tenant_id = $1 AND name = $2 AND version = $3`,
		tenantID, name, target).Scan(&targetID)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, fmt.Sprintf("version %d not found", target))
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if _, err := a.publish(ctx, targetID); err != nil {
		writeError(w, errStatus(err), err.Error())
		return
	}
	a.afterWrite(r, "rollback_app", tenantID,
		map[string]any{"id": appID, "version": version},
		map[string]any{"published": targetID, "version": target})
	writeJSON(w, http.StatusOK, map[string]any{"published": targetID, "version": target})
}

// publish switches the published version of the app family in one tx and
// returns the owning tenant ID (for the audit record). Bindings follow the
// published version: channel_binding.app_id references a concrete version
// row, so the repoint must ride the same transaction.
func (a *AdminAPI) publish(ctx context.Context, appID string) (string, error) {
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var tenantID, name, status string
	var config []byte
	err = tx.QueryRow(ctx,
		`SELECT tenant_id, name, status, config FROM agent_app WHERE id = $1 FOR UPDATE`, appID).
		Scan(&tenantID, &name, &status, &config)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", errNotFound("app not found")
	}
	if err != nil {
		return "", err
	}
	if status == "published" {
		return "", errConflict("app version is already published")
	}
	// The allowlist applies at publish time too: a draft stored
	// before a policy change must not become the serving version, and a
	// rollback to such a version is blocked by the same gate.
	if err := agent.ValidateAppConfig(config, a.ModelHosts); err != nil {
		return "", errBadRequest(err.Error())
	}
	if _, err := tx.Exec(ctx,
		`UPDATE agent_app SET status = 'disabled', updated_at = now()
		 WHERE tenant_id = $1 AND name = $2 AND status = 'published'`,
		tenantID, name); err != nil {
		return "", fmt.Errorf("unpublish current: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE agent_app SET status = 'published', updated_at = now() WHERE id = $1`,
		appID); err != nil {
		return "", fmt.Errorf("publish: %w", err)
	}
	// Bindings reference a concrete version row; repoint them at the newly
	// published version in the same tx, or callbacks keep routing to the
	// version that just left "published" and got disabled.
	if _, err := tx.Exec(ctx,
		`UPDATE channel_binding SET app_id = $1, updated_at = now()
		 WHERE app_id IN (SELECT id FROM agent_app WHERE tenant_id = $2 AND name = $3)`,
		appID, tenantID, name); err != nil {
		return "", fmt.Errorf("repoint bindings: %w", err)
	}
	return tenantID, tx.Commit(ctx)
}

// ---------------------------------------------------------------------------
// Bindings
// ---------------------------------------------------------------------------

func (a *AdminAPI) createBinding(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Channel     string          `json:"channel"`
		WebhookPath string          `json:"webhook_path"`
		TokenRef    string          `json:"token_ref"` // secret reference, never plaintext
		AESKeyRef   string          `json:"aeskey_ref"`
		Config      json.RawMessage `json:"config"`
	}
	if !decodeBody(w, r, &in) {
		return
	}
	if in.Channel == "" {
		writeError(w, http.StatusBadRequest, "channel is required")
		return
	}
	var id, tenantID, webhookPath string
	// Only a published app is bindable: binding a draft would route traffic
	// around the gray-release flow.
	//
	// An empty webhook_path is filled with the canonical binding-scoped
	// callback path /callback/{channel}/{binding_id} (design 5.3.1): the id
	// is generated in the same statement that writes the row, so the path
	// the dispatcher resolves is self-consistent — the caller cannot know a
	// DB-generated id up front, which made the binding-scoped path
	// unreachable through this API.
	err := a.pool.QueryRow(r.Context(),
		`WITH new_id AS (SELECT gen_random_uuid()::text AS id)
		 INSERT INTO channel_binding (id, tenant_id, channel, app_id, webhook_path, token_ref, aeskey_ref, config)
		 SELECT n.id::uuid, a.tenant_id, $2::varchar, a.id,
		        COALESCE(NULLIF($3::varchar, ''), '/callback/' || $2::varchar || '/' || n.id),
		        $4, $5, $6
		 FROM agent_app a CROSS JOIN new_id n
		 WHERE a.id = $1::uuid AND a.status = 'published'
		 RETURNING id::text, tenant_id::text, webhook_path`,
		r.PathValue("id"), in.Channel, in.WebhookPath,
		nullStr(in.TokenRef), nullStr(in.AESKeyRef), rawOrNil(in.Config),
	).Scan(&id, &tenantID, &webhookPath)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "app not found or not published")
		return
	}
	if err != nil {
		if strings.Contains(err.Error(), "uk_channel_webhook") {
			writeError(w, http.StatusConflict, "webhook_path already bound")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	a.afterWrite(r, "create_binding", tenantID, nil, map[string]any{
		"id": id, "channel": in.Channel, "webhook_path": webhookPath,
		"token_ref": in.TokenRef, "aeskey_ref": in.AESKeyRef, "config": in.Config,
	})
	writeJSON(w, http.StatusCreated, map[string]any{"id": id, "webhook_path": webhookPath})
}

func (a *AdminAPI) listBindings(w http.ResponseWriter, r *http.Request) {
	rows, err := a.pool.Query(r.Context(),
		`SELECT id, channel, webhook_path, token_ref, aeskey_ref, status, created_at
		 FROM channel_binding WHERE app_id = $1 ORDER BY created_at`, r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id, channel, path, status string
		var tokenRef, aeskeyRef *string
		var createdAt time.Time
		if err := rows.Scan(&id, &channel, &path, &tokenRef, &aeskeyRef, &status, &createdAt); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		out = append(out, map[string]any{
			"id": id, "channel": channel, "webhook_path": path,
			"token_ref": tokenRef, "aeskey_ref": aeskeyRef,
			"status": status, "created_at": createdAt,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (a *AdminAPI) deleteBinding(w http.ResponseWriter, r *http.Request) {
	before := a.rowJSON(r.Context(), "channel_binding", "id", r.PathValue("binding"))
	tag, err := a.pool.Exec(r.Context(),
		`DELETE FROM channel_binding WHERE id = $1 AND app_id = $2`,
		r.PathValue("binding"), r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if tag.RowsAffected() == 0 {
		writeError(w, http.StatusNotFound, "binding not found")
		return
	}
	a.afterWrite(r, "delete_binding", tenantIDOf(before), before, nil)
	writeJSON(w, http.StatusOK, map[string]any{"deleted": true})
}

// ---------------------------------------------------------------------------
// Knowledge ingestion
// ---------------------------------------------------------------------------

// addKnowledgeDocument ingests one inline document into the shared knowledge
// store. tenant_id / app_id are forced into the document metadata so agent
// searches filtered by them stay tenant-isolated.
func (a *AdminAPI) addKnowledgeDocument(w http.ResponseWriter, r *http.Request) {
	if a.Knowledge == nil {
		writeError(w, http.StatusServiceUnavailable, "knowledge is disabled (no embedder configured)")
		return
	}
	var in struct {
		Name    string `json:"name"`
		Content string `json:"content"`
	}
	if !decodeBody(w, r, &in) {
		return
	}
	if in.Name == "" || in.Content == "" {
		writeError(w, http.StatusBadRequest, "name and content are required")
		return
	}
	var tenantID string
	err := a.pool.QueryRow(r.Context(),
		`SELECT tenant_id FROM agent_app WHERE id = $1`, r.PathValue("id")).Scan(&tenantID)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "app not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	src := &agent.DocSource{
		DocName: in.Name,
		Content: in.Content,
		Metadata: map[string]any{
			"tenant_id": tenantID,
			"app_id":    r.PathValue("id"),
		},
	}
	if err := a.Knowledge.AddSource(r.Context(), src); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("ingest document: %v", err))
		return
	}
	// The document content can be large; the audit records the metadata only.
	a.afterWrite(r, "add_knowledge_document", tenantID, nil, map[string]any{"name": in.Name})
	writeJSON(w, http.StatusCreated, map[string]any{"ingested": in.Name})
}

// ---------------------------------------------------------------------------
// Storage migrations
// ---------------------------------------------------------------------------

// createMigration starts a backend migration for one tenant. The row appears
// in the resolver snapshot within seconds (invalidation broadcast), and from
// then on the assembler dual-writes both backends; the Migrator advances the
// phases.
func (a *AdminAPI) createMigration(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Resource  string `json:"resource"`
		ToBackend string `json:"to_backend"`
	}
	if !decodeBody(w, r, &in) {
		return
	}
	if in.Resource != "session" {
		writeError(w, http.StatusBadRequest, "resource must be \"session\" (knowledge/artifact arrive later)")
		return
	}
	if in.ToBackend != "redis" && in.ToBackend != "postgres" {
		writeError(w, http.StatusBadRequest, `to_backend must be "redis" or "postgres"`)
		return
	}

	tenantID := r.PathValue("id")
	var storageCfg []byte
	err := a.pool.QueryRow(r.Context(),
		`SELECT storage_config FROM tenant WHERE id = $1 AND status = 'active'`, tenantID).Scan(&storageCfg)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "tenant not found or inactive")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	from := a.DefaultSessionBackend
	if from == "" {
		from = "redis"
	}
	if override := parseSessionBackendOverride(storageCfg); override != "" {
		from = override
	}
	if from == in.ToBackend {
		writeError(w, http.StatusConflict, "tenant already on "+in.ToBackend)
		return
	}

	var id string
	err = a.pool.QueryRow(r.Context(),
		`INSERT INTO storage_migration (tenant_id, resource, from_backend, to_backend, phase)
		 VALUES ($1, $2, $3, $4, 'dual_write') RETURNING id`,
		tenantID, in.Resource, from, in.ToBackend).Scan(&id)
	if err != nil {
		if strings.Contains(err.Error(), "uk_storage_migration_active") {
			writeError(w, http.StatusConflict, "an active migration already exists for this tenant/resource")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	a.afterWrite(r, "create_storage_migration", tenantID, nil, map[string]any{
		"id": id, "resource": in.Resource, "from": from, "to": in.ToBackend,
	})
	writeJSON(w, http.StatusCreated, map[string]any{"id": id, "phase": "dual_write"})
}

func (a *AdminAPI) getMigration(w http.ResponseWriter, r *http.Request) {
	var (
		tenantID, resource, from, to, phase string
		progress                            []byte
		migErr                              *string
		createdAt, updatedAt                time.Time
	)
	err := a.pool.QueryRow(r.Context(),
		`SELECT tenant_id, resource, from_backend, to_backend, phase, progress, error, created_at, updated_at
		 FROM storage_migration WHERE id = $1`, r.PathValue("id"),
	).Scan(&tenantID, &resource, &from, &to, &phase, &progress, &migErr, &createdAt, &updatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "migration not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id": r.PathValue("id"), "tenant_id": tenantID, "resource": resource,
		"from_backend": from, "to_backend": to, "phase": phase,
		"progress": jsonOrNull(progress), "error": migErr,
		"created_at": createdAt, "updated_at": updatedAt,
	})
}

// parseSessionBackendOverride reads storage_config.session.type.
func parseSessionBackendOverride(raw []byte) string {
	var c struct {
		Session struct {
			Type string `json:"type"`
		} `json:"session"`
	}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &c)
	}
	return c.Session.Type
}

// ---------------------------------------------------------------------------
// Audit query
// ---------------------------------------------------------------------------

func (a *AdminAPI) queryAudit(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := a.pool.Query(r.Context(),
		`SELECT id, tenant_id, COALESCE(channel,''), COALESCE(user_id,''),
		        COALESCE(session_id::text,''), COALESCE(tool_name,''), decision,
		        COALESCE(error_type,''), COALESCE(latency_ms,0), COALESCE(trace_id,''),
		        detail, created_at
		 FROM audit_log
		 WHERE ($1 = '' OR tenant_id = $1::uuid)
		   AND ($2 = '' OR session_id = $2::uuid)
		   AND ($3 = '' OR trace_id = $3)
		   AND ($4 = '' OR decision = $4)
		 ORDER BY created_at DESC LIMIT $5`,
		q.Get("tenant_id"), q.Get("session_id"), q.Get("trace_id"), q.Get("decision"), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id, tenantID, channel, userID, sessionID, toolName, decision, errorType, traceID string
		var latency int
		var detail []byte
		var createdAt time.Time
		if err := rows.Scan(&id, &tenantID, &channel, &userID, &sessionID, &toolName,
			&decision, &errorType, &latency, &traceID, &detail, &createdAt); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		out = append(out, map[string]any{
			"id": id, "tenant_id": tenantID, "channel": channel, "user_id": userID,
			"session_id": sessionID, "tool_name": toolName, "decision": decision,
			"error_type": errorType, "latency_ms": latency, "trace_id": traceID,
			"detail": jsonOrNull(detail), "created_at": createdAt,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// afterWrite audits the write operation (operator + before/after content)
// and broadcasts config invalidation: workers drop their cache within
// seconds; TTL is the fallback when the notification is lost.
func (a *AdminAPI) afterWrite(r *http.Request, op, tenantID string, before, after any) {
	if a.rdb != nil {
		if err := tenant.PublishInvalidation(r.Context(), a.rdb); err != nil {
			plog.Warnf("admin %s: invalidation broadcast failed (TTL fallback): %v", op, err)
		}
	}
	if a.auditor == nil {
		return
	}
	if tenantID == "" {
		tenantID = "00000000-0000-0000-0000-000000000000"
	}
	operator := r.Header.Get("X-Admin-User")
	if operator == "" {
		operator = "admin"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := a.auditor.LogSync(ctx, storage.AuditEvent{
		TenantID: tenantID, Channel: "admin", UserID: operator,
		ToolName: op, Decision: "allow", Detail: changeDetail(before, after),
	}); err != nil {
		plog.Errorf("admin %s audit failed: %v", op, err)
	}
}

// changeDetail packs the before/after images of a change, dropping nil sides.
func changeDetail(before, after any) json.RawMessage {
	if before == nil && after == nil {
		return nil
	}
	d := map[string]any{}
	if before != nil {
		d["before"] = before
	}
	if after != nil {
		d["after"] = after
	}
	raw, err := json.Marshal(d)
	if err != nil {
		return nil
	}
	return raw
}

// rowJSON reads one row as JSONB for the audit's before image. A missing row
// yields nil (delete of a phantom); read errors are logged and yield nil —
// audit detail must never fail the operation itself.
func (a *AdminAPI) rowJSON(ctx context.Context, table, idColumn, id string) json.RawMessage {
	var raw []byte
	err := a.pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT to_jsonb(t) FROM %s t WHERE %s = $1`, table, idColumn), id).Scan(&raw)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			plog.Warnf("audit before-image read %s.%s=%s: %v", table, idColumn, id, err)
		}
		return nil
	}
	return raw
}

type httpError struct {
	status int
	msg    string
}

func (e httpError) Error() string { return e.msg }

func errNotFound(msg string) error { return httpError{http.StatusNotFound, msg} }
func errConflict(msg string) error { return httpError{http.StatusConflict, msg} }
func errBadRequest(msg string) error {
	return httpError{http.StatusBadRequest, msg}
}

func errStatus(err error) int {
	var he httpError
	if errors.As(err, &he) {
		return he.status
	}
	return http.StatusInternalServerError
}

func decodeBody(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// rawOrNil maps empty JSON to NULL (nullable jsonb columns).
func rawOrNil(v json.RawMessage) any {
	if len(v) == 0 {
		return nil
	}
	return []byte(v)
}

// jsonOrNull renders a nullable jsonb column back as JSON.
func jsonOrNull(v []byte) any {
	if len(v) == 0 {
		return nil
	}
	return json.RawMessage(v)
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// tenantIDOf extracts tenant_id from a to_jsonb row image (audit detail).
func tenantIDOf(row json.RawMessage) string {
	var m struct {
		TenantID string `json:"tenant_id"`
	}
	if err := json.Unmarshal(row, &m); err != nil {
		return ""
	}
	return m.TenantID
}
