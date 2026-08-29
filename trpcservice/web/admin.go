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

	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// AdminAPI serves the management endpoints of design 5.4: tenant/app CRUD,
// publish/rollback with atomic switching, channel binding management and
// audit queries. It is intended for internal networks only; authentication is
// a bearer token (TRPC_ADMIN_TOKEN) — empty means dev mode (no auth).
//
// Storage-migration endpoints (5.2.6) are intentionally absent until the
// migration executor lands.
type AdminAPI struct {
	pool    *pgxpool.Pool
	auditor *storage.Auditor // nil disables write-op auditing
	rdb     *redis.Client    // nil disables invalidation broadcasts
	token   string
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

// ---------------------------------------------------------------------------
// Tenants
// ---------------------------------------------------------------------------

func (a *AdminAPI) createTenant(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Name          string          `json:"name"`
		ModelConfig   json.RawMessage `json:"model_config"`
		ToolPolicy    json.RawMessage `json:"tool_policy"`
		AuditPolicy   json.RawMessage `json:"audit_policy"`
		StorageConfig json.RawMessage `json:"storage_config"`
	}
	if !decodeBody(w, r, &in) {
		return
	}
	if in.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	var id string
	err := a.pool.QueryRow(r.Context(),
		`INSERT INTO tenant (name, model_config, tool_policy, audit_policy, storage_config)
		 VALUES ($1, $2, $3, $4, $5) RETURNING id`,
		in.Name, rawOrNil(in.ModelConfig), rawOrNil(in.ToolPolicy),
		rawOrNil(in.AuditPolicy), rawOrNil(in.StorageConfig)).Scan(&id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	a.afterWrite(r, "create_tenant", id)
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
		name, status                            string
		modelCfg, toolPol, auditPol, storageCfg []byte
		createdAt, updatedAt                    time.Time
	)
	err := a.pool.QueryRow(r.Context(),
		`SELECT name, status, model_config, tool_policy, audit_policy, storage_config, created_at, updated_at
		 FROM tenant WHERE id = $1`, r.PathValue("id"),
	).Scan(&name, &status, &modelCfg, &toolPol, &auditPol, &storageCfg, &createdAt, &updatedAt)
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
		"audit_policy": jsonOrNull(auditPol), "storage_config": jsonOrNull(storageCfg),
		"created_at": createdAt, "updated_at": updatedAt,
	})
}

func (a *AdminAPI) updateTenant(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Name          *string         `json:"name"`
		Status        *string         `json:"status"`
		ModelConfig   json.RawMessage `json:"model_config"`
		ToolPolicy    json.RawMessage `json:"tool_policy"`
		AuditPolicy   json.RawMessage `json:"audit_policy"`
		StorageConfig json.RawMessage `json:"storage_config"`
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
		add("status = $%d", *in.Status)
	}
	if in.ModelConfig != nil {
		add("model_config = $%d", []byte(in.ModelConfig))
	}
	if in.ToolPolicy != nil {
		add("tool_policy = $%d", []byte(in.ToolPolicy))
	}
	if in.AuditPolicy != nil {
		add("audit_policy = $%d", []byte(in.AuditPolicy))
	}
	if in.StorageConfig != nil {
		// storage_config changes must go through the migration flow (design
		// 5.2.6); live switching is rejected here.
		writeError(w, http.StatusConflict,
			"storage_config cannot be changed directly; use the migration flow")
		return
	}
	if len(sets) == 0 {
		writeError(w, http.StatusBadRequest, "nothing to update")
		return
	}
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
	a.afterWrite(r, "update_tenant", r.PathValue("id"))
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
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	a.afterWrite(r, "create_app", tenantID)
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
// snapshots so rollback stays a pure status switch (design 5.2.3).
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
	a.afterWrite(r, "update_app", r.PathValue("id"))
	writeJSON(w, http.StatusOK, map[string]any{"updated": true})
}

// publishApp atomically switches the (tenant, name) published version: the
// previously published version leaves published status and the target takes
// it, guarded by the partial unique index (design 5.1.3).
func (a *AdminAPI) publishApp(w http.ResponseWriter, r *http.Request) {
	if err := a.publish(r.Context(), r.PathValue("id")); err != nil {
		writeError(w, errStatus(err), err.Error())
		return
	}
	a.afterWrite(r, "publish_app", "")
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
	if err := a.publish(ctx, targetID); err != nil {
		writeError(w, errStatus(err), err.Error())
		return
	}
	a.afterWrite(r, "rollback_app", tenantID)
	writeJSON(w, http.StatusOK, map[string]any{"published": targetID, "version": target})
}

// publish switches the published version of the app family in one tx.
func (a *AdminAPI) publish(ctx context.Context, appID string) error {
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var tenantID, name, status string
	err = tx.QueryRow(ctx,
		`SELECT tenant_id, name, status FROM agent_app WHERE id = $1 FOR UPDATE`, appID).
		Scan(&tenantID, &name, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return errNotFound("app not found")
	}
	if err != nil {
		return err
	}
	if status == "published" {
		return errConflict("app version is already published")
	}
	if _, err := tx.Exec(ctx,
		`UPDATE agent_app SET status = 'disabled', updated_at = now()
		 WHERE tenant_id = $1 AND name = $2 AND status = 'published'`,
		tenantID, name); err != nil {
		return fmt.Errorf("unpublish current: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE agent_app SET status = 'published', updated_at = now() WHERE id = $1`,
		appID); err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	return tx.Commit(ctx)
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
	if in.Channel == "" || in.WebhookPath == "" {
		writeError(w, http.StatusBadRequest, "channel and webhook_path are required")
		return
	}
	var id, tenantID string
	err := a.pool.QueryRow(r.Context(),
		`INSERT INTO channel_binding (tenant_id, channel, app_id, webhook_path, token_ref, aeskey_ref, config)
		 SELECT tenant_id, $2, id, $3, $4, $5, $6 FROM agent_app WHERE id = $1
		 RETURNING id, tenant_id`,
		r.PathValue("id"), in.Channel, in.WebhookPath,
		nullStr(in.TokenRef), nullStr(in.AESKeyRef), rawOrNil(in.Config),
	).Scan(&id, &tenantID)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "app not found")
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
	a.afterWrite(r, "create_binding", tenantID)
	writeJSON(w, http.StatusCreated, map[string]any{"id": id})
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
	a.afterWrite(r, "delete_binding", "")
	writeJSON(w, http.StatusOK, map[string]any{"deleted": true})
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
		        COALESCE(error_type,''), COALESCE(latency_ms,0), COALESCE(trace_id,''), created_at
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
		var createdAt time.Time
		if err := rows.Scan(&id, &tenantID, &channel, &userID, &sessionID, &toolName,
			&decision, &errorType, &latency, &traceID, &createdAt); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		out = append(out, map[string]any{
			"id": id, "tenant_id": tenantID, "channel": channel, "user_id": userID,
			"session_id": sessionID, "tool_name": toolName, "decision": decision,
			"error_type": errorType, "latency_ms": latency, "trace_id": traceID,
			"created_at": createdAt,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// afterWrite audits the write operation and broadcasts config invalidation
// (design 5.2.3: workers drop their cache within seconds; TTL is the
// fallback when the notification is lost).
func (a *AdminAPI) afterWrite(r *http.Request, op, tenantID string) {
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
		ToolName: op, Decision: "allow",
	}); err != nil {
		plog.Errorf("admin %s audit failed: %v", op, err)
	}
}

type httpError struct {
	status int
	msg    string
}

func (e httpError) Error() string { return e.msg }

func errNotFound(msg string) error { return httpError{http.StatusNotFound, msg} }
func errConflict(msg string) error { return httpError{http.StatusConflict, msg} }

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
