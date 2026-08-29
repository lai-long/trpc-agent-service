package web_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/web"
)

// adminTestAPI builds the API over the compose PG; skips when unreachable.
// Rows created by the test are cleaned up.
func adminTestAPI(t *testing.T) (*http.ServeMux, *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	pool, err := storage.NewPG(ctx, "postgres://trpc:trpc-dev-only@localhost:5432/trpc?sslmode=disable")
	if err != nil {
		t.Skipf("postgres unavailable (%v), skipping integration test", err)
	}
	t.Cleanup(func() { pool.Close() })

	mux := http.NewServeMux()
	web.NewAdminAPI(pool, nil, nil, "").RegisterRoutes(mux)
	return mux, pool
}

func doJSON(t *testing.T, mux *http.ServeMux, method, path, body string) (int, map[string]any) {
	t.Helper()
	var rdr *bytes.Reader
	if body != "" {
		rdr = bytes.NewReader([]byte(body))
	} else {
		rdr = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, rdr)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	var out map[string]any
	if rec.Body.Len() > 0 {
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("%s %s: decode response: %v (body %q)", method, path, err, rec.Body.String())
		}
	}
	return rec.Code, out
}

func doJSONList(t *testing.T, mux *http.ServeMux, path string) []map[string]any {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s: status %d (%s)", path, rec.Code, rec.Body.String())
	}
	var out []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("GET %s: decode: %v", path, err)
	}
	return out
}

func TestAdminLifecycle(t *testing.T) {
	mux, pool := adminTestAPI(t)
	ctx := context.Background()

	var tenantID, appV1, appV2, bindingID string
	t.Cleanup(func() {
		if tenantID != "" {
			_, _ = pool.Exec(ctx, `DELETE FROM channel_binding WHERE app_id IN
				(SELECT id FROM agent_app WHERE tenant_id = $1)`, tenantID)
			_, _ = pool.Exec(ctx, `DELETE FROM agent_app WHERE tenant_id = $1`, tenantID)
			_, _ = pool.Exec(ctx, `DELETE FROM tenant WHERE id = $1`, tenantID)
		}
	})

	// Create tenant.
	code, out := doJSON(t, mux, http.MethodPost, "/admin/tenants",
		`{"name":"admin-test-tenant","tool_policy":{"rate_limit":100}}`)
	if code != http.StatusCreated {
		t.Fatalf("create tenant: %d %v", code, out)
	}
	tenantID, _ = out["id"].(string)
	if tenantID == "" {
		t.Fatal("no tenant id returned")
	}

	// Detail carries the jsonb config.
	code, out = doJSON(t, mux, http.MethodGet, "/admin/tenants/"+tenantID, "")
	if code != http.StatusOK {
		t.Fatalf("get tenant: %d", code)
	}
	if pol, ok := out["tool_policy"].(map[string]any); !ok || pol["rate_limit"] != float64(100) {
		t.Fatalf("tool_policy round trip failed: %v", out["tool_policy"])
	}

	// storage_config is migration-flow only (risk 9): direct change rejected.
	code, _ = doJSON(t, mux, http.MethodPatch, "/admin/tenants/"+tenantID,
		`{"storage_config":{"session":{"type":"pg"}}}`)
	if code != http.StatusConflict {
		t.Fatalf("storage_config patch must be 409, got %d", code)
	}

	// Create two versions of one app; both start as draft.
	code, out = doJSON(t, mux, http.MethodPost, "/admin/tenants/"+tenantID+"/apps",
		`{"name":"bot","agent_type":"llm","config":{"prompt":"v1"}}`)
	if code != http.StatusCreated {
		t.Fatalf("create app v1: %d %v", code, out)
	}
	appV1, _ = out["id"].(string)
	if out["version"] != float64(1) || out["status"] != "draft" {
		t.Fatalf("want draft v1, got %v", out)
	}
	code, out = doJSON(t, mux, http.MethodPost, "/admin/tenants/"+tenantID+"/apps",
		`{"name":"bot","agent_type":"llm","config":{"prompt":"v2"}}`)
	if code != http.StatusCreated {
		t.Fatalf("create app v2: %d %v", code, out)
	}
	appV2, _ = out["id"].(string)
	if out["version"] != float64(2) {
		t.Fatalf("want v2, got %v", out)
	}

	// Draft config is editable; published versions are not.
	code, _ = doJSON(t, mux, http.MethodPatch,
		fmt.Sprintf("/admin/tenants/%s/apps/%s", tenantID, appV2), `{"config":{"prompt":"v2.1"}}`)
	if code != http.StatusOK {
		t.Fatalf("edit draft: %d", code)
	}

	// Publish v2, then v1: the published flag switches atomically.
	code, out = doJSON(t, mux, http.MethodPost, "/admin/apps/"+appV2+"/publish", "")
	if code != http.StatusOK {
		t.Fatalf("publish v2: %d %v", code, out)
	}
	code, _ = doJSON(t, mux, http.MethodPatch,
		fmt.Sprintf("/admin/tenants/%s/apps/%s", tenantID, appV2), `{"config":{"prompt":"x"}}`)
	if code != http.StatusConflict {
		t.Fatalf("published app must be immutable, got %d", code)
	}
	code, _ = doJSON(t, mux, http.MethodPost, "/admin/apps/"+appV1+"/publish", "")
	if code != http.StatusOK {
		t.Fatalf("publish v1 (switch): %d", code)
	}
	var published int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM agent_app WHERE tenant_id=$1 AND name='bot' AND status='published'`,
		tenantID).Scan(&published); err != nil {
		t.Fatal(err)
	}
	if published != 1 {
		t.Fatalf("exactly one published version expected, got %d", published)
	}

	// Rollback (no body): re-publishes v2, the next-lower version is wrong —
	// v1 is current, so previous is none... v2 IS higher. Rollback from v1
	// must fail (no earlier version); publish v2 again then roll back to v1.
	code, _ = doJSON(t, mux, http.MethodPost, "/admin/apps/"+appV1+"/rollback", "")
	if code != http.StatusConflict {
		t.Fatalf("rollback from v1 must have no earlier version, got %d", code)
	}
	code, _ = doJSON(t, mux, http.MethodPost, "/admin/apps/"+appV2+"/publish", "")
	if code != http.StatusOK {
		t.Fatalf("re-publish v2: %d", code)
	}
	code, out = doJSON(t, mux, http.MethodPost, "/admin/apps/"+appV2+"/rollback", "")
	if code != http.StatusOK || out["version"] != float64(1) {
		t.Fatalf("rollback to v1: %d %v", code, out)
	}

	// Bindings: create, list, delete.
	code, out = doJSON(t, mux, http.MethodPost, "/admin/apps/"+appV1+"/bindings",
		`{"channel":"wecom","webhook_path":"/wecom/admin-test","token_ref":"wecom-token"}`)
	if code != http.StatusCreated {
		t.Fatalf("create binding: %d %v", code, out)
	}
	bindingID, _ = out["id"].(string)
	// Duplicate webhook_path is rejected by the unique constraint.
	code, _ = doJSON(t, mux, http.MethodPost, "/admin/apps/"+appV1+"/bindings",
		`{"channel":"mock","webhook_path":"/wecom/admin-test"}`)
	if code != http.StatusConflict {
		t.Fatalf("duplicate webhook_path must be 409, got %d", code)
	}
	list := doJSONList(t, mux, "/admin/apps/"+appV1+"/bindings")
	if len(list) != 1 || list[0]["webhook_path"] != "/wecom/admin-test" {
		t.Fatalf("list bindings: %+v", list)
	}
	if list[0]["token_ref"] != "wecom-token" {
		t.Fatalf("token_ref must be a reference, got %v", list[0]["token_ref"])
	}
	code, _ = doJSON(t, mux, http.MethodDelete, "/admin/apps/"+appV1+"/bindings/"+bindingID, "")
	if code != http.StatusOK {
		t.Fatalf("delete binding: %d", code)
	}
}

func TestAdminAuthAndAuditQuery(t *testing.T) {
	ctx := context.Background()
	pool, err := storage.NewPG(ctx, "postgres://trpc:trpc-dev-only@localhost:5432/trpc?sslmode=disable")
	if err != nil {
		t.Skipf("postgres unavailable (%v), skipping integration test", err)
	}
	// Cleanup is LIFO: registered first, runs last — after row deletions.
	t.Cleanup(func() { pool.Close() })

	// Auth: with a token set, requests without it get 401.
	mux := http.NewServeMux()
	web.NewAdminAPI(pool, nil, nil, "secret-token").RegisterRoutes(mux)
	req := httptest.NewRequest(http.MethodGet, "/admin/tenants", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", rec.Code)
	}
	req = httptest.NewRequest(http.MethodGet, "/admin/tenants", nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("want 200 with token, got %d", rec.Code)
	}

	// Audit query: seed one row and filter by trace_id.
	if _, err := pool.Exec(ctx,
		`INSERT INTO audit_log (tenant_id, channel, decision, trace_id)
		 VALUES ('00000000-0000-0000-0000-000000000001', 'admin', 'allow', 'admin-test-trace')`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM audit_log WHERE trace_id = 'admin-test-trace'`)
	})
	mux2 := http.NewServeMux()
	web.NewAdminAPI(pool, nil, nil, "").RegisterRoutes(mux2)
	rows := doJSONList(t, mux2, "/admin/audit?trace_id=admin-test-trace")
	if len(rows) != 1 || rows[0]["decision"] != "allow" {
		t.Fatalf("audit query: %+v", rows)
	}
	rows = doJSONList(t, mux2, "/admin/audit?trace_id=admin-test-trace&decision=deny")
	if len(rows) != 0 {
		t.Fatalf("decision filter must exclude the row: %+v", rows)
	}
}
