package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/session"
	ttool "trpc.group/trpc-go/trpc-agent-go/tool"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool"
)

// ModelSpec carries OpenAI-compatible model settings. tenant.model_config
// uses this shape directly; agent_app.config nests it under "model".
// Precedence: app config → tenant model_config → the service env default
// (AssemblerConfig.Defaults).
type ModelSpec struct {
	Name        string   `json:"name"`
	BaseURL     string   `json:"base_url"`
	APIKeyRef   string   `json:"api_key_ref"` // secret ref, never the key itself
	Temperature *float64 `json:"temperature"`
}

// mergeModel overlays non-zero fields of each later spec onto the former.
func mergeModel(base ModelSpec, overrides ...ModelSpec) ModelSpec {
	out := base
	for _, o := range overrides {
		if o.Name != "" {
			out.Name = o.Name
		}
		if o.BaseURL != "" {
			out.BaseURL = o.BaseURL
		}
		if o.APIKeyRef != "" {
			out.APIKeyRef = o.APIKeyRef
		}
		if o.Temperature != nil {
			out.Temperature = o.Temperature
		}
	}
	return out
}

// appConfig is the parsed agent_app.config JSONB. All fields are optional;
// unset fields inherit the tenant policy or the service default.
type appConfig struct {
	Prompt string          `json:"prompt"` // system instruction
	Model  ModelSpec       `json:"model"`
	Tools  tool.ToolPolicy `json:"tools"` // narrows the tenant tool whitelist
}

func parseAppConfig(raw json.RawMessage) (appConfig, error) {
	var c appConfig
	if len(raw) == 0 {
		return c, nil
	}
	if err := json.Unmarshal(raw, &c); err != nil {
		return c, fmt.Errorf("parse app config: %w", err)
	}
	return c, nil
}

func parseModelSpec(raw json.RawMessage) ModelSpec {
	var m ModelSpec
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &m); err != nil {
			plog.Warnf("invalid tenant model_config ignored: %v", err)
		}
	}
	return m
}

func parseToolPolicy(raw json.RawMessage) tool.ToolPolicy {
	var p tool.ToolPolicy
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &p); err != nil {
			plog.Warnf("invalid tenant tool_policy ignored: %v", err)
		}
	}
	return p
}

// StorageConfig is the tenant's storage backend override (design 5.1.1 受控
// 菜单): empty means the platform default stack. Only session has a choice
// today (redis / postgres); unknown values fall back to the default with a
// warning. Changes arrive only through the migration flow — a direct edit is
// rejected by the Admin API.
type StorageConfig struct {
	Session struct {
		Type string `json:"type"` // "redis" | "postgres"
	} `json:"session"`
}

func parseStorageConfig(raw json.RawMessage) StorageConfig {
	var c StorageConfig
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &c); err != nil {
			plog.Warnf("invalid tenant storage_config ignored: %v", err)
		}
	}
	return c
}

// AppProvider resolves the app a message was routed to, plus its owning
// tenant (*tenant.Resolver satisfies it).
type AppProvider interface {
	AppByID(ctx context.Context, appID string) (tenant.AgentApp, tenant.Tenant, error)
	// ActiveMigration reports a tenant's in-flight storage migration, nil when
	// none; while one is active the assembler dual-writes both backends.
	ActiveMigration(tenantID, resource string) *tenant.Migration
}

// keyError marks a model key resolution failure: the app is served by the
// echo processor for this message, but the failure is not cached so a
// repaired secret takes effect on the next message.
type keyError struct{ Err error }

func (e *keyError) Error() string { return e.Err.Error() }
func (e *keyError) Unwrap() error { return e.Err }

// Assembler builds a Runner-backed Processor per agent app (design 4.3:
// tenant-level agent registration and routing) and caches assemblies keyed by
// app ID. An entry is rebuilt whenever the app config or the tenant's
// model_config / tool_policy bytes change; the Resolver underneath refreshes
// on TTL plus the Admin API's pub/sub invalidation (design 5.2.3), so a
// publish/rollback propagates to workers within seconds.
//
// Replaced runners are not closed on eviction — an in-flight message may
// still hold one; they are closed all together at Close (process exit).
type Assembler struct {
	cfg AssemblerConfig

	mu    sync.RWMutex
	cache map[string]cacheEntry
	live  []*RunnerProcessor // every assembled runner, closed at Close
}

type cacheEntry struct {
	proc        Processor
	fingerprint []byte
}

// AssemblerConfig carries the shared dependencies every per-app assembly
// reuses: memory/knowledge backends are process-level, while model,
// prompt and tools come from the app config and tenant policies.
type AssemblerConfig struct {
	Apps      AppProvider // nil: env-only single-app mode (PG down at startup)
	Secrets   config.SecretResolver
	Registry  *tool.Registry
	Memory    memory.Service
	Knowledge knowledge.Knowledge
	Callbacks *ttool.Callbacks

	// SessionsByType holds one session service per supported backend
	// ("redis" / "postgres"); DefaultSession is the key of the
	// platform-recommended stack. The tenant's storage_config.session.type
	// routes its apps to the chosen backend; empty/unknown means the default.
	SessionsByType map[string]session.Service
	DefaultSession string
	// Artifact, when set, is wired onto every per-app runner (design:
	// Artifact = S3, tenant differentiation is a later menu item).
	Artifact artifact.Service

	// Defaults is the env model config (lowest precedence); DefaultApp is the
	// runner app name for unrouted messages (TRPC_APP_NAME, routing disabled).
	Defaults   ModelSpec
	DefaultApp string
	// Timeout / Retries for every per-app runner (design 5.2.2).
	Timeout time.Duration
	Retries int
}

// NewAssembler creates an Assembler.
func NewAssembler(cfg AssemblerConfig) *Assembler {
	return &Assembler{cfg: cfg, cache: make(map[string]cacheEntry)}
}

// Process implements Processor: dispatch the message to the runner assembled
// for msg.AppID (or to the default app when routing is disabled).
func (a *Assembler) Process(ctx context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
	p, err := a.processorFor(ctx, msg.AppID)
	if err != nil {
		return channels.OutboundMessage{}, err
	}
	return p.Process(ctx, msg)
}

// processorFor returns the cached processor for the app, assembling it when
// the app config or tenant policies changed since the cached entry was built.
func (a *Assembler) processorFor(ctx context.Context, appID string) (Processor, error) {
	app, t, err := a.resolveApp(ctx, appID)
	if err != nil {
		return nil, err
	}
	fp := fingerprint(app.Config, t.ToolPolicy, t.ModelConfig, t.StorageConfig, migrationFingerprint(a.activeMigration(t.ID)))

	a.mu.RLock()
	entry, ok := a.cache[app.ID]
	a.mu.RUnlock()
	if ok && bytes.Equal(entry.fingerprint, fp) {
		return entry.proc, nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if entry, ok := a.cache[app.ID]; ok && bytes.Equal(entry.fingerprint, fp) {
		return entry.proc, nil
	}
	proc, err := a.assemble(ctx, app, t)
	if err != nil {
		var ke *keyError
		if errors.As(err, &ke) {
			// Demoability over strictness: no model key means this app echoes.
			// Uncached on purpose — see keyError.
			plog.Warnf("model key for app %s unavailable (%v), serving echo", app.ID, ke.Err)
			return EchoProcessor{}, nil
		}
		return nil, fmt.Errorf("assemble app %s: %w", app.ID, err)
	}
	if rp, ok := proc.(*RunnerProcessor); ok {
		a.live = append(a.live, rp)
	}
	a.cache[app.ID] = cacheEntry{proc: proc, fingerprint: fp}
	plog.Infof("assembled runner for app %s (name=%s, version=%d)", app.ID, app.Name, app.Version)
	return proc, nil
}

// resolveApp maps the stamped app ID to its config and tenant. Messages with
// an empty app ID arrive only when gateway routing is disabled; they fall
// back to the env-configured default app (the single-tenant dev path).
func (a *Assembler) resolveApp(ctx context.Context, appID string) (tenant.AgentApp, tenant.Tenant, error) {
	if appID == "" {
		if a.cfg.Apps != nil && a.cfg.DefaultApp != "" {
			if app, t, err := a.cfg.Apps.AppByID(ctx, a.cfg.DefaultApp); err == nil {
				return app, t, nil
			}
		}
		return tenant.AgentApp{ID: a.cfg.DefaultApp, Name: "default"}, tenant.Tenant{}, nil
	}
	if a.cfg.Apps == nil {
		return tenant.AgentApp{}, tenant.Tenant{},
			fmt.Errorf("no app provider (PG down) for app %s", appID)
	}
	return a.cfg.Apps.AppByID(ctx, appID)
}

// assemble builds the RunnerProcessor for one app from its config, the tenant
// policies and the shared process-level dependencies.
func (a *Assembler) assemble(ctx context.Context, app tenant.AgentApp, t tenant.Tenant) (Processor, error) {
	ac, err := parseAppConfig(app.Config)
	if err != nil {
		return nil, err
	}
	spec := mergeModel(a.cfg.Defaults, parseModelSpec(t.ModelConfig), ac.Model)
	key, err := a.cfg.Secrets.Resolve(ctx, spec.APIKeyRef)
	if err != nil {
		return nil, &keyError{Err: fmt.Errorf("resolve model key %q: %w", spec.APIKeyRef, err)}
	}

	// Tool isolation: the platform registry is narrowed by the tenant
	// whitelist first, then by the app's own policy (design 4.3).
	tools := a.cfg.Registry.Allowed(parseToolPolicy(t.ToolPolicy), ac.Tools)

	// Session backend routing (design 5.1.1): the tenant's storage_config
	// picks from the controlled menu; empty/unknown means the platform
	// default. Session history does not follow the runner across backends —
	// switching happens through the migration flow (5.2.6).
	sess := a.sessionServiceFor(t)

	// Knowledge isolation: the shared pgvector base is filtered down to this
	// tenant/app pair. Without a resolved tenant (env-only fallback) the
	// filter stays empty — that path is single-tenant dev only.
	var filter map[string]any
	if t.ID != "" {
		filter = map[string]any{"tenant_id": t.ID, "app_id": app.ID}
	}
	return NewRunnerProcessor(RunnerConfig{
		AppName:         app.ID,
		BaseURL:         spec.BaseURL,
		APIKey:          key,
		ModelName:       spec.Name,
		Instruction:     ac.Prompt,
		Temperature:     spec.Temperature,
		SessionService:  sess,
		ArtifactService: a.cfg.Artifact,
		Timeout:         a.cfg.Timeout,
		Retries:         a.cfg.Retries,
		Tools:           tools,
		ToolCallbacks:   a.cfg.Callbacks,
		MemoryService:   a.cfg.Memory,
		Knowledge:       a.cfg.Knowledge,
		KnowledgeFilter: filter,
	}), nil
}

// sessionServiceFor picks the tenant's session backend from the controlled
// menu (design 5.1.1); empty or unknown types fall back to the platform
// default with a warning. During a migration (design 5.2.6) the choice wraps
// in a dual-write fanout: reads follow the phase (old backend until the read
// switch, new one while observing), writes hit both.
func (a *Assembler) sessionServiceFor(t tenant.Tenant) session.Service {
	mig := a.activeMigration(t.ID)
	if mig != nil {
		primary, secondary := mig.FromBackend, mig.ToBackend
		if mig.Phase == tenant.PhaseObserving {
			primary, secondary = secondary, primary
		}
		p, pok := a.cfg.SessionsByType[primary]
		s, sok := a.cfg.SessionsByType[secondary]
		if pok && sok {
			return &storage.FanoutSessionService{Primary: p, Secondary: s}
		}
		plog.Warnf("tenant %s migration backend pair %s/%s unavailable, using default %s",
			t.ID, primary, secondary, a.cfg.DefaultSession)
	}
	def := a.cfg.SessionsByType[a.cfg.DefaultSession]
	st := parseStorageConfig(t.StorageConfig).Session.Type
	if st == "" {
		return def
	}
	alt, ok := a.cfg.SessionsByType[st]
	if !ok {
		plog.Warnf("tenant %s storage_config session type %q unavailable, using default %s",
			t.ID, st, a.cfg.DefaultSession)
		return def
	}
	return alt
}

// activeMigration returns the tenant's in-flight session migration, nil when
// routing is disabled or none is active.
func (a *Assembler) activeMigration(tenantID string) *tenant.Migration {
	if a.cfg.Apps == nil || tenantID == "" {
		return nil
	}
	return a.cfg.Apps.ActiveMigration(tenantID, "session")
}

// migrationFingerprint distinguishes assemblies built under different
// migration states so a phase change rebuilds the runner.
func migrationFingerprint(m *tenant.Migration) []byte {
	if m == nil {
		return nil
	}
	return []byte(m.FromBackend + ">" + m.ToBackend + "@" + m.Phase)
}

// Close shuts down every runner the assembler created (process exit).
func (a *Assembler) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	var first error
	for _, p := range a.live {
		if err := p.Close(); err != nil && first == nil {
			first = err
		}
	}
	a.live = nil
	a.cache = make(map[string]cacheEntry)
	return first
}

// fingerprint identifies the inputs an assembly depends on; a change in any
// of them (publish, rollback, policy edit) rebuilds the runner. JSONB columns
// round-trip in normalized form, so byte equality matches logical equality.
func fingerprint(parts ...json.RawMessage) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, 0)
		out = append(out, p...)
	}
	return out
}
