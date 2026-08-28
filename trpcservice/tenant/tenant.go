// Package tenant models multi-tenant isolation for config, data, tools, and keys.
//
// The package owns the tenant / agent app / channel binding models, their
// loading from PG, and the cached Resolver the Gateway uses to route an
// inbound callback to its tenant and app:
//
//	webhook_path → channel_binding → tenant + agent_app
//
// The stamped tenant_id then travels on the message through the Worker into
// audit and metrics, giving every downstream component its isolation key.
package tenant

import "encoding/json"

// Status values stored in the status columns.
const (
	StatusActive   = "active"
	StatusDisabled = "disabled"
)

// Tenant is one row of the tenant table. Dynamic config (model_config,
// tool_policy, audit_policy, storage_config) is carried raw and parsed by
// its consumers (agent assembly, guardrail, storage routing) — routing
// itself only needs identity and status.
type Tenant struct {
	ID            string
	Name          string
	ModelConfig   json.RawMessage
	ToolPolicy    json.RawMessage
	AuditPolicy   json.RawMessage
	StorageConfig json.RawMessage
	Status        string
}

// AgentApp is one row of agent_app: a versioned agent configuration owned by
// a tenant (status draft / published / disabled).
type AgentApp struct {
	ID        string
	TenantID  string
	Name      string
	AgentType string
	Config    json.RawMessage
	Version   int
	Status    string
}

// ChannelBinding is one row of channel_binding: binds an IM channel webhook
// to an agent app. Secret material stays as references (token_ref /
// aeskey_ref) and is resolved through config.SecretResolver at use time —
// never logged.
type ChannelBinding struct {
	ID          string
	TenantID    string
	Channel     string
	AppID       string
	WebhookPath string
	TokenRef    string
	AESKeyRef   string
	Config      json.RawMessage
	Status      string
}
