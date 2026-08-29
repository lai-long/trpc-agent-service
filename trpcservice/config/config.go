// Package config loads the service's own configuration (port, DB, Redis,
// logging) and provides the Secret Resolver abstraction.
//
// It only covers service-level config; tenant-level config (model_config
// etc.) lives in the PG tenant tables and is owned by the tenant module.
//
// Configuration comes from environment variables only (12-factor / K8s
// friendly), no config files. Secrets never appear in plaintext config —
// only references, resolved at runtime through a SecretResolver.
package config

import (
	"fmt"
	"os"
)

// Config aggregates the service's own configuration.
type Config struct {
	HTTPAddr   string // TRPC_HTTP_ADDR: Gateway/HTTP listen address
	PGDSN      string // TRPC_PG_DSN: PostgreSQL DSN (required for worker/admin roles)
	RedisAddr  string // TRPC_REDIS_ADDR: Redis address (required for gateway/worker roles)
	LogLevel   string // TRPC_LOG_LEVEL: debug/info/warn/error
	LogFormat  string // TRPC_LOG_FORMAT: "json" for JSON output, anything else for console
	SecretsDir string // TRPC_SECRETS_DIR: key directory for the local file-based SecretResolver

	ModelBaseURL   string // TRPC_MODEL_BASE_URL: OpenAI-compatible endpoint (DeepSeek default)
	ModelName      string // TRPC_MODEL_NAME: model name, e.g. deepseek-v4-flash (cheapest)
	ModelAPIKeyRef string // TRPC_MODEL_APIKEY_REF: secret ref (NOT the key itself) resolved via SecretResolver

	// SessionBackend selects the session store: "redis" (default, hot data)
	// or "postgres" (event journal + snapshot in the 5.1.3 tables).
	SessionBackend string // TRPC_SESSION_BACKEND
	// AppName is the framework runner's app name. Until per-app Runner
	// assembly lands (Admin API stage), it carries the agent_app UUID the
	// deployment serves — the PG session backend maps it onto session.app_id.
	// The default is the seed demo app.
	AppName string // TRPC_APP_NAME

	// WeCom channel: enabled when TRPC_WECOM_CORP_ID is set. All secret
	// material is referenced, resolved via SecretResolver.
	WecomCorpID    string // TRPC_WECOM_CORP_ID
	WecomAgentID   string // TRPC_WECOM_AGENT_ID (integer)
	WecomTokenRef  string // TRPC_WECOM_TOKEN_REF: callback token secret ref
	WecomAESKeyRef string // TRPC_WECOM_AESKEY_REF: EncodingAESKey secret ref
	WecomSecretRef string // TRPC_WECOM_SECRET_REF: corpsecret secret ref
	WecomAPIBase   string // TRPC_WECOM_API_BASE: default https://qyapi.weixin.qq.com

	// AdminToken guards the Admin API (Authorization: Bearer); empty means
	// dev mode with no auth — set it in any shared environment.
	AdminToken string // TRPC_ADMIN_TOKEN
}

// Load reads configuration from environment variables, filling defaults for
// unset ones. Roles that need PG/Redis (gateway/worker/admin) validate those
// fields at startup and fail fast.
func Load() Config {
	return Config{
		HTTPAddr:   getenv("TRPC_HTTP_ADDR", ":8080"),
		PGDSN:      getenv("TRPC_PG_DSN", "postgres://trpc:trpc-dev-only@localhost:5432/trpc?sslmode=disable"),
		RedisAddr:  getenv("TRPC_REDIS_ADDR", "localhost:6380"), // host 6379 is often taken by other local services; compose maps 6380
		LogLevel:   getenv("TRPC_LOG_LEVEL", "info"),
		LogFormat:  getenv("TRPC_LOG_FORMAT", "console"),
		SecretsDir: getenv("TRPC_SECRETS_DIR", "data/secrets"),

		ModelBaseURL:   getenv("TRPC_MODEL_BASE_URL", "https://api.deepseek.com"),
		ModelName:      getenv("TRPC_MODEL_NAME", "deepseek-v4-flash"),
		ModelAPIKeyRef: getenv("TRPC_MODEL_APIKEY_REF", "deepseek-apikey"),

		SessionBackend: getenv("TRPC_SESSION_BACKEND", "redis"),
		AppName:        getenv("TRPC_APP_NAME", "00000000-0000-0000-0000-000000000101"),

		WecomCorpID:    getenv("TRPC_WECOM_CORP_ID", ""),
		WecomAgentID:   getenv("TRPC_WECOM_AGENT_ID", ""),
		WecomTokenRef:  getenv("TRPC_WECOM_TOKEN_REF", "wecom-token"),
		WecomAESKeyRef: getenv("TRPC_WECOM_AESKEY_REF", "wecom-aeskey"),
		WecomSecretRef: getenv("TRPC_WECOM_SECRET_REF", "wecom-secret"),
		WecomAPIBase:   getenv("TRPC_WECOM_API_BASE", "https://qyapi.weixin.qq.com"),

		AdminToken: getenv("TRPC_ADMIN_TOKEN", ""),
	}
}

// MustEnv reads a required environment variable and returns an error if it is
// unset (for startup validation in roles like worker).
func MustEnv(key string) (string, error) {
	v := os.Getenv(key)
	if v == "" {
		return "", fmt.Errorf("required env %s is not set", key)
	}
	return v, nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
