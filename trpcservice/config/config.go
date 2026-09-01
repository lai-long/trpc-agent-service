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
	HTTPAddr string // TRPC_HTTP_ADDR: Gateway/HTTP listen address
	// AdminAddr is the Admin API listen address in split-role deployments
	// (serve admin); all-in-one shares the gateway listener. Internal only
	// (design 5.4).
	AdminAddr string // TRPC_ADMIN_ADDR
	// WorkerAddr is the worker role's metrics listener in split-role
	// deployments (serve worker); each role needs its own address on a
	// shared host.
	WorkerAddr string // TRPC_WORKER_ADDR
	PGDSN      string // TRPC_PG_DSN: PostgreSQL DSN (required for worker/admin roles)
	RedisAddr  string // TRPC_REDIS_ADDR: Redis address (required for gateway/worker roles)
	LogLevel   string // TRPC_LOG_LEVEL: debug/info/warn/error
	LogFormat  string // TRPC_LOG_FORMAT: "json" for JSON output, anything else for console
	SecretsDir string // TRPC_SECRETS_DIR: key directory for the local file-based SecretResolver

	ModelBaseURL   string // TRPC_MODEL_BASE_URL: OpenAI-compatible endpoint (DeepSeek default)
	ModelName      string // TRPC_MODEL_NAME: model name, e.g. deepseek-v4-flash (cheapest)
	ModelAPIKeyRef string // TRPC_MODEL_APIKEY_REF: secret ref (NOT the key itself) resolved via SecretResolver
	// ModelTimeout bounds one model run (design 5.2.2: 60s deadline, retry
	// once, then a busy reply). Go duration syntax.
	ModelTimeout string // TRPC_MODEL_TIMEOUT
	// ModelPrices maps model name to USD per 1M tokens for cost accounting
	// (audit_log.cost), JSON: {"deepseek-v4-flash":[0.1,0.4]}. Empty means
	// cost is not tracked (token counts always are).
	ModelPrices string // TRPC_MODEL_PRICES

	// SessionBackend selects the session store: "redis" (default, hot data)
	// or "postgres" (event journal + snapshot in the 5.1.3 tables).
	SessionBackend string // TRPC_SESSION_BACKEND
	// AppName is the fallback runner app name for messages the Gateway could
	// not route (tenant routing disabled, e.g. PG down at startup). Routed
	// messages run under their own agent_app ID; the env model config above
	// serves as the lowest-precedence default beneath tenant.model_config and
	// agent_app.config. The default is the seed demo app.
	AppName string // TRPC_APP_NAME

	// WeCom channel: enabled when TRPC_WECOM_CORP_ID is set. All secret
	// material is referenced, resolved via SecretResolver.
	WecomCorpID    string // TRPC_WECOM_CORP_ID
	WecomAgentID   string // TRPC_WECOM_AGENT_ID (integer)
	WecomTokenRef  string // TRPC_WECOM_TOKEN_REF: callback token secret ref
	WecomAESKeyRef string // TRPC_WECOM_AESKEY_REF: EncodingAESKey secret ref
	WecomSecretRef string // TRPC_WECOM_SECRET_REF: corpsecret secret ref
	WecomAPIBase   string // TRPC_WECOM_API_BASE: default https://qyapi.weixin.qq.com

	// WeChat KF (微信客服) channel: enabled when both are set. Same secret-ref
	// discipline; the KF secret is independent of the WeCom corpsecret.
	WxkfCorpID    string // TRPC_WXKF_CORP_ID
	WxkfKfAccount string // TRPC_WXKF_KF_ACCOUNT: open_kfid
	WxkfTokenRef  string // TRPC_WXKF_TOKEN_REF
	WxkfAESKeyRef string // TRPC_WXKF_AESKEY_REF
	WxkfSecretRef string // TRPC_WXKF_SECRET_REF
	WxkfAPIBase   string // TRPC_WXKF_API_BASE

	// AdminToken guards the Admin API (Authorization: Bearer); empty means
	// dev mode with no auth — set it in any shared environment.
	AdminToken string // TRPC_ADMIN_TOKEN

	// Admin mTLS (split admin role only, design 5.4): when all three are set,
	// the admin listener serves TLS and requires client certificates signed
	// by the given CA.
	AdminTLSCert     string // TRPC_ADMIN_TLS_CERT
	AdminTLSKey      string // TRPC_ADMIN_TLS_KEY
	AdminTLSClientCA string // TRPC_ADMIN_TLS_CLIENT_CA

	// SecretResolverType selects the secret backend (design 决策三): "file"
	// (local dev, default) or "kms" (KMS sidecar / Vault agent at
	// TRPC_KMS_ENDPOINT). The KMS bearer token is itself a secret, resolved
	// from TRPC_KMS_TOKEN_REF through the file resolver. Every backend is
	// wrapped in the short-TTL process cache (TRPC_SECRET_CACHE_TTL).
	SecretResolverType string // TRPC_SECRET_RESOLVER
	KMSEndpoint        string // TRPC_KMS_ENDPOINT
	KMSTokenRef        string // TRPC_KMS_TOKEN_REF
	SecretCacheTTL     string // TRPC_SECRET_CACHE_TTL

	// GatewayRateQPS / GatewayRateBurst are the platform default for the
	// per-tenant admission token bucket (design 5.1.4); tenant.rate_policy
	// overrides per tenant.
	GatewayRateQPS   string // TRPC_GATEWAY_RATE_QPS
	GatewayRateBurst string // TRPC_GATEWAY_RATE_BURST

	// SendRateQPS / SendRateBurst pace outbound IM sends per
	// {channel, tenant} (design 5.3.2, IM proactive-send rate limits).
	SendRateQPS   string // TRPC_SEND_RATE_QPS
	SendRateBurst string // TRPC_SEND_RATE_BURST

	// SummaryEventThreshold is the number of uncovered events that triggers
	// session summarization (PG session backend only). ArchiveRetention is
	// how long session_event/audit_log rows stay in the hot tables before the
	// archive sweep moves them (design 5.1.3); ArchiveInterval is the sweep
	// cadence. Go duration syntax for the two intervals.
	SummaryEventThreshold string // TRPC_SUMMARY_EVENT_THRESHOLD
	ArchiveRetention      string // TRPC_ARCHIVE_RETENTION
	ArchiveInterval       string // TRPC_ARCHIVE_INTERVAL

	// MigrationObserve is the dual-write observation window after a migration
	// read switch (design 5.2.6, default 24h). Go duration syntax.
	MigrationObserve string // TRPC_MIGRATION_OBSERVE

	// Artifact S3 backend (MinIO / cloud OSS). Secret refs only; disabled when
	// the endpoint is unreachable at startup.
	S3Endpoint     string // TRPC_S3_ENDPOINT
	S3Bucket       string // TRPC_S3_BUCKET
	S3AccessKeyRef string // TRPC_S3_ACCESSKEY_REF
	S3SecretKeyRef string // TRPC_S3_SECRETKEY_REF
	S3Secure       string // TRPC_S3_SECURE: "true" for TLS (cloud OSS)

	// Embedder config for Knowledge (pgvector). Disabled when
	// TRPC_EMBEDDER_MODEL is unset: the default chat endpoint (DeepSeek) has
	// no embeddings API, so point these at an embeddings-capable
	// OpenAI-compatible endpoint.
	EmbedderBaseURL string // TRPC_EMBEDDER_BASE_URL
	EmbedderModel   string // TRPC_EMBEDDER_MODEL
	EmbedderKeyRef  string // TRPC_EMBEDDER_APIKEY_REF: secret ref
	EmbedderDim     string // TRPC_EMBEDDER_DIMENSION: vector size, default 1536
	KnowledgeTable  string // TRPC_KNOWLEDGE_TABLE: pgvector table, default knowledge_embeddings
}

// Load reads configuration from environment variables, filling defaults for
// unset ones. Roles that need PG/Redis (gateway/worker/admin) validate those
// fields at startup and fail fast.
func Load() Config {
	return Config{
		HTTPAddr:   getenv("TRPC_HTTP_ADDR", ":8080"),
		AdminAddr:  getenv("TRPC_ADMIN_ADDR", ":8081"),
		WorkerAddr: getenv("TRPC_WORKER_ADDR", ":8082"),
		PGDSN:      getenv("TRPC_PG_DSN", "postgres://trpc:trpc-dev-only@localhost:5432/trpc?sslmode=disable"),
		RedisAddr:  getenv("TRPC_REDIS_ADDR", "localhost:6380"), // host 6379 is often taken by other local services; compose maps 6380
		LogLevel:   getenv("TRPC_LOG_LEVEL", "info"),
		LogFormat:  getenv("TRPC_LOG_FORMAT", "console"),
		SecretsDir: getenv("TRPC_SECRETS_DIR", "data/secrets"),

		ModelBaseURL:   getenv("TRPC_MODEL_BASE_URL", "https://api.deepseek.com"),
		ModelName:      getenv("TRPC_MODEL_NAME", "deepseek-v4-flash"),
		ModelAPIKeyRef: getenv("TRPC_MODEL_APIKEY_REF", "deepseek-apikey"),
		ModelTimeout:   getenv("TRPC_MODEL_TIMEOUT", "60s"),
		ModelPrices:    getenv("TRPC_MODEL_PRICES", ""),

		SessionBackend: getenv("TRPC_SESSION_BACKEND", "redis"),
		AppName:        getenv("TRPC_APP_NAME", "00000000-0000-0000-0000-000000000101"),

		WecomCorpID:    getenv("TRPC_WECOM_CORP_ID", ""),
		WecomAgentID:   getenv("TRPC_WECOM_AGENT_ID", ""),
		WecomTokenRef:  getenv("TRPC_WECOM_TOKEN_REF", "wecom-token"),
		WecomAESKeyRef: getenv("TRPC_WECOM_AESKEY_REF", "wecom-aeskey"),
		WecomSecretRef: getenv("TRPC_WECOM_SECRET_REF", "wecom-secret"),
		WecomAPIBase:   getenv("TRPC_WECOM_API_BASE", "https://qyapi.weixin.qq.com"),

		WxkfCorpID:    getenv("TRPC_WXKF_CORP_ID", ""),
		WxkfKfAccount: getenv("TRPC_WXKF_KF_ACCOUNT", ""),
		WxkfTokenRef:  getenv("TRPC_WXKF_TOKEN_REF", "wxkf-token"),
		WxkfAESKeyRef: getenv("TRPC_WXKF_AESKEY_REF", "wxkf-aeskey"),
		WxkfSecretRef: getenv("TRPC_WXKF_SECRET_REF", "wxkf-secret"),
		WxkfAPIBase:   getenv("TRPC_WXKF_API_BASE", "https://qyapi.weixin.qq.com"),

		AdminToken: getenv("TRPC_ADMIN_TOKEN", ""),
		// Admin mTLS (split admin role): all three must be set to engage.
		AdminTLSCert:     getenv("TRPC_ADMIN_TLS_CERT", ""),
		AdminTLSKey:      getenv("TRPC_ADMIN_TLS_KEY", ""),
		AdminTLSClientCA: getenv("TRPC_ADMIN_TLS_CLIENT_CA", ""),

		SecretResolverType: getenv("TRPC_SECRET_RESOLVER", "file"),
		KMSEndpoint:        getenv("TRPC_KMS_ENDPOINT", ""),
		KMSTokenRef:        getenv("TRPC_KMS_TOKEN_REF", "kms-token"),
		SecretCacheTTL:     getenv("TRPC_SECRET_CACHE_TTL", "1m"),

		GatewayRateQPS:   getenv("TRPC_GATEWAY_RATE_QPS", "50"),
		GatewayRateBurst: getenv("TRPC_GATEWAY_RATE_BURST", "100"),
		SendRateQPS:      getenv("TRPC_SEND_RATE_QPS", "20"),
		SendRateBurst:    getenv("TRPC_SEND_RATE_BURST", "40"),

		SummaryEventThreshold: getenv("TRPC_SUMMARY_EVENT_THRESHOLD", "20"),
		ArchiveRetention:      getenv("TRPC_ARCHIVE_RETENTION", "720h"), // 30 days online (design 6.2)
		ArchiveInterval:       getenv("TRPC_ARCHIVE_INTERVAL", "24h"),
		MigrationObserve:      getenv("TRPC_MIGRATION_OBSERVE", "24h"),

		S3Endpoint:     getenv("TRPC_S3_ENDPOINT", "localhost:9000"),
		S3Bucket:       getenv("TRPC_S3_BUCKET", "artifacts"),
		S3AccessKeyRef: getenv("TRPC_S3_ACCESSKEY_REF", "s3-accesskey"),
		S3SecretKeyRef: getenv("TRPC_S3_SECRETKEY_REF", "s3-secretkey"),
		S3Secure:       getenv("TRPC_S3_SECURE", "false"),

		EmbedderBaseURL: getenv("TRPC_EMBEDDER_BASE_URL", ""),
		EmbedderModel:   getenv("TRPC_EMBEDDER_MODEL", ""),
		EmbedderKeyRef:  getenv("TRPC_EMBEDDER_APIKEY_REF", "embedder-apikey"),
		EmbedderDim:     getenv("TRPC_EMBEDDER_DIMENSION", "1536"),
		KnowledgeTable:  getenv("TRPC_KNOWLEDGE_TABLE", "knowledge_embeddings"),
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
