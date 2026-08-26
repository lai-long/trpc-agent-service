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
}

// Load reads configuration from environment variables, filling defaults for
// unset ones. All items are currently optional (the mock pipeline needs
// neither PG nor Redis); worker/admin roles should validate PGDSN/RedisAddr
// themselves at startup and fail fast.
func Load() Config {
	return Config{
		HTTPAddr:   getenv("TRPC_HTTP_ADDR", ":8080"),
		PGDSN:      getenv("TRPC_PG_DSN", ""),
		RedisAddr:  getenv("TRPC_REDIS_ADDR", "localhost:6380"), // host 6379 is often taken by other local services; compose maps 6380
		LogLevel:   getenv("TRPC_LOG_LEVEL", "info"),
		LogFormat:  getenv("TRPC_LOG_FORMAT", "console"),
		SecretsDir: getenv("TRPC_SECRETS_DIR", "data/secrets"),
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
