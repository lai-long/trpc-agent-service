// Package testenv gates the integration tests' infrastructure dependencies
// through environment variables (review P0-4): CI services and dev shells set
// them explicitly, so every skip message names the variable that enables the
// test, and a CI run with skips > 0 fails the build instead of silently
// shrugging past the platform's core paths.
package testenv

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// RedisAddr is the test Redis address (TRPC_TEST_REDIS_ADDR); the default
// matches docker-compose.yml's mapped port.
func RedisAddr() string { return getenv("TRPC_TEST_REDIS_ADDR", "localhost:6380") }

// PGDSN is the test PostgreSQL DSN (TRPC_TEST_PG_DSN); the default matches
// the compose stack's dev credentials.
func PGDSN() string {
	return getenv("TRPC_TEST_PG_DSN", "postgres://trpc:trpc-dev-only@localhost:5432/trpc?sslmode=disable")
}

// S3Endpoint is the test MinIO endpoint (TRPC_TEST_S3_ENDPOINT).
func S3Endpoint() string { return getenv("TRPC_TEST_S3_ENDPOINT", "localhost:9000") }

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// Redis connects for an integration test and registers cleanup; it skips with
// the enabling variable named when the server is unreachable.
func Redis(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: RedisAddr()})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		t.Skipf("redis unavailable (%v) — set TRPC_TEST_REDIS_ADDR (default %s), skipping integration test", err, RedisAddr())
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// PG connects for an integration test and registers cleanup; it skips with
// the enabling variable named when the server is unreachable.
func PG(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, PGDSN())
	if err == nil {
		err = pool.Ping(ctx)
	}
	if err != nil {
		if pool != nil {
			pool.Close()
		}
		t.Skipf("postgres unavailable (%v) — set TRPC_TEST_PG_DSN (default %s), skipping integration test", err, PGDSN())
	}
	t.Cleanup(pool.Close)
	return pool
}
