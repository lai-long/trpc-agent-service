package config

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadDefaults(t *testing.T) {
	for _, k := range []string{"TRPC_HTTP_ADDR", "TRPC_PG_DSN", "TRPC_REDIS_ADDR",
		"TRPC_LOG_LEVEL", "TRPC_LOG_FORMAT", "TRPC_SECRETS_DIR"} {
		os.Unsetenv(k)
	}

	cfg := Load()
	if cfg.HTTPAddr != ":8080" {
		t.Errorf("HTTPAddr default = %q, want :8080", cfg.HTTPAddr)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel default = %q, want info", cfg.LogLevel)
	}
	if cfg.RedisAddr != "localhost:6380" {
		t.Errorf("RedisAddr default = %q, want localhost:6380", cfg.RedisAddr)
	}
}

func TestLoadFromEnv(t *testing.T) {
	t.Setenv("TRPC_HTTP_ADDR", ":9090")
	t.Setenv("TRPC_LOG_FORMAT", "json")

	cfg := Load()
	if cfg.HTTPAddr != ":9090" {
		t.Errorf("HTTPAddr = %q, want :9090", cfg.HTTPAddr)
	}
	if cfg.LogFormat != "json" {
		t.Errorf("LogFormat = %q, want json", cfg.LogFormat)
	}
}

func TestMustEnv(t *testing.T) {
	t.Setenv("TRPC_TEST_REQUIRED", "v")
	if _, err := MustEnv("TRPC_TEST_REQUIRED"); err != nil {
		t.Errorf("MustEnv on set var should succeed: %v", err)
	}
	if _, err := MustEnv("TRPC_TEST_MISSING"); err == nil {
		t.Error("MustEnv on missing var should fail")
	}
}

func TestFileResolver(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "wecom-token"), []byte("  s3cret\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	r := NewFileResolver(dir)
	got, err := r.Resolve(context.Background(), "wecom-token")
	if err != nil {
		t.Fatal(err)
	}
	if got != "s3cret" {
		t.Errorf("Resolve = %q, want trimmed content", got)
	}

	if _, err := r.Resolve(context.Background(), "../etc/passwd"); err == nil {
		t.Error("path traversal ref should be rejected")
	}
	if _, err := r.Resolve(context.Background(), "not-exists"); err == nil {
		t.Error("missing file should return error")
	}
}
