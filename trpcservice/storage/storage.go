// Package storage provides connection and access abstractions for the
// platform's data layer.
//
// The storage layer owns the multi-backend implementations and tenant-level
// routing of the Session/Memory/Knowledge/Artifact interfaces. This file
// lands the basics first: Redis/PG connection construction with ping
// (fail fast); upper-level semantics are added incrementally.
//
// Only the cmd layer creates connections (constructed, injected and closed
// at the process entry by role); business packages receive ready-made
// clients and never know where connections come from.
package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// dialTimeout bounds the connection ping: unreachable infrastructure must
// fail fast at startup instead of hanging on the default timeout.
const dialTimeout = 5 * time.Second

// NewRedis creates a Redis client and pings it; any failure is returned as
// an error (fail fast at startup).
func NewRedis(ctx context.Context, addr string) (*redis.Client, error) {
	c := redis.NewClient(&redis.Options{Addr: addr})

	ctx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("ping redis %s: %w", addr, err)
	}
	return c, nil
}

// NewPG creates a PostgreSQL connection pool and pings it; any failure is
// returned as an error.
// dsn looks like postgres://user:pass@host:5432/dbname?sslmode=disable.
func NewPG(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("parse pg dsn: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return pool, nil
}
