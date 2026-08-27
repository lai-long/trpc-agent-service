package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Lock is a session-scoped distributed lock (key lock:sess:{session_id}).
// It serializes concurrent processing of the same session across worker
// replicas. The (session_id, event_seq) unique constraint remains the
// last-resort backstop if the lock is ever lost.
type Lock struct {
	rdb *redis.Client
}

// NewLock creates a Lock on an established Redis client.
func NewLock(rdb *redis.Client) *Lock {
	return &Lock{rdb: rdb}
}

func lockKey(sessionID string) string { return "lock:sess:" + sessionID }

// TryAcquire attempts SET NX EX once; ok=false means another worker holds it.
func (l *Lock) TryAcquire(ctx context.Context, sessionID, owner string, ttl time.Duration) (bool, error) {
	ok, err := l.rdb.SetNX(ctx, lockKey(sessionID), owner, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("lock acquire %s: %w", sessionID, err)
	}
	return ok, nil
}

// releaseScript deletes the key only when the value still belongs to us, so a
// lock that expired and was re-acquired by someone else is never deleted.
var releaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
else
  return 0
end`)

// Release frees the lock if and only if we still own it.
func (l *Lock) Release(ctx context.Context, sessionID, owner string) error {
	if err := releaseScript.Run(ctx, l.rdb, []string{lockKey(sessionID)}, owner).Err(); err != nil {
		return fmt.Errorf("lock release %s: %w", sessionID, err)
	}
	return nil
}

// extendScript refreshes the TTL only when we still own the lock.
var extendScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
  return 0
end`)

// Extend renews the TTL; ok=false means the lock is no longer ours (expired
// or taken over) and the caller should consider the session unprotected.
func (l *Lock) Extend(ctx context.Context, sessionID, owner string, ttl time.Duration) (bool, error) {
	n, err := extendScript.Run(ctx, l.rdb, []string{lockKey(sessionID)}, owner, ttl.Milliseconds()).Int()
	if err != nil {
		return false, fmt.Errorf("lock extend %s: %w", sessionID, err)
	}
	return n == 1, nil
}
