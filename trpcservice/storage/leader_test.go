package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// A non-positive ttl must be refused, not leased as an immortal key (and
// certainly not panic the watchdog's ticker on ttl/3).
func TestLeaderLockRejectsBadTTL(t *testing.T) {
	rdb := redisOrSkip(t)
	for _, ttl := range []time.Duration{0, -time.Second} {
		if _, _, err := NewLeaderLock(rdb).Acquire(context.Background(), "bad-ttl", "A", ttl); err == nil {
			t.Fatalf("ttl %s must be refused", ttl)
		}
	}
}

// TestLeaderLock covers the exactly-one-of semantics the wecomws leader
// hangs off: mutual exclusion while held, takeover after the lease
// disappears, owner-checked release and idempotent release.
func TestLeaderLockMutexAndTakeover(t *testing.T) {
	rdb := redisOrSkip(t)
	ctx := context.Background()
	name := fmt.Sprintf("leader-test-%d", time.Now().UnixNano())
	t.Cleanup(func() { rdb.Del(ctx, leaderKey(name)) })

	releaseA, lostA, err := NewLeaderLock(rdb).Acquire(ctx, name, "A", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// Mutex: B must not acquire while A holds the lease.
	bctx, bcancel := context.WithTimeout(ctx, 400*time.Millisecond)
	_, _, err = NewLeaderLock(rdb).Acquire(bctx, name, "B", 2*time.Second)
	bcancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("B must time out while A holds the lease, got %v", err)
	}

	// Takeover: the lease disappears (expiry or DEL) and B wins the next
	// SetNX; A's watchdog must notice and close lost.
	if err := rdb.Del(ctx, leaderKey(name)).Err(); err != nil {
		t.Fatal(err)
	}
	bctx, bcancel = context.WithTimeout(ctx, 5*time.Second)
	defer bcancel()
	releaseB, lostB, err := NewLeaderLock(rdb).Acquire(bctx, name, "B", 2*time.Second)
	if err != nil {
		t.Fatalf("B must acquire after the lease vanished: %v", err)
	}
	select {
	case <-lostA:
	case <-time.After(4 * time.Second):
		t.Fatal("A's lost channel did not close after takeover")
	}
	select {
	case <-lostB:
		t.Fatal("B's lost channel must stay open while it holds the lease")
	default:
	}

	// Release is owner-checked: A releasing must not delete B's lease, and
	// it is idempotent (double release, no panic).
	releaseA()
	releaseA()
	if n, err := rdb.Exists(ctx, leaderKey(name)).Result(); err != nil || n != 1 {
		t.Fatalf("B's lease must survive A's release (exists=%d err=%v)", n, err)
	}

	// B's release frees the key; a fresh Acquire wins immediately.
	releaseB()
	releaseB()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if n, _ := rdb.Exists(ctx, leaderKey(name)).Result(); n == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	gctx, gcancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer gcancel()
	if _, _, err := NewLeaderLock(rdb).Acquire(gctx, name, "C", 2*time.Second); err != nil {
		t.Fatalf("acquire after release must succeed immediately: %v", err)
	}
}
