package agent

import (
	"context"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/testenv"
)

// Unit tests for the worker's session-lock plumbing: acquireSession's spin,
// timeout, cancellation and error paths, and the lock-renewal watchdog.

// A lock held by another owner for longer than LockWait leaves the message
// pending: the reaper takes it over once the session goes quiet.
func TestAcquireSessionLeavesPendingOnTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := testenv.Redis(t)
	lock := storage.NewLock(rdb)
	appID, sessKey := "unit-app", "sess-timeout-"+t.Name()
	lockKey := "lock:sess:" + appID + ":" + sessKey
	t.Cleanup(func() { rdb.Del(context.Background(), lockKey) })
	if ok, err := lock.TryAcquire(ctx, appID, sessKey, "other-owner", 30*time.Second); err != nil || !ok {
		t.Fatalf("pre-lock failed: ok=%v err=%v", ok, err)
	}

	w := &Worker{Name: "unit-w", Lock: lock, LockTTL: time.Second, LockWait: 50 * time.Millisecond}
	owner, stop, ok := w.acquireSession(ctx, storage.Message{ID: "m-1"}, appID, sessKey)
	if ok {
		t.Fatal("acquireSession must not succeed while another owner holds the lock")
	}
	if owner != "" || stop != nil {
		t.Fatalf("timeout must return zero values, got owner=%q stop!=nil=%t", owner, stop != nil)
	}
}

// A context cancellation during the spin aborts the wait immediately.
func TestAcquireSessionStopsOnContextCancel(t *testing.T) {
	rdb := testenv.Redis(t)
	lock := storage.NewLock(rdb)
	appID, sessKey := "unit-app", "sess-cancel-"+t.Name()
	lockKey := "lock:sess:" + appID + ":" + sessKey
	t.Cleanup(func() { rdb.Del(context.Background(), lockKey) })
	if ok, err := lock.TryAcquire(context.Background(), appID, sessKey, "other-owner", 30*time.Second); err != nil || !ok {
		t.Fatalf("pre-lock failed: ok=%v err=%v", ok, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := &Worker{Name: "unit-w", Lock: lock, LockTTL: time.Second, LockWait: 30 * time.Second}
	type result struct {
		owner string
		stop  func()
		ok    bool
	}
	done := make(chan result, 1)
	go func() {
		owner, stop, ok := w.acquireSession(ctx, storage.Message{ID: "m-2"}, appID, sessKey)
		done <- result{owner, stop, ok}
	}()
	time.Sleep(100 * time.Millisecond) // let the spin reach its select
	cancel()

	select {
	case r := <-done:
		if r.ok || r.owner != "" || r.stop != nil {
			t.Fatalf("cancel must return zero values, got ok=%v owner=%q stop!=nil=%t", r.ok, r.owner, r.stop != nil)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("acquireSession did not return after context cancel")
	}
}

// A Redis error on TryAcquire is logged and retried until LockWait expires,
// not treated as a fatal failure.
func TestAcquireSessionWarnsOnLockError(t *testing.T) {
	rdb := testenv.Redis(t)
	_ = rdb.Close() // every lock operation now fails
	lock := storage.NewLock(rdb)

	w := &Worker{Name: "unit-w", Lock: lock, LockTTL: time.Second, LockWait: 50 * time.Millisecond}
	owner, stop, ok := w.acquireSession(context.Background(), storage.Message{ID: "m-3"}, "unit-app", "sess-closed")
	if ok || owner != "" || stop != nil {
		t.Fatalf("closed Redis must fail the acquire, got ok=%v owner=%q stop!=nil=%t", ok, owner, stop != nil)
	}
}

// On a free lock the acquire succeeds and returns the owner token plus a
// working watchdog stop; the lock is releasable afterwards.
func TestAcquireSessionSuccessStartsWatchdog(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := testenv.Redis(t)
	lock := storage.NewLock(rdb)
	appID, sessKey := "unit-app", "sess-ok-"+t.Name()
	lockKey := "lock:sess:" + appID + ":" + sessKey
	t.Cleanup(func() { rdb.Del(context.Background(), lockKey) })

	w := &Worker{Name: "unit-w", Lock: lock, LockTTL: time.Second, LockWait: time.Second}
	owner, stop, ok := w.acquireSession(ctx, storage.Message{ID: "m-4"}, appID, sessKey)
	if !ok {
		t.Fatal("acquireSession must succeed on a free lock")
	}
	if owner != "unit-w:m-4" || stop == nil {
		t.Fatalf("unexpected result: owner=%q", owner)
	}
	stop()
	if err := lock.Release(context.Background(), appID, sessKey, owner); err != nil {
		t.Fatalf("release after acquire: %v", err)
	}
}

// The watchdog renews the lease every TTL/3: with a lease that would
// otherwise expire mid-test, the key must still be ours afterwards.
func TestLockWatchdogRenewsLease(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := testenv.Redis(t)
	lock := storage.NewLock(rdb)
	appID, sessKey := "unit-app", "sess-renew-"+t.Name()
	lockKey := "lock:sess:" + appID + ":" + sessKey
	t.Cleanup(func() { rdb.Del(context.Background(), lockKey) })
	if ok, err := lock.TryAcquire(ctx, appID, sessKey, "unit-owner", 400*time.Millisecond); err != nil || !ok {
		t.Fatalf("pre-lock failed: ok=%v err=%v", ok, err)
	}

	w := &Worker{Name: "unit-w", Lock: lock, LockTTL: 400 * time.Millisecond}
	stop := w.startLockWatchdog(ctx, appID, sessKey, "unit-owner")
	defer stop()

	// Without renewal the 400ms lease would be gone long before this point.
	time.Sleep(600 * time.Millisecond)
	got, err := rdb.Get(context.Background(), lockKey).Result()
	if err != nil {
		t.Fatalf("lock key vanished despite watchdog renewal: %v", err)
	}
	if got != "unit-owner" {
		t.Fatalf("lock owner changed unexpectedly: %q", got)
	}
}

// When the lock is lost (expired and taken over by another owner), the
// watchdog stops renewing and leaves the new owner untouched.
func TestLockWatchdogStopsOnLostLock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := testenv.Redis(t)
	lock := storage.NewLock(rdb)
	appID, sessKey := "unit-app", "sess-lost-"+t.Name()
	lockKey := "lock:sess:" + appID + ":" + sessKey
	t.Cleanup(func() { rdb.Del(context.Background(), lockKey) })
	if ok, err := lock.TryAcquire(ctx, appID, sessKey, "unit-owner", 400*time.Millisecond); err != nil || !ok {
		t.Fatalf("pre-lock failed: ok=%v err=%v", ok, err)
	}

	w := &Worker{Name: "unit-w", Lock: lock, LockTTL: 400 * time.Millisecond}
	stop := w.startLockWatchdog(ctx, appID, sessKey, "unit-owner")
	defer stop()

	// Simulate a takeover: the key is stolen while the watchdog runs.
	if err := rdb.Del(ctx, lockKey).Err(); err != nil {
		t.Fatal(err)
	}
	if ok, err := lock.TryAcquire(ctx, appID, sessKey, "thief", 30*time.Second); err != nil || !ok {
		t.Fatalf("takeover failed: ok=%v err=%v", ok, err)
	}
	time.Sleep(200 * time.Millisecond) // at least one renewal tick

	got, err := rdb.Get(ctx, lockKey).Result()
	if err != nil || got != "thief" {
		t.Fatalf("watchdog must not disturb the new owner, got %q err=%v", got, err)
	}
}

// A Redis hiccup during renewal is logged and retried on the next tick; the
// watchdog exits through the context path, not by wedging or panicking.
func TestLockWatchdogToleratesRedisErrors(t *testing.T) {
	rdb := testenv.Redis(t)
	_ = rdb.Close() // every Extend now fails
	lock := storage.NewLock(rdb)

	w := &Worker{Name: "unit-w", Lock: lock, LockTTL: 100 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	stop := w.startLockWatchdog(ctx, "unit-app", "sess-errors", "unit-owner")

	time.Sleep(150 * time.Millisecond) // several failing renewal ticks
	cancel()
	stop()
}
