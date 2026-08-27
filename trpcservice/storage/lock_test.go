package storage

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestLockMutualExclusion(t *testing.T) {
	rdb := redisOrSkip(t)
	ctx := context.Background()
	sess := fmt.Sprintf("lock-test-%d", time.Now().UnixNano())
	t.Cleanup(func() { rdb.Del(ctx, lockKey(sess)) })

	l := NewLock(rdb)

	ok, err := l.TryAcquire(ctx, sess, "w1", 10*time.Second)
	if err != nil || !ok {
		t.Fatalf("first acquire should succeed: ok=%v err=%v", ok, err)
	}
	ok, err = l.TryAcquire(ctx, sess, "w2", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("second acquire must fail while w1 holds the lock")
	}

	if err := l.Release(ctx, sess, "w1"); err != nil {
		t.Fatal(err)
	}
	ok, _ = l.TryAcquire(ctx, sess, "w2", 10*time.Second)
	if !ok {
		t.Fatal("acquire after release should succeed")
	}
}

func TestLockReleaseChecksOwner(t *testing.T) {
	rdb := redisOrSkip(t)
	ctx := context.Background()
	sess := fmt.Sprintf("lock-test-%d", time.Now().UnixNano())
	t.Cleanup(func() { rdb.Del(ctx, lockKey(sess)) })

	l := NewLock(rdb)
	if ok, _ := l.TryAcquire(ctx, sess, "w1", 10*time.Second); !ok {
		t.Fatal("acquire failed")
	}

	// A non-owner release must not delete the lock.
	if err := l.Release(ctx, sess, "intruder"); err != nil {
		t.Fatal(err)
	}
	ok, _ := l.TryAcquire(ctx, sess, "w2", 10*time.Second)
	if ok {
		t.Fatal("non-owner release must not free the lock")
	}
}

func TestLockExtend(t *testing.T) {
	rdb := redisOrSkip(t)
	ctx := context.Background()
	sess := fmt.Sprintf("lock-test-%d", time.Now().UnixNano())
	t.Cleanup(func() { rdb.Del(ctx, lockKey(sess)) })

	l := NewLock(rdb)
	if ok, _ := l.TryAcquire(ctx, sess, "w1", 500*time.Millisecond); !ok {
		t.Fatal("acquire failed")
	}

	// Extend by the owner keeps it alive past the original TTL.
	time.Sleep(300 * time.Millisecond)
	ok, err := l.Extend(ctx, sess, "w1", 800*time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("owner extend should succeed: ok=%v err=%v", ok, err)
	}
	time.Sleep(400 * time.Millisecond) // total 700ms > original 500ms TTL
	ok, _ = l.TryAcquire(ctx, sess, "w2", time.Second)
	if ok {
		t.Fatal("extended lock should still be held")
	}

	// Extend by a non-owner fails.
	if ok, _ := l.Extend(ctx, sess, "intruder", time.Second); ok {
		t.Fatal("non-owner extend must fail")
	}
}

func TestLockExpiresOnCrash(t *testing.T) {
	rdb := redisOrSkip(t)
	ctx := context.Background()
	sess := fmt.Sprintf("lock-test-%d", time.Now().UnixNano())
	t.Cleanup(func() { rdb.Del(ctx, lockKey(sess)) })

	l := NewLock(rdb)
	if ok, _ := l.TryAcquire(ctx, sess, "crashed-worker", 200*time.Millisecond); !ok {
		t.Fatal("acquire failed")
	}
	// No release (crash): the TTL alone must free the lock.
	time.Sleep(300 * time.Millisecond)
	ok, _ := l.TryAcquire(ctx, sess, "w2", time.Second)
	if !ok {
		t.Fatal("lock should expire with its TTL after a crash")
	}
}
