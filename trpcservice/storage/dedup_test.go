package storage

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestDeduper(t *testing.T) {
	rdb := redisOrSkip(t)
	ctx := context.Background()
	msgID := fmt.Sprintf("dedup-test-%d", time.Now().UnixNano())
	key := fmt.Sprintf("dedup:mock:%s", msgID)
	t.Cleanup(func() { rdb.Del(ctx, key) })

	d := NewDeduper(rdb)

	first, err := d.Check(ctx, "mock", msgID)
	if err != nil {
		t.Fatal(err)
	}
	if !first {
		t.Error("first arrival should pass")
	}

	// Concurrent-style duplicates: same msg_id must be rejected.
	for i := 0; i < 3; i++ {
		first, err := d.Check(ctx, "mock", msgID)
		if err != nil {
			t.Fatal(err)
		}
		if first {
			t.Fatalf("duplicate %d should be rejected", i)
		}
	}

	// The key carries the TTL covering the IM redelivery window.
	ttl, err := rdb.TTL(ctx, key).Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl <= 0 || ttl > DedupTTL {
		t.Errorf("ttl = %v, want (0, %v]", ttl, DedupTTL)
	}

	// A different channel namespace must not collide.
	other, err := d.Check(ctx, "wecom", msgID)
	if err != nil {
		t.Fatal(err)
	}
	if !other {
		t.Error("same msg_id on another channel should pass")
	}
	rdb.Del(ctx, fmt.Sprintf("dedup:wecom:%s", msgID))
}

// Forget reopens a message for redelivery. This is what keeps a failed
// delivery retryable: without it, a failure after Check (a failed enqueue)
// leaves the key behind and the IM's redelivery is dropped as a duplicate for
// the whole TTL.
func TestDeduperForget(t *testing.T) {
	rdb := redisOrSkip(t)
	ctx := context.Background()
	msgID := fmt.Sprintf("dedup-forget-%d", time.Now().UnixNano())
	key := fmt.Sprintf("dedup:mock:%s", msgID)
	t.Cleanup(func() { rdb.Del(ctx, key) })

	d := NewDeduper(rdb)
	if first, err := d.Check(ctx, "mock", msgID); err != nil || !first {
		t.Fatalf("first arrival should pass, got first=%v err=%v", first, err)
	}
	if err := d.Forget(ctx, "mock", msgID); err != nil {
		t.Fatal(err)
	}

	first, err := d.Check(ctx, "mock", msgID)
	if err != nil {
		t.Fatal(err)
	}
	if !first {
		t.Fatal("message must pass again after Forget")
	}
	// Forgetting only touches its own key namespace.
	if n, err := rdb.Exists(ctx, "dedup:wecom:"+msgID).Result(); err != nil || n != 0 {
		t.Fatalf("other channel key must be untouched, n=%d err=%v", n, err)
	}
}
