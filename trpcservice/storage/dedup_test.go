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
