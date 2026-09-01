package storage

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestLimiterBurstThenDeny(t *testing.T) {
	rdb := redisOrSkip(t)
	ctx := context.Background()
	scope := fmt.Sprintf("tenant:test-%d", time.Now().UnixNano())
	t.Cleanup(func() { rdb.Del(ctx, "ratelimit:"+scope) })

	l := NewLimiter(rdb)
	// burst=2: two immediate passes, the third is denied.
	for i := 0; i < 2; i++ {
		ok, err := l.Allow(ctx, scope, 1, 2)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("attempt %d within burst must pass", i+1)
		}
	}
	ok, err := l.Allow(ctx, scope, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("burst exhausted: third attempt must be denied")
	}
}

func TestLimiterRefills(t *testing.T) {
	rdb := redisOrSkip(t)
	ctx := context.Background()
	scope := fmt.Sprintf("tenant:test-%d", time.Now().UnixNano())
	t.Cleanup(func() { rdb.Del(ctx, "ratelimit:"+scope) })

	l := NewLimiter(rdb)
	if ok, _ := l.Allow(ctx, scope, 50, 1); !ok {
		t.Fatal("first token must pass")
	}
	if ok, _ := l.Allow(ctx, scope, 50, 1); ok {
		t.Fatal("bucket of 1 must be empty after the first token")
	}
	// 50 tokens/s → one token every 20ms; after 100ms the bucket has refilled.
	time.Sleep(100 * time.Millisecond)
	if ok, _ := l.Allow(ctx, scope, 50, 1); !ok {
		t.Fatal("bucket must refill over time")
	}
}

func TestLimiterWaitAllow(t *testing.T) {
	rdb := redisOrSkip(t)
	ctx := context.Background()
	scope := fmt.Sprintf("tenant:test-%d", time.Now().UnixNano())
	t.Cleanup(func() { rdb.Del(ctx, "ratelimit:"+scope) })

	l := NewLimiter(rdb)
	if ok, _ := l.Allow(ctx, scope, 50, 1); !ok {
		t.Fatal("first token must pass")
	}
	// A short wait rides out the refill; a tiny wait times out.
	if ok, err := l.WaitAllow(ctx, scope, 50, 1, 2*time.Second); err != nil || !ok {
		t.Fatalf("wait through refill must succeed: ok=%v err=%v", ok, err)
	}
	if ok, err := l.WaitAllow(ctx, scope, 0.001, 1, 200*time.Millisecond); err != nil || ok {
		t.Fatalf("near-zero rate must time out the wait: ok=%v err=%v", ok, err)
	}
}

func TestLimiterNonPositiveDisables(t *testing.T) {
	l := NewLimiter(nil) // no Redis: the disabled path must not touch it
	ok, err := l.Allow(context.Background(), "tenant:x", 0, 0)
	if err != nil || !ok {
		t.Fatalf("non-positive config must disable the limit: ok=%v err=%v", ok, err)
	}
}
