package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// DedupTTL covers the IM redelivery window: retries after a failed ack can
// arrive minutes apart, 24h covers all of them.
const DedupTTL = 24 * time.Hour

// Deduper implements inbound idempotency: SET dedup:{channel}:{msg_id} NX EX
// 24h. The key is shared across replicas, so IM redeliveries are dropped no
// matter which gateway instance receives them.
type Deduper struct {
	rdb *redis.Client
}

// NewDeduper creates a Deduper on an established Redis client.
func NewDeduper(rdb *redis.Client) *Deduper {
	return &Deduper{rdb: rdb}
}

// Check reports whether the message arrives for the first time. SETNX is
// atomic, so concurrent duplicates of the same msg_id race safely.
func (d *Deduper) Check(ctx context.Context, channel, msgID string) (bool, error) {
	key := fmt.Sprintf("dedup:%s:%s", channel, msgID)
	ok, err := d.rdb.SetNX(ctx, key, 1, DedupTTL).Result()
	if err != nil {
		return false, fmt.Errorf("dedup %s: %w", key, err)
	}
	return ok, nil
}
