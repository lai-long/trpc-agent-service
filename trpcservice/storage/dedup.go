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

// Deduper implements inbound idempotency: SET dedup:{channel}:{binding}:{msg_id}
// NX EX 24h. The key is shared across replicas, so IM redeliveries are dropped
// no matter which gateway instance receives them. The binding dimension keeps
// two tenants' callbacks apart: msg_id uniqueness is guaranteed by the IM per
// corp/app only, so the same numeric ID can legitimately arrive on two
// bindings of one channel (design 5.1.4).
type Deduper struct {
	rdb *redis.Client
}

// NewDeduper creates a Deduper on an established Redis client.
func NewDeduper(rdb *redis.Client) *Deduper {
	return &Deduper{rdb: rdb}
}

// Check reports whether the message arrives for the first time. SETNX is
// atomic, so concurrent duplicates of the same msg_id race safely.
func (d *Deduper) Check(ctx context.Context, channel, binding, msgID string) (bool, error) {
	key := dedupKey(channel, binding, msgID)
	ok, err := d.rdb.SetNX(ctx, key, 1, DedupTTL).Result()
	if err != nil {
		return false, fmt.Errorf("dedup %s: %w", key, err)
	}
	return ok, nil
}

// Forget clears the key set by a successful Check, reopening the message for
// redelivery.
//
// It is the undo path for failures after Check (e.g. a failed enqueue): the
// dedup key only exists to absorb the IM's own redeliveries, and those
// redeliveries are exactly our retry mechanism (design 5.2.2). Leaving the
// key behind would drop every retry for the whole 24h TTL — the message is
// silently lost instead of delayed.
func (d *Deduper) Forget(ctx context.Context, channel, binding, msgID string) error {
	key := dedupKey(channel, binding, msgID)
	if err := d.rdb.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("dedup forget %s: %w", key, err)
	}
	return nil
}

func dedupKey(channel, binding, msgID string) string {
	return fmt.Sprintf("dedup:%s:%s:%s", channel, binding, msgID)
}
