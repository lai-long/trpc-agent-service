package storage

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// SentMarker implements outbound idempotency (key sent:{channel}:{msg_id},
// TTL DedupTTL): the sender checks before calling the IM API and marks after
// a successful send, so a redelivery after "sent but not acked" cannot push
// the same reply to the user twice.
type SentMarker struct {
	rdb *redis.Client
}

// NewSentMarker creates a SentMarker on an established Redis client.
func NewSentMarker(rdb *redis.Client) *SentMarker {
	return &SentMarker{rdb: rdb}
}

func sentKey(channel, msgID string) string {
	return fmt.Sprintf("sent:%s:%s", channel, msgID)
}

// IsSent reports whether the reply to this message was already delivered.
func (m *SentMarker) IsSent(ctx context.Context, channel, msgID string) (bool, error) {
	n, err := m.rdb.Exists(ctx, sentKey(channel, msgID)).Result()
	if err != nil {
		return false, fmt.Errorf("check sent %s:%s: %w", channel, msgID, err)
	}
	return n > 0, nil
}

// MarkSent records the reply as delivered; imMsgID is the message ID returned
// by the IM platform (empty for channels without one).
func (m *SentMarker) MarkSent(ctx context.Context, channel, msgID, imMsgID string) error {
	if err := m.rdb.Set(ctx, sentKey(channel, msgID), imMsgID, DedupTTL).Err(); err != nil {
		return fmt.Errorf("mark sent %s:%s: %w", channel, msgID, err)
	}
	return nil
}
