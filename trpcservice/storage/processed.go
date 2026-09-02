package storage

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// ProcessedMarker is the execution-layer idempotency of design 5.1.4: the
// worker marks a message done:{channel}:{msg_id} (24h) after its reply is
// enqueued. A Stream redelivery (reaper takeover after a crash, or a delayed
// Ack) finds the marker and skips reprocessing — without it a redelivered
// message would run the LLM again, journal duplicate events, and double the
// token spend. The (session_id, event_seq) unique constraint remains the
// last-resort backstop below this.
//
// Note the boundary: a crash DURING processing (before the mark) still
// re-runs — that window is inherent to at-least-once delivery; the outbound
// sent: key stops the duplicate reply from reaching the user.
type ProcessedMarker struct {
	rdb *redis.Client
}

// NewProcessedMarker creates the marker on an established Redis client.
func NewProcessedMarker(rdb *redis.Client) *ProcessedMarker {
	return &ProcessedMarker{rdb: rdb}
}

// IsDone reports whether the message was already fully processed.
func (m *ProcessedMarker) IsDone(ctx context.Context, channel, msgID string) (bool, error) {
	n, err := m.rdb.Exists(ctx, doneKey(channel, msgID)).Result()
	if err != nil {
		return false, fmt.Errorf("done check: %w", err)
	}
	return n > 0, nil
}

// MarkDone records the message as fully processed (24h TTL, matching the
// inbound dedup window).
func (m *ProcessedMarker) MarkDone(ctx context.Context, channel, msgID string) error {
	key := doneKey(channel, msgID)
	if err := m.rdb.Set(ctx, key, "1", DedupTTL).Err(); err != nil {
		return fmt.Errorf("done mark: %w", err)
	}
	return nil
}

func doneKey(channel, msgID string) string {
	return fmt.Sprintf("done:%s:%s", channel, msgID)
}
