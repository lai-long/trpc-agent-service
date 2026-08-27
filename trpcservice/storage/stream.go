package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Names of the two Stream queues.
const (
	// StreamInbound is the Gateway→Worker inbound queue (consumer group workers).
	StreamInbound = "stream:inbound"
	// StreamOutbound is the Worker→Channel Adapter outbound queue (consumer group senders).
	StreamOutbound = "stream:outbound"
	// StreamDeadletter receives messages that exhausted redelivery attempts.
	StreamDeadletter = "stream:deadletter"

	// streamMaxLen caps the queue length (XADD MAXLEN ~) so a backlog cannot
	// exhaust Redis memory; crossing the threshold should trigger an alert.
	streamMaxLen = 100000

	// payloadField is the field name of the payload inside a Stream entry.
	payloadField = "payload"
)

// Message is a message consumed from a Stream.
type Message struct {
	ID      string // Stream entry ID, used for Ack
	Payload []byte // payload (JSON by convention; encoding is the caller's job)
}

// Stream wraps Redis Stream send/receive: capped enqueue, consumer-group
// blocking reads, and Ack after processing. XCLAIM takeover of pending
// messages after a worker crash is implemented at the worker layer, not here.
type Stream struct {
	rdb *redis.Client
}

// NewStream creates a Stream transceiver on an established Redis client.
func NewStream(rdb *redis.Client) *Stream {
	return &Stream{rdb: rdb}
}

// Add enqueues one message and returns its entry ID.
func (s *Stream) Add(ctx context.Context, stream string, payload []byte) (string, error) {
	id, err := s.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		MaxLen: streamMaxLen,
		Approx: true, // approximate trimming by node avoids per-entry exact trimming cost
		Values: map[string]any{payloadField: payload},
	}).Result()
	if err != nil {
		return "", fmt.Errorf("xadd %s: %w", stream, err)
	}
	return id, nil
}

// EnsureGroup creates the consumer group if missing; an existing group is
// skipped idempotently. MkStream also creates the stream, so a group may
// exist before the first message.
func (s *Stream) EnsureGroup(ctx context.Context, stream, group string) error {
	err := s.rdb.XGroupCreateMkStream(ctx, stream, group, "$").Err()
	if err != nil && !errors.Is(err, redis.Nil) && !isBusyGroup(err) {
		return fmt.Errorf("xgroup create %s %s: %w", stream, group, err)
	}
	return nil
}

// Read blocks for messages as a consumer-group member. A block timeout with
// no new messages returns (nil, nil) — normal idling, not an error.
func (s *Stream) Read(ctx context.Context, stream, group, consumer string, count int64, block time.Duration) ([]Message, error) {
	res, err := s.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{stream, ">"},
		Count:    count,
		Block:    block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil // idle: block timed out with no new messages
	}
	if err != nil {
		return nil, fmt.Errorf("xreadgroup %s %s: %w", stream, group, err)
	}

	var msgs []Message
	for _, sm := range res {
		for _, xm := range sm.Messages {
			payload, _ := xm.Values[payloadField].(string)
			msgs = append(msgs, Message{ID: xm.ID, Payload: []byte(payload)})
		}
	}
	return msgs, nil
}

// Ack confirms processed messages; un-acked ones stay pending, available for
// XCLAIM takeover and redelivery after a crash.
func (s *Stream) Ack(ctx context.Context, stream, group string, ids ...string) error {
	if err := s.rdb.XAck(ctx, stream, group, ids...).Err(); err != nil {
		return fmt.Errorf("xack %s %s: %w", stream, group, err)
	}
	return nil
}

// AutoClaim transfers ownership of pending messages idle for longer than
// minIdle to consumer, and returns them for reprocessing. This is how a
// surviving node takes over the messages of a crashed consumer.
func (s *Stream) AutoClaim(ctx context.Context, stream, group, consumer string, minIdle time.Duration, count int64) ([]Message, error) {
	var msgs []Message
	start := "0"
	for {
		xms, next, err := s.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   stream,
			Group:    group,
			Consumer: consumer,
			MinIdle:  minIdle,
			Start:    start,
			Count:    count,
		}).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return msgs, fmt.Errorf("xautoclaim %s %s: %w", stream, group, err)
		}
		for _, xm := range xms {
			payload, _ := xm.Values[payloadField].(string)
			msgs = append(msgs, Message{ID: xm.ID, Payload: []byte(payload)})
		}
		if next == "0" || len(xms) == 0 {
			return msgs, nil
		}
		start = next
	}
}

// Attempts increments the redelivery counter of a message. The Worker uses it
// to cut off poison messages that keep failing after every takeover.
func (s *Stream) Attempts(ctx context.Context, stream, id string) (int64, error) {
	key := fmt.Sprintf("retry:%s:%s", stream, id)
	n, err := s.rdb.Incr(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("incr %s: %w", key, err)
	}
	if n == 1 {
		// Bound the counter's lifetime; the message itself resolves or dies first.
		s.rdb.Expire(ctx, key, DedupTTL)
	}
	return n, nil
}

// DeadLetter moves a message to stream:deadletter (kept for manual
// intervention and alerting) and acks it in the origin group.
func (s *Stream) DeadLetter(ctx context.Context, stream, group string, m Message) error {
	payload, _ := json.Marshal(map[string]string{
		"origin_stream": stream,
		"origin_id":     m.ID,
		"payload":       string(m.Payload),
	})
	if _, err := s.Add(ctx, StreamDeadletter, payload); err != nil {
		return fmt.Errorf("deadletter %s: %w", m.ID, err)
	}
	return s.Ack(ctx, stream, group, m.ID)
}

func isBusyGroup(err error) bool {
	return err != nil && len(err.Error()) >= 9 && err.Error()[:9] == "BUSYGROUP"
}
