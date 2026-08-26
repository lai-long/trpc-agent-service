// Package web provides the Admin API and the Gateway HTTP entry points.
package web

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

// EnqueueHandler implements channels.Handler as the Gateway's inbound core
// (sync ack + async consume): a message is written to stream:inbound and
// acknowledged immediately; the reply is produced asynchronously by a Worker
// and delivered via stream:outbound.
//
// An empty OutboundMessage.Text means "accepted, reply follows
// asynchronously" (see the channels.Handler contract).
//
// Tenant routing (channel_binding lookup by webhook_path) and per-tenant
// token-bucket rate limiting belong before the enqueue.
type EnqueueHandler struct {
	Stream *storage.Stream
}

// Handle implements channels.Handler.
func (h EnqueueHandler) Handle(ctx context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
	payload, err := json.Marshal(msg)
	if err != nil {
		return channels.OutboundMessage{}, fmt.Errorf("marshal inbound message: %w", err)
	}
	if _, err := h.Stream.Add(ctx, storage.StreamInbound, payload); err != nil {
		// Return the error so the channel layer replies 5xx and the IM
		// retries later.
		return channels.OutboundMessage{}, fmt.Errorf("enqueue inbound: %w", err)
	}
	return channels.OutboundMessage{}, nil
}
