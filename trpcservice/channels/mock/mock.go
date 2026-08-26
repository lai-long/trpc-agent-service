// Package mock provides a Mock Channel that depends on no real IM platform.
//
// It exercises the full pipeline (callback → normalization → handling →
// reply) without external IM accounts; a real adapter implements the same
// channels.Channel interface and slots in.
package mock

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
)

// callbackRequest is the callback payload pushed by the simulated IM platform.
type callbackRequest struct {
	MsgID  string `json:"msg_id"`  // message ID, used for idempotency
	UserID string `json:"user_id"` // sender
	ChatID string `json:"chat_id"` // group ID; empty means a direct chat
	Text   string `json:"text"`
}

// Channel is the mock implementation of channels.Channel.
type Channel struct {
	mu   sync.Mutex
	seen map[string]struct{}        // in-memory dedup set; the real implementation uses Redis
	sent []channels.OutboundMessage // inbox of sent replies, for inspection in tests
}

// New creates a mock Channel.
func New() *Channel {
	return &Channel{seen: make(map[string]struct{})}
}

// Name implements channels.Channel.
func (c *Channel) Name() string { return "mock" }

// RegisterRoutes implements channels.Channel.
// POST /mock/callback simulates the IM webhook callback.
func (c *Channel) RegisterRoutes(mux *http.ServeMux, h channels.Handler) {
	mux.HandleFunc("POST /mock/callback", func(w http.ResponseWriter, r *http.Request) {
		// 1. Decode the IM callback payload (a real implementation verifies
		//    the signature and decrypts before this step).
		var req callbackRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
			return
		}
		if req.MsgID == "" || req.UserID == "" {
			http.Error(w, "msg_id and user_id are required", http.StatusBadRequest)
			return
		}

		// 2. Idempotent dedup, mirroring SETNX dedup:{channel}:{msg_id}.
		if !c.markSeen(req.MsgID) {
			zap.L().Warn("duplicate message dropped",
				zap.String(plog.FieldChannel, c.Name()), zap.String(plog.FieldMsgID, req.MsgID))
			// Duplicates still get a success reply so the IM stops retrying.
			writeReply(w, "duplicate", "")
			return
		}

		// 3. Normalize the external payload into the platform-wide InboundMessage.
		msg := channels.InboundMessage{
			Channel:    c.Name(),
			MsgID:      req.MsgID,
			SessionKey: channels.SessionKey(c.Name(), req.UserID, req.ChatID),
			UserID:     req.UserID,
			ChatID:     req.ChatID,
			Text:       req.Text,
			TraceID:    req.MsgID, // the minimal version reuses msg_id as trace_id; the real one uses OTel
			ReceivedAt: time.Now(),
		}

		// 4. Hand over to the upper layer (formal chain: gateway enqueues to
		//    stream:inbound and returns an empty "accepted" reply; sync/debug
		//    path: the handler returns the reply inline).
		out, err := h.Handle(r.Context(), msg)
		if err != nil {
			http.Error(w, "handle error: "+err.Error(), http.StatusInternalServerError)
			return
		}

		// Empty reply = accepted, the reply is delivered asynchronously
		// (see the Handler contract: sync ack + async consume).
		if out.Text == "" {
			writeReply(w, "accepted", "")
			return
		}

		// 5. Sync reply path (debug): record it in the in-memory inbox.
		if err := c.Send(r.Context(), out); err != nil {
			http.Error(w, "send error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		writeReply(w, "ok", out.Text)
	})
}

// Send implements channels.Channel. The mock calls no IM API; it only records
// the reply in memory for tests and debugging.
func (c *Channel) Send(_ context.Context, msg channels.OutboundMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sent = append(c.sent, msg)
	zap.L().Info("reply sent",
		zap.String(plog.FieldChannel, msg.Channel),
		zap.String(plog.FieldSessionKey, msg.SessionKey),
		zap.String(plog.FieldTraceID, msg.TraceID),
		zap.String("text", msg.Text))
	return nil
}

// Sent returns all recorded replies, for test assertions.
func (c *Channel) Sent() []channels.OutboundMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]channels.OutboundMessage(nil), c.sent...)
}

// markSeen reports whether msgID arrives for the first time.
func (c *Channel) markSeen(msgID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.seen[msgID]; ok {
		return false
	}
	c.seen[msgID] = struct{}{}
	return true
}

func writeReply(w http.ResponseWriter, status, text string) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": status, "reply": text})
}
