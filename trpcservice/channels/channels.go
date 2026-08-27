// Package channels adapts IM platforms (WeCom, WeChat, Telegram, etc.)
// into tRPC-Agent-Go Runner inputs, following the OpenClaw Channel model.
//
// A Channel Adapter owns signature verification, encryption/decryption,
// message encoding/decoding, and the IM proactive-send API. This file
// defines the minimal IM-agnostic abstraction; each channel (mock / wecom /
// ...) implements the Channel interface.
package channels

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"
)

// InboundMessage is a normalized inbound message: the Channel builds it from
// the platform-specific callback payload and hands it to the Handler.
type InboundMessage struct {
	Channel     string    // channel type: mock / wecom / wechat_kf ...
	MsgID       string    // IM platform message ID, for idempotent dedup (dedup:{channel}:{msg_id})
	SessionKey  string    // unique conversation key within an app, see SessionKey()
	UserID      string    // user ID on the IM side (wecom external_userid / wechat openid)
	ChatID      string    // group chat ID; empty for direct chats
	Text        string    // text content
	TraceID     string    // trace ID, spanning callback → Worker → reply
	TraceParent string    // W3C traceparent, carries the trace across the Stream boundary
	ReceivedAt  time.Time // when the callback arrived
}

// OutboundMessage is a normalized outbound reply, produced by the Handler and
// delivered through the Channel's Send.
type OutboundMessage struct {
	Channel     string // which channel to reply on
	MsgID       string // the inbound message this replies to (outbound idempotency unit sent:{channel}:{msg_id})
	SessionKey  string // conversation the reply belongs to
	UserID      string // recipient (required for direct chats)
	ChatID      string // recipient group (required for group chats)
	Text        string
	TraceID     string
	TraceParent string // W3C traceparent, carried through to the outbound span
}

// SessionKey builds the unique conversation key:
//   - direct chat: dm:{channel}:{user_id} — a user reuses one session across days
//   - group chat: group:{channel}:{chat_id} — the bot's context is shared per group
//
// Uniqueness is ultimately enforced by (app_id, session_key); since an app
// belongs to a tenant, isolation across tenants comes for free.
func SessionKey(channel, userID, chatID string) string {
	if chatID != "" {
		return fmt.Sprintf("group:%s:%s", channel, chatID)
	}
	return fmt.Sprintf("dm:%s:%s", channel, userID)
}

// Handler processes one normalized inbound message.
//
// Reply contract: a non-empty OutboundMessage.Text is a synchronous reply
// (the local debug path); an empty one means accepted, with the reply sent
// asynchronously (the formal chain: the Gateway writes to stream:inbound
// and the reply is delivered via stream:outbound).
//
// Error contract: ErrDuplicate means the message was seen before; the channel
// layer answers 200 so the IM stops redelivering.
type Handler interface {
	Handle(ctx context.Context, msg InboundMessage) (OutboundMessage, error)
}

// ErrDuplicate is returned by Handler when the message ID was already seen.
// It is a success outcome, not a failure: the IM must not redeliver.
var ErrDuplicate = errors.New("duplicate message")

// HandlerFunc lets a plain function be used as a Handler.
type HandlerFunc func(ctx context.Context, msg InboundMessage) (OutboundMessage, error)

// Handle implements Handler.
func (f HandlerFunc) Handle(ctx context.Context, msg InboundMessage) (OutboundMessage, error) {
	return f(ctx, msg)
}

// Channel integrates one kind of IM platform.
// Each channel implements both directions: RegisterRoutes receives IM
// callbacks (inbound), Send proactively pushes messages (outbound).
type Channel interface {
	// Name returns the channel identifier, e.g. "mock", "wecom".
	Name() string
	// RegisterRoutes mounts the IM callback routes onto the HTTP mux;
	// received messages are normalized and handed to h.
	RegisterRoutes(mux *http.ServeMux, h Handler)
	// Send calls the IM proactive-send API (wecom message/send, wechat kf
	// messages, etc.) to deliver the reply to the user.
	Send(ctx context.Context, msg OutboundMessage) error
}
