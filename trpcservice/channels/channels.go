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
	"net/url"
	"strings"
	"time"
)

// InboundMessage is a normalized inbound message: the Channel builds it from
// the platform-specific callback payload and hands it to the Handler.
// TenantID/AppID start empty; the Gateway stamps them after resolving
// WebhookPath through channel_binding (tenant routing).
type InboundMessage struct {
	Channel     string    // channel type: mock / wecom / wxkf ...
	MsgID       string    // IM platform message ID, for idempotent dedup (dedup:{channel}:{msg_id})
	SessionKey  string    // unique conversation key within an app, see SessionKey()
	UserID      string    // user ID on the IM side (wecom external_userid / wechat openid)
	ChatID      string    // group chat ID; empty for direct chats
	Text        string    // text content (media messages carry a placeholder)
	Type        string    // Type* constants; empty means TypeText
	MediaRef    string    // TypeMedia: artifact reference of the fetched media (design 5.3.2)
	WebhookPath string    // callback path the message arrived on; routes to tenant/app
	BindingID   string    // channel_binding row serving this callback, stamped by the Gateway; scopes the per-binding idempotency keys (done:/sent:)
	TenantID    string    // owning tenant UUID, stamped by the Gateway
	AppID       string    // owning agent app UUID, stamped by the Gateway
	TraceID     string    // trace ID, spanning callback → Worker → reply
	TraceParent string    // W3C traceparent, carries the trace across the Stream boundary
	ReceivedAt  time.Time // when the callback arrived
}

// Inbound message types (InboundMessage.Type).
const (
	// TypeText is a plain text message (the default).
	TypeText = "text"
	// TypeMedia is an image/voice/file message: the adapter fetched the
	// platform's media_id into artifact storage and MediaRef carries the
	// reference; Text is a human-readable placeholder for the model.
	TypeMedia = "media"
	// TypeRecall is a message-recalled event: it never reaches the LLM — the
	// guardrail audits it and marks the session state (design 5.3.2 撤回).
	TypeRecall = "recall"
)

// TextTypeMarkdown marks outbound text carrying markdown markup.
const TextTypeMarkdown = "markdown"

// LooksMarkdown heuristically detects markdown markup in a reply, so channels
// that render markdown can use it and others downgrade (RenderPlain).
func LooksMarkdown(s string) bool {
	for _, marker := range []string{"```", "**", "## ", "](", "- "} {
		if strings.Contains(s, marker) {
			return true
		}
	}
	return false
}

// OutboundMessage is a normalized outbound reply, produced by the Handler and
// delivered through the Channel's Send.
type OutboundMessage struct {
	Channel     string // which channel to reply on
	MsgID       string // the inbound message this replies to (outbound idempotency unit sent:{channel}:{binding}:{msg_id})
	SessionKey  string // conversation the reply belongs to
	UserID      string // recipient (required for direct chats)
	ChatID      string // recipient group (required for group chats)
	Text        string
	BindingID   string // channel_binding row the inbound arrived on, carried for the sent: idempotency key
	TenantID    string // owning tenant UUID, carried through for sender metrics
	TraceID     string
	TraceParent string // W3C traceparent, carried through to the outbound span

	// Token usage of the generating run plus the model that produced it, for
	// audit/cost accounting (design 5.1.3 audit_log cost columns). Never sent
	// to the IM; the sender ignores these fields.
	PromptTokens     int
	CompletionTokens int
	Model            string

	// TextType is the reply markup: "" or "markdown". Channels without
	// markdown support (e.g. WeChat KF) downgrade via RenderPlain (design
	// 5.3.2 通道级渲染降级: 卡片 → markdown → 纯文本).
	TextType string

	// ReceivedAt is the inbound callback's arrival time, carried through so
	// the sender can record the end-to-end latency (design 5.2.4: 端到端
	// P95 回调→回复落 IM).
	ReceivedAt time.Time
}

// RenderPlain strips common markdown syntax for channels that render plain
// text only (微信客服): bold/italic markers, heading hashes, inline code
// ticks and link targets are dropped, the visible text is kept.
func RenderPlain(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '*' || c == '`':
			continue // bold/italic/code markers
		case c == '#' && (i == 0 || s[i-1] == '\n'):
			for i+1 < len(s) && s[i+1] == '#' {
				i++
			}
			if i+1 < len(s) && s[i+1] == ' ' {
				i++
			}
			continue // heading prefix
		case c == '[':
			// [text](url) → text (url); end is the offset of "]" in s[i:].
			if end := strings.Index(s[i:], "]("); end > 0 {
				rest := s[i+end+2:]
				if close := strings.IndexByte(rest, ')'); close >= 0 {
					b.WriteString(s[i+1 : i+end])
					b.WriteString(" (")
					b.WriteString(rest[:close])
					b.WriteString(")")
					i += end + 2 + close
					continue
				}
			}
			b.WriteByte(c)
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

// MediaStore persists IM media fetched by an adapter (design 5.3.2: 素材落
// Artifact，消息体只带引用). *storage.S3ArtifactService satisfies it.
type MediaStore interface {
	SaveMedia(ctx context.Context, channel, msgID, filename, mimeType string, data []byte) (ref string, err error)
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

// BindingCredentials carries one channel_binding row's callback verification
// material (design 5.3.1 多租户接入). Secret material stays as references —
// the adapter resolves them through its SecretResolver and never logs them.
type BindingCredentials struct {
	BindingID string
	// CorpID is the crypt receiver id (WeCom corp / WeChat corp); empty falls
	// back to the adapter's env-configured corp, or the binding's config.
	CorpID string
	// TokenRef / AESKeyRef are the callback verification secret references;
	// empty falls back to the adapter's env-configured single-binding default.
	TokenRef  string
	AESKeyRef string
}

// BindingAware is implemented by Channel adapters that can serve multiple
// tenants, verifying each callback with the serving binding's own credentials.
// The gateway dispatches /callback/{channel}/{binding_id} to the adapter;
// the env-configured callback path stays mounted as the single-binding
// default for deployments that have not moved to binding rows. The hardcoded
// one-path-per-adapter model structurally limited the platform to one tenant
// per channel (review P1-5).
type BindingAware interface {
	// CallbackHandler returns the HTTP handler serving one binding's
	// callbacks: GET answers the platform's URL-registration challenge, POST
	// verifies the signature and decrypts before any payload is parsed, then
	// hands the normalized message to h. Errors mean the binding's credential
	// references could not be resolved — the caller answers 5xx and the IM
	// redelivers.
	CallbackHandler(h Handler, creds BindingCredentials) (http.HandlerFunc, error)
}

// ScrubError strips credentials from *url.Error values before they reach a
// log or trace: the WeCom/KF APIs carry access_token (and the corpsecret, on
// gettoken) in the query string, and net/http embeds the full URL in the
// error. The field-key log redaction cannot see inside a string, so the
// scrub happens at the source (design: 密钥零明文红线).
func ScrubError(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		if u, perr := url.Parse(ue.URL); perr == nil {
			q := u.Query()
			for _, k := range []string{"access_token", "corpsecret"} {
				if q.Has(k) {
					q.Set(k, "***")
				}
			}
			u.RawQuery = q.Encode()
			ue.URL = u.String()
		}
	}
	return err
}
