package wecomws

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// WeCom aibot WebSocket protocol commands.
const (
	cmdSubscribe     = "aibot_subscribe"      // client → platform credential handshake
	cmdPing          = "ping"                 // client → platform heartbeat
	cmdPong          = "pong"                 // platform → client heartbeat answer (echoes req_id)
	cmdRespond       = "aibot_respond_msg"    // client → platform reply; headers.req_id must be the callback's req_id verbatim
	cmdMsgCallback   = "aibot_msg_callback"   // platform → client message callback
	cmdEventCallback = "aibot_event_callback" // platform → client event callback
)

// Inbound event types (eventCallback).
const (
	eventDisconnected = "disconnected_event" // this connection was kicked by a newer subscription
	eventEnterChat    = "enter_chat"
	eventTemplateCard = "template_card_event"
)

// envelope is the outer frame shared by every command: a cmd, the headers
// carrying the request id that correlates a response (or a reply) to its
// request (or callback), and the command-specific body.
type envelope struct {
	Cmd     string          `json:"cmd"`
	Headers frameHeaders    `json:"headers,omitempty"`
	Body    json.RawMessage `json:"body,omitempty"`
}

type frameHeaders struct {
	ReqID string `json:"req_id,omitempty"`
}

// errcodeBody is the platform's ack shape (the subscribe response, among
// others).
type errcodeBody struct {
	ErrCode int    `json:"errcode"`
	ErrMsg  string `json:"errmsg"`
}

// subscribeFrame asks the platform to bind the connection to botID. The
// secret rides this first frame instead of the URL/handshake, keeping it out
// of access logs.
func subscribeFrame(botID, secret, reqID string) (envelope, error) {
	//nolint:gosec // G117: the secret is the subscribe credential, sent to the platform over wss by design
	body, err := json.Marshal(struct {
		BotID  string `json:"bot_id"`
		Secret string `json:"secret"`
	}{BotID: botID, Secret: secret})
	if err != nil {
		return envelope{}, err
	}
	return envelope{Cmd: cmdSubscribe, Headers: frameHeaders{ReqID: reqID}, Body: body}, nil
}

// pingFrame is one heartbeat, carrying a fresh req_id the pong must echo.
func pingFrame(reqID string) envelope {
	return envelope{Cmd: cmdPing, Headers: frameHeaders{ReqID: reqID}}
}

// respondFrame builds the reply to one callback: the callback's req_id rides
// the headers verbatim — the platform correlates a reply to its question by
// it and rejects (or drops) mismatches. msgType doubles as the content key
// ("markdown": {...} / "text": {...}).
func respondFrame(reqID, msgType, content string) (envelope, error) {
	body, err := json.Marshal(map[string]any{
		"msgtype": msgType,
		msgType:   map[string]string{"content": content},
	})
	if err != nil {
		return envelope{}, err
	}
	return envelope{Cmd: cmdRespond, Headers: frameHeaders{ReqID: reqID}, Body: body}, nil
}

// msgCallback is the body of an inbound message callback.
type msgCallback struct {
	MsgID    string `json:"msgid"`
	AibotID  string `json:"aibot_id"`
	ChatID   string `json:"chatid"`   // group chats only; empty for direct chats
	ChatType string `json:"chattype"` // single / group
	From     struct {
		UserID string `json:"userid"`
	} `json:"from"`
	MsgType string `json:"msgtype"`
	Text    struct {
		Content string `json:"content"`
	} `json:"text"`
}

// mediaLabels names the media msgtypes for the downgrade placeholder.
var mediaLabels = map[string]string{
	"image": "图片",
	"voice": "语音",
	"video": "视频",
	"file":  "文件",
	"mixed": "混合消息",
}

// mediaPlaceholder is the text a media message degrades to (no inbound
// media download/decryption yet); unknown types fall back to their
// raw msgtype.
func mediaPlaceholder(msgType string) string {
	label, ok := mediaLabels[msgType]
	if !ok {
		label = msgType
	}
	return "[" + label + "]（暂不支持媒体消息）"
}

// eventCallback is the body of an inbound event frame. The eventtype rides
// either directly on the body or nested under "event"; both nestings are
// accepted.
type eventCallback struct {
	EventType string `json:"eventtype"`
	Event     struct {
		EventType string `json:"eventtype"`
	} `json:"event"`
}

func (e eventCallback) eventType() string {
	if e.EventType != "" {
		return e.EventType
	}
	return e.Event.EventType
}

// newReqID returns a random 16-byte hex ID for client-initiated frames
// (subscribe, ping).
func newReqID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("new req_id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}
