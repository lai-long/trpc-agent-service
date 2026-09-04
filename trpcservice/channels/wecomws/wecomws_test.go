package wecomws

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

// respondContent extracts the content of a recorded aibot_respond_msg frame.
func respondContent(t *testing.T, env envelope) (msgType, content string) {
	t.Helper()
	var body struct {
		MsgType string `json:"msgtype"`
		Text    struct {
			Content string `json:"content"`
		} `json:"text"`
		Markdown struct {
			Content string `json:"content"`
		} `json:"markdown"`
	}
	if err := json.Unmarshal(env.Body, &body); err != nil {
		t.Fatalf("respond body: %v (%s)", err, env.Body)
	}
	switch body.MsgType {
	case "text":
		return body.MsgType, body.Text.Content
	case "markdown":
		return body.MsgType, body.Markdown.Content
	default:
		t.Fatalf("unexpected msgtype %q in %s", body.MsgType, env.Body)
		return "", ""
	}
}

func startSubscribed(t *testing.T) (*fakePlatform, *fakeConn, *Channel) {
	t.Helper()
	f := newFakePlatform(t, "bot-1", "s3cr3t")
	ch := newTestChannel(t, f.addr(), []Binding{mustBinding(t, "b1", "bot-1", "ref")}, "s3cr3t", &recordingHandler{})
	conn := f.waitConn(t, 1)
	waitFor(t, func() bool { return f.totalAcks.Load() >= 1 })
	return f, conn, ch
}

// TestSendRespondPassthrough: the reply rides aibot_respond_msg frames that
// echo the callback's req_id verbatim, with text and markdown body shapes.
func TestSendRespondPassthrough(t *testing.T) {
	_, conn, ch := startSubscribed(t)
	ctx := context.Background()

	if err := ch.Send(ctx, channels.OutboundMessage{
		Channel: "wecomws", BindingID: "b1", MsgID: "m1", ReplyToken: "req-42",
		SessionKey: "dm:wecomws:u1", UserID: "u1", Text: "你好",
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(conn.respondFrames()) == 1 })
	resp := conn.respondFrames()[0]
	if resp.Cmd != cmdRespond || resp.Headers.ReqID != "req-42" {
		t.Fatalf("reply must echo the callback req_id verbatim: %+v", resp)
	}
	if msgType, content := respondContent(t, resp); msgType != "text" || content != "你好" {
		t.Fatalf("text reply shape: msgtype=%q content=%q", msgType, content)
	}

	if err := ch.Send(ctx, channels.OutboundMessage{
		Channel: "wecomws", BindingID: "b1", MsgID: "m2", ReplyToken: "req-43",
		SessionKey: "dm:wecomws:u1", UserID: "u1", Text: "**bold**",
		TextType: channels.TextTypeMarkdown,
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(conn.respondFrames()) == 2 })
	if msgType, content := respondContent(t, conn.respondFrames()[1]); msgType != "markdown" || content != "**bold**" {
		t.Fatalf("markdown reply shape: msgtype=%q content=%q", msgType, content)
	}
}

// TestSendSplit: a reply longer than the per-frame cap goes out as sequential
// segments, split on rune boundaries, each echoing the same req_id. The cap
// is compressed to 32 bytes so the multi-frame path runs in milliseconds.
func TestSendSplit(t *testing.T) {
	text := "需要分段的消息" + strings.Repeat("好", 100) // 5 + 300 bytes
	f, conn, ch := startSubscribedWithSegment(t, 32)
	_ = f

	if err := ch.Send(context.Background(), channels.OutboundMessage{
		Channel: "wecomws", BindingID: "b1", MsgID: "m1", ReplyToken: "req-split",
		SessionKey: "dm:wecomws:u1", UserID: "u1", Text: text,
	}); err != nil {
		t.Fatal(err)
	}
	want := channels.SplitText(text, 32)
	waitFor(t, func() bool { return len(conn.respondFrames()) == len(want) })

	frames := conn.respondFrames()
	var joined strings.Builder
	for i, resp := range frames {
		if resp.Headers.ReqID != "req-split" {
			t.Fatalf("segment %d must echo the callback req_id, got %q", i, resp.Headers.ReqID)
		}
		_, content := respondContent(t, resp)
		if len(content) > 32 {
			t.Fatalf("segment %d exceeds the cap: %d bytes", i, len(content))
		}
		if !utf8.ValidString(content) {
			t.Fatalf("segment %d split mid-rune: %q", i, content)
		}
		if content != want[i] {
			t.Fatalf("segment %d = %q, want %q", i, content, want[i])
		}
		joined.WriteString(content)
	}
	if joined.String() != text {
		t.Fatal("segments must reassemble the original reply")
	}
}

// startSubscribedWithSegment is startSubscribed with a small per-frame cap.
func startSubscribedWithSegment(t *testing.T, segment int) (*fakePlatform, *fakeConn, *Channel) {
	t.Helper()
	f := newFakePlatform(t, "bot-1", "s3cr3t")
	ch := newTestChannel(t, f.addr(), []Binding{mustBinding(t, "b1", "bot-1", "ref")}, "s3cr3t",
		&recordingHandler{}, WithSegmentBytes(segment))
	conn := f.waitConn(t, 1)
	waitFor(t, func() bool { return f.totalAcks.Load() >= 1 })
	return f, conn, ch
}

// TestSendUnknownBinding: no live connection for the binding is an error —
// the message stays in the sender's PEL and redelivers instead of vanishing.
func TestSendUnknownBinding(t *testing.T) {
	_, _, ch := startSubscribed(t)
	err := ch.Send(context.Background(), channels.OutboundMessage{
		Channel: "wecomws", BindingID: "b-gone", MsgID: "m1", ReplyToken: "req-1",
		UserID: "u1", Text: "hi",
	})
	if err == nil {
		t.Fatal("unknown binding must error")
	}
}

// TestSendEmptyReplyToken: without the callback's req_id the platform cannot
// correlate the reply — fail loudly, never send a silent no-op.
func TestSendEmptyReplyToken(t *testing.T) {
	_, conn, ch := startSubscribed(t)
	err := ch.Send(context.Background(), channels.OutboundMessage{
		Channel: "wecomws", BindingID: "b1", MsgID: "m1",
		UserID: "u1", Text: "hi",
	})
	if err == nil {
		t.Fatal("missing reply token must error")
	}
	time.Sleep(100 * time.Millisecond)
	if got := len(conn.respondFrames()); got != 0 {
		t.Fatalf("no frame may leave without a reply token, got %d", got)
	}
}

// TestParseBindingConfig: the two required fields are enforced; a malformed
// row is skipped by the reconcile instead of crashing a connection.
func TestParseBindingConfig(t *testing.T) {
	valid, err := json.Marshal(bindingConfig{BotID: "bot-1", SecretRef: "ref"})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name    string
		raw     json.RawMessage
		wantErr bool
	}{
		{"valid", valid, false},
		{"empty", nil, true},
		{"not json", json.RawMessage(`{oops`), true},
		{"missing bot_id", json.RawMessage(`{"secret_ref":"r"}`), true},
		{"missing secret_ref", json.RawMessage(`{"bot_id":"b"}`), true},
		{"empty bot_id", json.RawMessage(`{"bot_id":"","secret_ref":"r"}`), true},
		{"empty secret_ref", json.RawMessage(`{"bot_id":"b","secret_ref":""}`), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseBindingConfig(tc.raw)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want error, got %+v", cfg)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.BotID != "bot-1" || cfg.SecretRef != "ref" {
				t.Fatalf("parsed %+v", cfg)
			}
		})
	}
}
