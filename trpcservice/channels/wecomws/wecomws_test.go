package wecomws

import (
	"context"
	"encoding/json"
	"errors"
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

// TestSendStaleReplyTokenAfterReconnect: a req_id is only deliverable on the
// connection that received its callback. After a reconnect the write on the
// successor would succeed while the platform drops the uncorrelatable frame —
// under that behavior the sender recorded the reply as sent and the user never
// saw it. Send must reject the stale token as an error and write nothing,
// while a fresh token on the live connection still delivers, and a bare
// pre-scheme token (in flight across a rolling deploy) keeps sending as-is.
func TestSendStaleReplyTokenAfterReconnect(t *testing.T) {
	f := newFakePlatform(t, "bot-1", "s3cr3t")
	h := &recordingHandler{}
	ch, bc, _ := startOne(t, f, h)
	conn := f.waitConn(t, 1)
	waitFor(t, func() bool { return f.totalAcks.Load() >= 1 })

	// One callback lands on the first connection; its token must carry that
	// connection's epoch.
	conn.Push(msgEnvelope("req-stale", "m-stale"))
	waitFor(t, func() bool { return h.calls() >= 1 })
	staleToken := h.last().ReplyToken
	epoch, reqID := parseReplyToken(staleToken)
	if epoch == 0 || reqID != "req-stale" {
		t.Fatalf("the reply token must be epoch-scoped, got %q", staleToken)
	}

	// The platform kicks the connection; a fresh one takes over.
	conn.Push(envelope{
		Cmd:     cmdEventCallback,
		Headers: frameHeaders{ReqID: "ev-kick"},
		Body:    json.RawMessage(`{"eventtype":"disconnected_event"}`),
	})
	conn2 := f.waitConn(t, 2)
	// Wait for the takeover to be published (not merely dialed): until the
	// successor's epoch lands, Send would race a "connection is down" instead
	// of the stale verdict.
	waitFor(t, func() bool { return bc.currentEpoch() != epoch })

	err := ch.Send(context.Background(), channels.OutboundMessage{
		Channel: "wecomws", BindingID: "b1", MsgID: "m-stale", ReplyToken: staleToken,
		UserID: "u1", Text: "too late",
	})
	var stale *staleReplyError
	if !errors.As(err, &stale) {
		t.Fatalf("a reply across a reconnect must fail as stale, got %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	if got := len(conn2.respondFrames()); got != 0 {
		t.Fatalf("a stale reply must not be written on the successor, got %d frames", got)
	}

	// A fresh callback on the successor replies normally, and the frame
	// carries the bare req_id — never the scoped token.
	conn2.Push(msgEnvelope("req-fresh", "m-fresh"))
	waitFor(t, func() bool { return h.calls() >= 2 })
	if err := ch.Send(context.Background(), channels.OutboundMessage{
		Channel: "wecomws", BindingID: "b1", MsgID: "m-fresh", ReplyToken: h.last().ReplyToken,
		UserID: "u1", Text: "on time",
	}); err != nil {
		t.Fatalf("a reply on the live connection must send: %v", err)
	}
	waitFor(t, func() bool { return len(conn2.respondFrames()) == 1 })
	if resp := conn2.respondFrames()[0]; resp.Headers.ReqID != "req-fresh" {
		t.Fatalf("the frame must echo the bare req_id, got %+v", resp)
	}

	// A token without an epoch (written before the scheme existed) keeps the
	// old fail-open behavior: sent verbatim.
	if err := ch.Send(context.Background(), channels.OutboundMessage{
		Channel: "wecomws", BindingID: "b1", MsgID: "m-legacy", ReplyToken: "req-legacy",
		UserID: "u1", Text: "rolling deploy",
	}); err != nil {
		t.Fatalf("a bare pre-scheme token must still send: %v", err)
	}
	waitFor(t, func() bool { return len(conn2.respondFrames()) == 2 })
	if resp := conn2.respondFrames()[1]; resp.Headers.ReqID != "req-legacy" {
		t.Fatalf("the bare token must ride the frame verbatim, got %+v", resp)
	}
}

// TestParseReplyToken: round-trip and the shapes parseReplyToken must not
// mistake for scoped — bare ids, and a colon whose prefix is not an epoch.
func TestParseReplyToken(t *testing.T) {
	if e, r := parseReplyToken(scopedReplyToken(42, "req-1")); e != 42 || r != "req-1" {
		t.Fatalf("round trip: epoch=%d req_id=%q", e, r)
	}
	if e, r := parseReplyToken("req-42"); e != 0 || r != "req-42" {
		t.Fatalf("a bare req_id must stay unscoped: epoch=%d req_id=%q", e, r)
	}
	if e, r := parseReplyToken("req:x"); e != 0 || r != "req:x" {
		t.Fatalf("a non-numeric prefix must stay unscoped: epoch=%d req_id=%q", e, r)
	}
	// A scoped token keeps a colon inside the req_id intact.
	if e, r := parseReplyToken(scopedReplyToken(7, "odd:req")); e != 7 || r != "odd:req" {
		t.Fatalf("the split must be at the first colon only: epoch=%d req_id=%q", e, r)
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
