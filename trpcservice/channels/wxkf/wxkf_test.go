package wxkf

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/sbzhu/weworkapi_golang/wxbizmsgcrypt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

const (
	testToken     = "test-token"
	testAESKey    = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG" // 43 chars, per the platform spec
	testCorpID    = "ww1234567890"
	testKfAccount = "wkABCDEFGHIJ"
)

// mapResolver resolves secret refs from a map.
type mapResolver map[string]string

func (m mapResolver) Resolve(_ context.Context, ref string) (string, error) {
	v, ok := m[ref]
	if !ok {
		return "", fmt.Errorf("unknown ref %q", ref)
	}
	return v, nil
}

func testChannel(t *testing.T, apiBase string) *Channel {
	t.Helper()
	c, err := New(Config{
		CorpID: testCorpID, KfAccount: testKfAccount,
		TokenRef: "tok", AESKeyRef: "aes", SecretRef: "secret", APIBase: apiBase,
	}, mapResolver{"tok": testToken, "aes": testAESKey, "secret": "kf-secret"})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// sendEnvelope is the wrapper produced by EncryptMsg; the test borrows its
// Encrypt and MsgSignature to forge a JSON callback body the callback can
// decrypt (the signature covers token/timestamp/nonce/encrypt only, so it is
// envelope-format independent).
type sendEnvelope struct {
	XMLName      xml.Name `xml:"xml"`
	Encrypt      string   `xml:"Encrypt"`
	MsgSignature string   `xml:"MsgSignature"`
	TimeStamp    string   `xml:"TimeStamp"`
	Nonce        string   `xml:"Nonce"`
}

// forgeCallback encrypts innerJSON the way the platform would and returns the
// JSON callback body plus the query string carrying a valid signature.
func forgeCallback(t *testing.T, innerJSON string) (body []byte, query string) {
	t.Helper()
	crypt := wxbizmsgcrypt.NewWXBizMsgCrypt(testToken, testAESKey, testCorpID, wxbizmsgcrypt.XmlType)
	encrypted, cerr := crypt.EncryptMsg(innerJSON, "1700000000", "nonce-1")
	if cerr != nil {
		t.Fatalf("encrypt: %s", cerr.ErrMsg)
	}
	var env sendEnvelope
	if err := xml.Unmarshal(encrypted, &env); err != nil {
		t.Fatal(err)
	}
	body = []byte(fmt.Sprintf(`{"encrypt":%q}`, env.Encrypt))
	query = fmt.Sprintf("msg_signature=%s&timestamp=%s&nonce=%s",
		url.QueryEscape(env.MsgSignature), env.TimeStamp, env.Nonce)
	return body, query
}

func TestCallbackRoundTrip(t *testing.T) {
	c := testChannel(t, "")
	var got channels.InboundMessage
	mux := http.NewServeMux()
	c.RegisterRoutes(mux, channels.HandlerFunc(func(_ context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
		got = msg
		return channels.OutboundMessage{}, nil
	}))

	inner := fmt.Sprintf(`{"msgid":"9876543210","openid":"oUSER1","msgtype":"text","text":{"content":"你好"},"create_time":%d}`, time.Now().Unix())
	body, query := forgeCallback(t, inner)

	req := httptest.NewRequest(http.MethodPost, "/wxkf/callback?"+query, strings.NewReader(string(body)))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("callback status = %d, body %s", rec.Code, rec.Body.String())
	}
	if got.MsgID != "9876543210" || got.UserID != "oUSER1" || got.Text != "你好" {
		t.Fatalf("bad normalization: %+v", got)
	}
	if got.SessionKey != "dm:wxkf:oUSER1" {
		t.Fatalf("unexpected session key: %q", got.SessionKey)
	}
	if got.ChatID != "" {
		t.Fatalf("kf is direct-chat only, ChatID must be empty: %q", got.ChatID)
	}
	if got.WebhookPath != "/wxkf/callback" {
		t.Fatalf("unexpected webhook path: %q", got.WebhookPath)
	}
}

func TestCallbackSkipsEventsAndNonText(t *testing.T) {
	c := testChannel(t, "")
	called := false
	mux := http.NewServeMux()
	c.RegisterRoutes(mux, channels.HandlerFunc(func(_ context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
		called = true
		return channels.OutboundMessage{}, nil
	}))

	// Event callbacks are acked and skipped.
	inner := fmt.Sprintf(`{"openid":"oUSER1","msgtype":"event","event":{"event_type":"enter_session"},"create_time":%d}`, time.Now().Unix())
	body, query := forgeCallback(t, inner)
	req := httptest.NewRequest(http.MethodPost, "/wxkf/callback?"+query, strings.NewReader(string(body)))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || called {
		t.Fatalf("event must be acked and skipped, status=%d called=%v", rec.Code, called)
	}

	// Media messages (image/voice/file/link/miniprogram) are acked and
	// skipped; media handling is a follow-up.
	inner = `{"msgid":"9876543211","openid":"oUSER1","msgtype":"image","image":{"media_id":"MEDIA_ID"}}`
	body, query = forgeCallback(t, inner)
	req = httptest.NewRequest(http.MethodPost, "/wxkf/callback?"+query, strings.NewReader(string(body)))
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || called {
		t.Fatalf("media must be acked and skipped, status=%d called=%v", rec.Code, called)
	}
}

func TestCallbackSkipsBadSignatures(t *testing.T) {
	c := testChannel(t, "")
	called := false
	mux := http.NewServeMux()
	c.RegisterRoutes(mux, channels.HandlerFunc(func(_ context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
		called = true
		return channels.OutboundMessage{}, nil
	}))

	inner := `{"msgid":"9876543212","openid":"oUSER1","msgtype":"text","text":{"content":"你好"}}`
	body, _ := forgeCallback(t, inner)

	// Tampered signature: acked (no redelivery), never handled.
	req := httptest.NewRequest(http.MethodPost, "/wxkf/callback?msg_signature=bad&timestamp=1700000000&nonce=nonce-1", strings.NewReader(string(body)))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || called {
		t.Fatalf("bad signature must be acked and skipped, status=%d called=%v", rec.Code, called)
	}

	// Malformed envelope (no encrypt field): acked, never handled.
	req = httptest.NewRequest(http.MethodPost, "/wxkf/callback?msg_signature=bad&timestamp=1700000000&nonce=nonce-1", strings.NewReader(`{"foo":"bar"}`))
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || called {
		t.Fatalf("malformed envelope must be acked and skipped, status=%d called=%v", rec.Code, called)
	}
}

// ErrDuplicate is a success outcome: the callback must answer 200 so the
// platform stops redelivering. A real failure stays 5xx — that is what makes
// the platform retry (and the gateway rolls the dedup key back).
func TestCallbackDuplicateVersusFailure(t *testing.T) {
	const inner = `{"msgid":"9876543213","openid":"oUSER1","msgtype":"text","text":{"content":"你好"}}`
	body, query := forgeCallback(t, inner)

	post := func(t *testing.T, h channels.HandlerFunc) int {
		t.Helper()
		c := testChannel(t, "")
		mux := http.NewServeMux()
		c.RegisterRoutes(mux, h)
		req := httptest.NewRequest(http.MethodPost, "/wxkf/callback?"+query, strings.NewReader(string(body)))
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec.Code
	}

	if code := post(t, func(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
		return channels.OutboundMessage{}, channels.ErrDuplicate
	}); code != http.StatusOK {
		t.Fatalf("duplicate must be answered 200, got %d", code)
	}

	if code := post(t, func(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
		return channels.OutboundMessage{}, fmt.Errorf("enqueue inbound: boom")
	}); code != http.StatusInternalServerError {
		t.Fatalf("real failure must be answered 5xx so the IM retries, got %d", code)
	}
}

func TestVerifyURL(t *testing.T) {
	c := testChannel(t, "")
	mux := http.NewServeMux()
	c.RegisterRoutes(mux, channels.HandlerFunc(func(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
		t.Error("handler must not run for URL verification")
		return channels.OutboundMessage{}, nil
	}))

	crypt := wxbizmsgcrypt.NewWXBizMsgCrypt(testToken, testAESKey, testCorpID, wxbizmsgcrypt.XmlType)
	encrypted, cerr := crypt.EncryptMsg("echo-challenge", "1700000000", "nonce-1")
	if cerr != nil {
		t.Fatalf("encrypt: %s", cerr.ErrMsg)
	}
	var env sendEnvelope
	if err := xml.Unmarshal(encrypted, &env); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/wxkf/callback?msg_signature=%s&timestamp=%s&nonce=%s&echostr=%s",
		url.QueryEscape(env.MsgSignature), env.TimeStamp, env.Nonce, url.QueryEscape(env.Encrypt)), nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "echo-challenge" {
		t.Fatalf("verify failed: status=%d body=%q", rec.Code, rec.Body.String())
	}
}
