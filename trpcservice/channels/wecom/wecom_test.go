package wecom

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/sbzhu/weworkapi_golang/wxbizmsgcrypt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

const (
	testToken  = "test-token"
	testAESKey = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG" // 43 chars, per the platform spec
	testCorpID = "ww1234567890"
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
		CorpID: testCorpID, AgentID: 1000002,
		TokenRef: "tok", AESKeyRef: "aes", SecretRef: "secret", APIBase: apiBase,
	}, mapResolver{"tok": testToken, "aes": testAESKey, "secret": "corp-secret"})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// sendEnvelope is the wrapper produced by EncryptMsg; the test borrows its
// Encrypt and MsgSignature to forge a callback body the callback can decrypt.
type sendEnvelope struct {
	XMLName      xml.Name `xml:"xml"`
	Encrypt      string   `xml:"Encrypt"`
	MsgSignature string   `xml:"MsgSignature"`
	TimeStamp    string   `xml:"TimeStamp"`
	Nonce        string   `xml:"Nonce"`
}

// forgeCallback encrypts innerXML the way the platform would and returns the
// callback body plus the query string carrying a valid signature.
func forgeCallback(t *testing.T, innerXML string) (body []byte, query string) {
	t.Helper()
	crypt := wxbizmsgcrypt.NewWXBizMsgCrypt(testToken, testAESKey, testCorpID, wxbizmsgcrypt.XmlType)
	encrypted, cerr := crypt.EncryptMsg(innerXML, "1700000000", "nonce-1")
	if cerr != nil {
		t.Fatalf("encrypt: %s", cerr.ErrMsg)
	}
	var env sendEnvelope
	if err := xml.Unmarshal(encrypted, &env); err != nil {
		t.Fatal(err)
	}
	body = []byte(fmt.Sprintf(`<xml><ToUserName><![CDATA[%s]]></ToUserName><Encrypt><![CDATA[%s]]></Encrypt><AgentID><![CDATA[1000002]]></AgentID></xml>`,
		testCorpID, env.Encrypt))
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

	inner := `<xml><ToUserName><![CDATA[ww1234567890]]></ToUserName><FromUserName><![CDATA[zhangsan]]></FromUserName><CreateTime>1700000000</CreateTime><MsgType><![CDATA[text]]></MsgType><Content><![CDATA[你好]]></Content><MsgId>9876543210</MsgId><AgentID>1000002</AgentID></xml>`
	body, query := forgeCallback(t, inner)

	req := httptest.NewRequest(http.MethodPost, "/wecom/callback?"+query, strings.NewReader(string(body)))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("callback status = %d, body %s", rec.Code, rec.Body.String())
	}
	if got.MsgID != "9876543210" || got.UserID != "zhangsan" || got.Text != "你好" {
		t.Fatalf("bad normalization: %+v", got)
	}
	if got.SessionKey != "dm:wecom:zhangsan" {
		t.Fatalf("unexpected session key: %q", got.SessionKey)
	}
	if got.WebhookPath != "/wecom/callback" {
		t.Fatalf("unexpected webhook path: %q", got.WebhookPath)
	}
}

func TestCallbackGroupChat(t *testing.T) {
	c := testChannel(t, "")
	var got channels.InboundMessage
	mux := http.NewServeMux()
	c.RegisterRoutes(mux, channels.HandlerFunc(func(_ context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
		got = msg
		return channels.OutboundMessage{}, nil
	}))

	inner := `<xml><ToUserName><![CDATA[ww1234567890]]></ToUserName><FromUserName><![CDATA[zhangsan]]></FromUserName><CreateTime>1700000000</CreateTime><MsgType><![CDATA[text]]></MsgType><Content><![CDATA[群里好]]></Content><MsgId>9876543211</MsgId><AgentID>1000002</AgentID><ChatId><![CDATA[roomA]]></ChatId></xml>`
	body, query := forgeCallback(t, inner)

	req := httptest.NewRequest(http.MethodPost, "/wecom/callback?"+query, strings.NewReader(string(body)))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("callback status = %d", rec.Code)
	}
	if got.ChatID != "roomA" || got.SessionKey != "group:wecom:roomA" {
		t.Fatalf("group message misrouted: %+v", got)
	}
}

func TestCallbackSkipsEventsAndBadSignatures(t *testing.T) {
	c := testChannel(t, "")
	called := false
	mux := http.NewServeMux()
	c.RegisterRoutes(mux, channels.HandlerFunc(func(_ context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
		called = true
		return channels.OutboundMessage{}, nil
	}))

	// Event callbacks (no MsgId) are acked and skipped.
	inner := `<xml><ToUserName><![CDATA[ww1234567890]]></ToUserName><FromUserName><![CDATA[zhangsan]]></FromUserName><CreateTime>1700000000</CreateTime><MsgType><![CDATA[event]]></MsgType><Event><![CDATA[enter_agent]]></Event><AgentID>1000002</AgentID></xml>`
	body, query := forgeCallback(t, inner)
	req := httptest.NewRequest(http.MethodPost, "/wecom/callback?"+query, strings.NewReader(string(body)))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || called {
		t.Fatalf("event must be acked and skipped, status=%d called=%v", rec.Code, called)
	}

	// Tampered signature: acked (no redelivery), never handled.
	req = httptest.NewRequest(http.MethodPost, "/wecom/callback?msg_signature=bad&timestamp=1700000000&nonce=nonce-1", strings.NewReader(string(body)))
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || called {
		t.Fatalf("bad signature must be acked and skipped, status=%d called=%v", rec.Code, called)
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

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/wecom/callback?msg_signature=%s&timestamp=%s&nonce=%s&echostr=%s",
		url.QueryEscape(env.MsgSignature), env.TimeStamp, env.Nonce, url.QueryEscape(env.Encrypt)), nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "echo-challenge" {
		t.Fatalf("verify failed: status=%d body=%q", rec.Code, rec.Body.String())
	}
}
