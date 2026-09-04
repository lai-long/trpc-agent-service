package wxkf

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/sbzhu/weworkapi_golang/wxbizmsgcrypt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

// postForged forges one encrypted JSON callback for innerJSON and serves it on
// a binding-scoped handler; the recorder carries the answer.
func postForged(t *testing.T, c *Channel, h channels.Handler, innerJSON string) *httptest.ResponseRecorder {
	t.Helper()
	handler, err := c.CallbackHandler(h, channels.BindingCredentials{
		CorpID: testCorpID, TokenRef: "tok", AESKeyRef: "aes",
	})
	if err != nil {
		t.Fatal(err)
	}
	body, query := forgeCallback(t, innerJSON)
	req := httptest.NewRequest(http.MethodPost, "/callback/wxkf/b1?"+query, strings.NewReader(string(body)))
	rec := httptest.NewRecorder()
	handler(rec, req)
	return rec
}

func TestNewValidation(t *testing.T) {
	resolver := mapResolver{"tok": testToken, "aes": testAESKey, "secret": "kf-secret"}
	cases := []struct {
		name string
		cfg  Config
		want string
	}{
		{"missing corpid", Config{KfAccount: testKfAccount, TokenRef: "tok", AESKeyRef: "aes"}, "CorpID and KfAccount are required"},
		{"missing kf account", Config{CorpID: testCorpID, TokenRef: "tok", AESKeyRef: "aes"}, "CorpID and KfAccount are required"},
		{"unresolvable token", Config{CorpID: testCorpID, KfAccount: testKfAccount, TokenRef: "nope", AESKeyRef: "aes"}, "resolve token"},
		{"unresolvable aes key", Config{CorpID: testCorpID, KfAccount: testKfAccount, TokenRef: "tok", AESKeyRef: "nope"}, "resolve aes key"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(tc.cfg, resolver); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want error %q, got %v", tc.want, err)
			}
		})
	}
}

func TestCryptFor(t *testing.T) {
	c := testChannel(t, "")

	// Empty refs fall back to the env single-binding default, and the same
	// resolved credential set must be served from the cache on the second call.
	first, err := c.cryptFor("", "", "")
	if err != nil {
		t.Fatal(err)
	}
	second, err := c.cryptFor("", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("the same resolved credentials must share one cached crypt")
	}

	if _, err := c.cryptFor("corp-x", "nope", "aes"); err == nil || !strings.Contains(err.Error(), "resolve token") {
		t.Fatalf("unresolvable token ref must error, got %v", err)
	}
	if _, err := c.cryptFor("corp-x", "tok", "nope"); err == nil || !strings.Contains(err.Error(), "resolve aes key") {
		t.Fatalf("unresolvable aes ref must error, got %v", err)
	}

	// The value-keyed cache resets once full, so key rotation cannot grow it.
	for i := 0; i < maxCryptCacheEntries+3; i++ {
		if _, err := c.cryptFor(fmt.Sprintf("corp-%d", i), "tok", "aes"); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(c.crypts); got > maxCryptCacheEntries {
		t.Fatalf("crypt cache must stay bounded, got %d entries", got)
	}
}

// RegisterRoutes is defensive against a misconfigured channel: a broken
// credential set must mount nothing so the path 404s instead of half-serving.
func TestRegisterRoutesMountFailure(t *testing.T) {
	c := &Channel{crypts: map[string]*wxbizmsgcrypt.WXBizMsgCrypt{}}
	mux := http.NewServeMux()
	c.RegisterRoutes(mux, channels.HandlerFunc(func(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
		t.Error("handler must not run on an unmounted path")
		return channels.OutboundMessage{}, nil
	}))
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(method, "/wxkf/callback", strings.NewReader(`{}`)))
		if rec.Code != http.StatusNotFound {
			t.Fatalf("broken channel must mount nothing (404) for %s, got %d", method, rec.Code)
		}
	}
}

func TestCallbackHandlerRejectsBadCredentials(t *testing.T) {
	c := testChannel(t, "")
	h := channels.HandlerFunc(func(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
		return channels.OutboundMessage{}, nil
	})
	if _, err := c.CallbackHandler(h, channels.BindingCredentials{BindingID: "b0"}); err == nil {
		t.Fatal("binding-scoped handler must refuse empty credential refs")
	}
	if _, err := c.CallbackHandler(h, channels.BindingCredentials{BindingID: "b0", TokenRef: "nope", AESKeyRef: "aes"}); err == nil {
		t.Fatal("binding-scoped handler must refuse unresolvable refs")
	}
}

func TestCallbackMethodNotAllowed(t *testing.T) {
	c := testChannel(t, "")
	handler, err := c.CallbackHandler(channels.HandlerFunc(func(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
		t.Error("handler must not run for other methods")
		return channels.OutboundMessage{}, nil
	}), channels.BindingCredentials{CorpID: testCorpID, TokenRef: "tok", AESKeyRef: "aes"})
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	handler(rec, httptest.NewRequest(http.MethodPut, "/callback/wxkf/b1", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("PUT must be rejected 405, got %d", rec.Code)
	}
}

func TestVerifyURLRejectsBadSignature(t *testing.T) {
	c := testChannel(t, "")
	handler, err := c.CallbackHandler(channels.HandlerFunc(func(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
		t.Error("handler must not run for a failed verification")
		return channels.OutboundMessage{}, nil
	}), channels.BindingCredentials{CorpID: testCorpID, TokenRef: "tok", AESKeyRef: "aes"})
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	handler(rec, httptest.NewRequest(http.MethodGet, "/callback/wxkf/b1?msg_signature=tampered&timestamp=1700000000&nonce=n&echostr=x", nil))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("bad signature must be rejected 403, got %d", rec.Code)
	}
}

// A callback whose body cannot be read (over the 1 MiB cap) is a bad request,
// not an ack: the platform must redeliver a truncated callback.
func TestReceiveOversizedBody(t *testing.T) {
	c := testChannel(t, "")
	handler, err := c.CallbackHandler(channels.HandlerFunc(func(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
		t.Error("handler must not run for an unreadable body")
		return channels.OutboundMessage{}, nil
	}), channels.BindingCredentials{CorpID: testCorpID, TokenRef: "tok", AESKeyRef: "aes"})
	if err != nil {
		t.Fatal(err)
	}
	big := strings.Repeat("x", (1<<20)+64)
	rec := httptest.NewRecorder()
	handler(rec, httptest.NewRequest(http.MethodPost, "/callback/wxkf/b1", strings.NewReader(big)))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("oversized body must answer 400, got %d", rec.Code)
	}
}

// A callback envelope that is not JSON is acked (no redelivery) and never
// enters the pipeline.
func TestReceiveUnparsableEnvelope(t *testing.T) {
	c := testChannel(t, "")
	handler, err := c.CallbackHandler(channels.HandlerFunc(func(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
		t.Error("handler must not run for unparsable envelope")
		return channels.OutboundMessage{}, nil
	}), channels.BindingCredentials{CorpID: testCorpID, TokenRef: "tok", AESKeyRef: "aes"})
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	handler(rec, httptest.NewRequest(http.MethodPost, "/callback/wxkf/b1", strings.NewReader(`not-json`)))
	if rec.Code != http.StatusOK || rec.Body.String() != "success" {
		t.Fatalf("unparsable envelope must be acked, got %d %q", rec.Code, rec.Body.String())
	}
}

// A decryptable callback carrying garbage JSON is acked and never enters the
// pipeline.
func TestReceiveUnparsableInnerJSON(t *testing.T) {
	c := testChannel(t, "")
	rec := postForged(t, c, channels.HandlerFunc(func(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
		t.Error("handler must not run for unparsable json")
		return channels.OutboundMessage{}, nil
	}), `not-json{{`)
	if rec.Code != http.StatusOK || rec.Body.String() != "success" {
		t.Fatalf("unparsable inner json must be acked, got %d %q", rec.Code, rec.Body.String())
	}
}

// KF renders plain text only: markdown replies are stripped of markup before
// they ride the wire.
func TestSendMarkdownDowngrades(t *testing.T) {
	fake := &scriptKfAPI{}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := testChannel(t, srv.URL)

	if err := c.Send(t.Context(), channels.OutboundMessage{
		Channel: "wxkf", MsgID: "1", UserID: "oUSER1", Text: "**加粗** 与 `代码`", TextType: channels.TextTypeMarkdown,
	}); err != nil {
		t.Fatal(err)
	}
	sends := fake.sends()
	if len(sends) != 1 {
		t.Fatalf("want 1 send, got %d", len(sends))
	}
	var payload struct {
		MsgType string `json:"msgtype"`
		Text    struct {
			Content string `json:"content"`
		} `json:"text"`
	}
	if err := json.Unmarshal([]byte(sends[0]), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.MsgType != "text" || payload.Text.Content != "加粗 与 代码" {
		t.Fatalf("markdown must downgrade to plain text: %s", sends[0])
	}
}

// A platform rejection after some segments already landed surfaces as a
// partial-delivery error naming the failed segment, so the sender can retry.
func TestSendPartialDelivery(t *testing.T) {
	fake := &scriptKfAPI{onSend: func(n int) string {
		if n == 2 {
			return `{"errcode":95020,"errmsg":"outside the 48h window"}`
		}
		return `{"errcode":0,"errmsg":"ok"}`
	}}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := testChannel(t, srv.URL)

	long := strings.Repeat("汉", 1500) // 4500 bytes → 3 segments
	err := c.Send(t.Context(), channels.OutboundMessage{
		Channel: "wxkf", MsgID: "1", UserID: "oUSER1", Text: long,
	})
	if err == nil || !strings.Contains(err.Error(), "2/3") || !strings.Contains(err.Error(), "partial delivery") {
		t.Fatalf("second-segment failure must surface as partial delivery, got %v", err)
	}
	if n := len(fake.sends()); n != 2 {
		t.Fatalf("send must stop at the failing segment, got %d sends", n)
	}
}

// scriptKfAPI is a configurable fake of the KF HTTP API: each call can be
// answered by an injected body (no script entries answer success).
type scriptKfAPI struct {
	mu      sync.Mutex
	tokenN  int
	sendN   int
	onToken func(n int) string // 1-based call index → raw response body
	onSend  func(n int) string

	sendSeen []string
}

func (f *scriptKfAPI) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/cgi-bin/gettoken", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.tokenN++
		n := f.tokenN
		f.mu.Unlock()
		if f.onToken != nil {
			_, _ = fmt.Fprint(w, f.onToken(n))
			return
		}
		_, _ = w.Write([]byte(`{"errcode":0,"access_token":"tok","expires_in":7200}`))
	})
	mux.HandleFunc("/cgi-bin/kf/send_msg", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.sendN++
		n := f.sendN
		var body json.RawMessage
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.sendSeen = append(f.sendSeen, string(body))
		f.mu.Unlock()
		if f.onSend != nil {
			_, _ = fmt.Fprint(w, f.onSend(n))
			return
		}
		_, _ = w.Write([]byte(`{"errcode":0,"errmsg":"ok"}`))
	})
	return mux
}

func (f *scriptKfAPI) sends() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.sendSeen...)
}

func (f *scriptKfAPI) tokenCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tokenN
}

func TestSendTokenFailures(t *testing.T) {
	t.Run("unresolvable kf secret", func(t *testing.T) {
		c, err := New(Config{CorpID: testCorpID, KfAccount: testKfAccount, TokenRef: "tok", AESKeyRef: "aes", SecretRef: "nope"},
			mapResolver{"tok": testToken, "aes": testAESKey})
		if err != nil {
			t.Fatal(err)
		}
		err = c.Send(t.Context(), channels.OutboundMessage{Channel: "wxkf", MsgID: "1", UserID: "u", Text: "hi"})
		if err == nil || !strings.Contains(err.Error(), "resolve kf secret") {
			t.Fatalf("unresolvable kf secret must surface, got %v", err)
		}
	})

	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close()

	t.Run("gettoken unreachable", func(t *testing.T) {
		c := testChannel(t, deadURL)
		err := c.Send(t.Context(), channels.OutboundMessage{Channel: "wxkf", MsgID: "1", UserID: "u", Text: "hi"})
		if err == nil || !strings.Contains(err.Error(), "gettoken") {
			t.Fatalf("gettoken transport failure must surface, got %v", err)
		}
	})
	t.Run("gettoken bad json", func(t *testing.T) {
		fake := &scriptKfAPI{onToken: func(int) string { return `not-json` }}
		srv := httptest.NewServer(fake.handler())
		defer srv.Close()
		c := testChannel(t, srv.URL)
		err := c.Send(t.Context(), channels.OutboundMessage{Channel: "wxkf", MsgID: "1", UserID: "u", Text: "hi"})
		if err == nil || !strings.Contains(err.Error(), "gettoken decode") {
			t.Fatalf("gettoken decode failure must surface, got %v", err)
		}
	})
	t.Run("gettoken errcode", func(t *testing.T) {
		fake := &scriptKfAPI{onToken: func(int) string { return `{"errcode":40013,"errmsg":"invalid corpid"}` }}
		srv := httptest.NewServer(fake.handler())
		defer srv.Close()
		c := testChannel(t, srv.URL)
		err := c.Send(t.Context(), channels.OutboundMessage{Channel: "wxkf", MsgID: "1", UserID: "u", Text: "hi"})
		if err == nil || !strings.Contains(err.Error(), "errcode 40013") {
			t.Fatalf("gettoken rejection must surface, got %v", err)
		}
	})
	t.Run("short ttl is extended to an hour", func(t *testing.T) {
		// expires_in below the refresh margin would compute a non-positive TTL;
		// the token must still be cached (one fetch for two sends).
		fake := &scriptKfAPI{onToken: func(int) string {
			return `{"errcode":0,"access_token":"tok","expires_in":60}`
		}}
		srv := httptest.NewServer(fake.handler())
		defer srv.Close()
		c := testChannel(t, srv.URL)
		for i := 0; i < 2; i++ {
			if err := c.Send(t.Context(), channels.OutboundMessage{
				Channel: "wxkf", MsgID: fmt.Sprint(i), UserID: "oUSER1", Text: "hi",
			}); err != nil {
				t.Fatal(err)
			}
		}
		if n := fake.tokenCalls(); n != 1 {
			t.Fatalf("token must be cached despite the short TTL, got %d fetches", n)
		}
	})
	t.Run("unbuildable gettoken request", func(t *testing.T) {
		c := testChannel(t, "://bad")
		err := c.Send(t.Context(), channels.OutboundMessage{Channel: "wxkf", MsgID: "1", UserID: "u", Text: "hi"})
		if err == nil {
			t.Fatalf("unparseable API base must surface, got %v", err)
		}
	})
}

func TestSendPostMessageFailures(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close()

	t.Run("send unreachable", func(t *testing.T) {
		fake := &scriptKfAPI{}
		srv := httptest.NewServer(fake.handler())
		defer srv.Close()
		c := testChannel(t, srv.URL)
		if err := c.Send(t.Context(), channels.OutboundMessage{Channel: "wxkf", MsgID: "0", UserID: "oUSER1", Text: "warmup"}); err != nil {
			t.Fatal(err)
		}
		// Token is now cached; point the API at a dead endpoint.
		c.cfg.APIBase = deadURL
		err := c.Send(t.Context(), channels.OutboundMessage{Channel: "wxkf", MsgID: "1", UserID: "oUSER1", Text: "hi"})
		if err == nil || !strings.Contains(err.Error(), "send request") {
			t.Fatalf("send transport failure must surface, got %v", err)
		}
	})
	t.Run("unbuildable send request", func(t *testing.T) {
		fake := &scriptKfAPI{}
		srv := httptest.NewServer(fake.handler())
		defer srv.Close()
		c := testChannel(t, srv.URL)
		if err := c.Send(t.Context(), channels.OutboundMessage{Channel: "wxkf", MsgID: "0", UserID: "oUSER1", Text: "warmup"}); err != nil {
			t.Fatal(err)
		}
		c.cfg.APIBase = "://bad"
		err := c.Send(t.Context(), channels.OutboundMessage{Channel: "wxkf", MsgID: "1", UserID: "oUSER1", Text: "hi"})
		if err == nil {
			t.Fatalf("unparseable API base must surface, got %v", err)
		}
	})
	t.Run("send bad json", func(t *testing.T) {
		fake := &scriptKfAPI{onSend: func(int) string { return `not-json` }}
		srv := httptest.NewServer(fake.handler())
		defer srv.Close()
		c := testChannel(t, srv.URL)
		err := c.Send(t.Context(), channels.OutboundMessage{Channel: "wxkf", MsgID: "1", UserID: "oUSER1", Text: "hi"})
		if err == nil || !strings.Contains(err.Error(), "send decode") {
			t.Fatalf("send decode failure must surface, got %v", err)
		}
	})
	t.Run("token refresh then gettoken fails", func(t *testing.T) {
		fake := &scriptKfAPI{
			onToken: func(n int) string {
				if n == 1 {
					return `{"errcode":0,"access_token":"tok","expires_in":7200}`
				}
				return `{"errcode":40001,"errmsg":"invalid credential"}`
			},
			onSend: func(int) string { return `{"errcode":40014,"errmsg":"invalid access_token"}` },
		}
		srv := httptest.NewServer(fake.handler())
		defer srv.Close()
		c := testChannel(t, srv.URL)
		err := c.Send(t.Context(), channels.OutboundMessage{Channel: "wxkf", MsgID: "1", UserID: "oUSER1", Text: "hi"})
		if err == nil || !strings.Contains(err.Error(), "errcode 40001") {
			t.Fatalf("failure of the refreshed token fetch must surface, got %v", err)
		}
	})
	t.Run("token refresh then send fails", func(t *testing.T) {
		fake := &scriptKfAPI{onSend: func(n int) string {
			if n == 1 {
				return `{"errcode":40014,"errmsg":"invalid access_token"}`
			}
			return `not-json` // the retried send cannot be decoded either
		}}
		srv := httptest.NewServer(fake.handler())
		defer srv.Close()
		c := testChannel(t, srv.URL)
		err := c.Send(t.Context(), channels.OutboundMessage{Channel: "wxkf", MsgID: "1", UserID: "oUSER1", Text: "hi"})
		if err == nil || !strings.Contains(err.Error(), "send decode") {
			t.Fatalf("failure of the retried send must surface, got %v", err)
		}
	})
}
