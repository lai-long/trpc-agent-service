package wecom

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

// fakeWeComAPI fakes gettoken and message/send for Send tests.
type fakeWeComAPI struct {
	tokenCalls  atomic.Int32
	sendCalls   atomic.Int32
	lastBodies  []string
	failNextTok bool // answer the next send with errcode 40014 (expired token)
}

func (f *fakeWeComAPI) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/cgi-bin/gettoken", func(w http.ResponseWriter, r *http.Request) {
		f.tokenCalls.Add(1)
		if !strings.Contains(r.URL.RawQuery, "corpsecret=corp-secret") {
			_, _ = w.Write([]byte(`{"errcode":40001,"errmsg":"invalid secret"}`))
			return
		}
		_, _ = w.Write([]byte(`{"errcode":0,"access_token":"tok","expires_in":7200}`))
	})
	mux.HandleFunc("/cgi-bin/message/send", func(w http.ResponseWriter, r *http.Request) {
		f.sendCalls.Add(1)
		var body json.RawMessage
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.lastBodies = append(f.lastBodies, string(body))
		if f.failNextTok {
			f.failNextTok = false
			_, _ = w.Write([]byte(`{"errcode":40014,"errmsg":"invalid access_token"}`))
			return
		}
		_, _ = w.Write([]byte(`{"errcode":0,"errmsg":"ok"}`))
	})
	mux.HandleFunc("/cgi-bin/appchat/send", func(w http.ResponseWriter, r *http.Request) {
		f.sendCalls.Add(1)
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["chatid"] == nil {
			_, _ = w.Write([]byte(`{"errcode":40013,"errmsg":"missing chatid"}`))
			return
		}
		_, _ = w.Write([]byte(`{"errcode":0,"errmsg":"ok"}`))
	})
	return mux
}

func TestSendCachesToken(t *testing.T) {
	fake := &fakeWeComAPI{}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := testChannel(t, srv.URL)

	for i := 0; i < 2; i++ {
		if err := c.Send(t.Context(), channels.OutboundMessage{
			Channel: "wecom", MsgID: fmt.Sprint(i), UserID: "zhangsan", Text: "你好",
		}); err != nil {
			t.Fatal(err)
		}
	}
	if n := fake.tokenCalls.Load(); n != 1 {
		t.Fatalf("token must be fetched once and cached, got %d fetches", n)
	}
	if n := fake.sendCalls.Load(); n != 2 {
		t.Fatalf("want 2 sends, got %d", n)
	}
	if !strings.Contains(fake.lastBodies[0], `"touser":"zhangsan"`) ||
		!strings.Contains(fake.lastBodies[0], `"agentid":1000002`) {
		t.Fatalf("bad send payload: %s", fake.lastBodies[0])
	}
}

func TestSendRefreshesExpiredToken(t *testing.T) {
	fake := &fakeWeComAPI{failNextTok: true}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := testChannel(t, srv.URL)

	if err := c.Send(t.Context(), channels.OutboundMessage{
		Channel: "wecom", MsgID: "1", UserID: "zhangsan", Text: "你好",
	}); err != nil {
		t.Fatal(err)
	}
	if n := fake.tokenCalls.Load(); n != 2 {
		t.Fatalf("expired token must trigger exactly one refresh, got %d fetches", n)
	}
}

func TestSendSplitsLongText(t *testing.T) {
	fake := &fakeWeComAPI{}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := testChannel(t, srv.URL)

	long := strings.Repeat("汉", 1500) // 4500 bytes → 3 segments
	if err := c.Send(t.Context(), channels.OutboundMessage{
		Channel: "wecom", MsgID: "1", UserID: "zhangsan", Text: long,
	}); err != nil {
		t.Fatal(err)
	}
	if n := fake.sendCalls.Load(); n != 3 {
		t.Fatalf("want 3 segments, got %d sends", n)
	}
	var joined strings.Builder
	for _, body := range fake.lastBodies {
		var payload struct {
			Text struct {
				Content string `json:"content"`
			} `json:"text"`
		}
		if err := json.Unmarshal([]byte(body), &payload); err != nil {
			t.Fatal(err)
		}
		joined.WriteString(payload.Text.Content)
	}
	if joined.String() != long {
		t.Fatal("segments must reassemble into the original text")
	}
}

func TestSendGroupUsesAppchat(t *testing.T) {
	fake := &fakeWeComAPI{}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := testChannel(t, srv.URL)

	if err := c.Send(t.Context(), channels.OutboundMessage{
		Channel: "wecom", MsgID: "1", ChatID: "roomA", Text: "群公告",
	}); err != nil {
		t.Fatal(err)
	}
	if n := fake.sendCalls.Load(); n != 1 {
		t.Fatalf("want 1 appchat send, got %d", n)
	}
}

func TestSplitTextRespectsRuneBoundaries(t *testing.T) {
	s := strings.Repeat("汉", 3) + strings.Repeat("a", 10)
	segs := splitText(s, 5)
	for _, seg := range segs {
		if len(seg) > 5 {
			t.Fatalf("segment too long: %d bytes", len(seg))
		}
	}
	if strings.Join(segs, "") != s {
		t.Fatal("segments must reassemble losslessly")
	}
}
