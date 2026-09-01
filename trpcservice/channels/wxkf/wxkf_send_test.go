package wxkf

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

// fakeKfAPI fakes gettoken and kf/send_msg for Send tests.
type fakeKfAPI struct {
	tokenCalls    atomic.Int32
	sendCalls     atomic.Int32
	lastBodies    []string
	failNextTok   bool // answer the next send with errcode 40014 (expired token)
	rejectErrcode int  // answer every send with this errcode (e.g. 95020 outside the 48h window)
}

func (f *fakeKfAPI) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/cgi-bin/gettoken", func(w http.ResponseWriter, r *http.Request) {
		f.tokenCalls.Add(1)
		if !strings.Contains(r.URL.RawQuery, "corpsecret=kf-secret") {
			_, _ = w.Write([]byte(`{"errcode":40001,"errmsg":"invalid secret"}`))
			return
		}
		_, _ = w.Write([]byte(`{"errcode":0,"access_token":"tok","expires_in":7200}`))
	})
	mux.HandleFunc("/cgi-bin/kf/send_msg", func(w http.ResponseWriter, r *http.Request) {
		f.sendCalls.Add(1)
		var body json.RawMessage
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.lastBodies = append(f.lastBodies, string(body))
		if f.failNextTok {
			f.failNextTok = false
			_, _ = w.Write([]byte(`{"errcode":40014,"errmsg":"invalid access_token"}`))
			return
		}
		if f.rejectErrcode != 0 {
			_, _ = w.Write([]byte(fmt.Sprintf(`{"errcode":%d,"errmsg":"rejected"}`, f.rejectErrcode)))
			return
		}
		_, _ = w.Write([]byte(`{"errcode":0,"errmsg":"ok"}`))
	})
	return mux
}

func TestSendCachesToken(t *testing.T) {
	fake := &fakeKfAPI{}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := testChannel(t, srv.URL)

	for i := 0; i < 2; i++ {
		if err := c.Send(t.Context(), channels.OutboundMessage{
			Channel: "wxkf", MsgID: fmt.Sprint(i), UserID: "oUSER1", Text: "你好",
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
	if !strings.Contains(fake.lastBodies[0], `"touser":"oUSER1"`) ||
		!strings.Contains(fake.lastBodies[0], `"open_kfid":"`+testKfAccount+`"`) ||
		!strings.Contains(fake.lastBodies[0], `"msgtype":"text"`) {
		t.Fatalf("bad send payload: %s", fake.lastBodies[0])
	}
}

func TestSendRefreshesExpiredToken(t *testing.T) {
	fake := &fakeKfAPI{failNextTok: true}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := testChannel(t, srv.URL)

	if err := c.Send(t.Context(), channels.OutboundMessage{
		Channel: "wxkf", MsgID: "1", UserID: "oUSER1", Text: "你好",
	}); err != nil {
		t.Fatal(err)
	}
	if n := fake.tokenCalls.Load(); n != 2 {
		t.Fatalf("expired token must trigger exactly one refresh, got %d fetches", n)
	}
}

func TestSendSplitsLongText(t *testing.T) {
	fake := &fakeKfAPI{}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := testChannel(t, srv.URL)

	long := strings.Repeat("汉", 1500) // 4500 bytes → 3 segments
	if err := c.Send(t.Context(), channels.OutboundMessage{
		Channel: "wxkf", MsgID: "1", UserID: "oUSER1", Text: long,
	}); err != nil {
		t.Fatal(err)
	}
	if n := fake.sendCalls.Load(); n != 3 {
		t.Fatalf("want 3 segments, got %d sends", n)
	}
	var joined strings.Builder
	msgids := map[string]bool{}
	for _, body := range fake.lastBodies {
		var payload struct {
			MsgID string `json:"msgid"`
			Text  struct {
				Content string `json:"content"`
			} `json:"text"`
		}
		if err := json.Unmarshal([]byte(body), &payload); err != nil {
			t.Fatal(err)
		}
		joined.WriteString(payload.Text.Content)
		if msgids[payload.MsgID] {
			t.Fatalf("segments must carry unique msgids, got %q twice", payload.MsgID)
		}
		msgids[payload.MsgID] = true
	}
	if joined.String() != long {
		t.Fatal("segments must reassemble into the original text")
	}
}

// Platform rejections (e.g. 95020 outside the 48h reply window) surface as
// ordinary send errors so the sender retries per its policy.
func TestSendErrorSurfaces(t *testing.T) {
	fake := &fakeKfAPI{rejectErrcode: 95020}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := testChannel(t, srv.URL)

	err := c.Send(t.Context(), channels.OutboundMessage{
		Channel: "wxkf", MsgID: "1", UserID: "oUSER1", Text: "你好",
	})
	if err == nil || !strings.Contains(err.Error(), "95020") {
		t.Fatalf("platform rejection must surface as an error, got %v", err)
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
