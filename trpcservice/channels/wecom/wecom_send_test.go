package wecom

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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

// A mid-split failure makes the sender retry the whole reply from segment 1;
// the platform's touser+content duplicate check absorbs the already-delivered
// prefix segments, so message/send must ask for it.
func TestSendEnablesDuplicateCheck(t *testing.T) {
	fake := &scriptWecomAPI{}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := testChannel(t, srv.URL)

	if err := c.Send(t.Context(), channels.OutboundMessage{
		Channel: "wecom", MsgID: "1", UserID: "zhangsan", Text: "你好",
	}); err != nil {
		t.Fatal(err)
	}
	sends := fake.sends()
	if len(sends) != 1 {
		t.Fatalf("want 1 send, got %d", len(sends))
	}
	if !strings.Contains(sends[0], `"enable_duplicate_check":1`) ||
		!strings.Contains(sends[0], `"duplicate_check_interval":1800`) {
		t.Fatalf("message/send must request the platform duplicate check: %s", sends[0])
	}
}

// An empty reply must not reach the platform at all: no token fetch, no send,
// no error.
func TestSendEmptyTextSkipsAPI(t *testing.T) {
	fake := &scriptWecomAPI{}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := testChannel(t, srv.URL)

	for _, text := range []string{"", "   ", "\n\t"} {
		if err := c.Send(t.Context(), channels.OutboundMessage{
			Channel: "wecom", MsgID: "1", UserID: "zhangsan", Text: text,
		}); err != nil {
			t.Fatalf("empty reply must succeed silently, got %v", err)
		}
	}
	if n := fake.tokenCalls(); n != 0 {
		t.Fatalf("empty replies must not fetch a token, got %d fetches", n)
	}
	if n := len(fake.sends()); n != 0 {
		t.Fatalf("empty replies must not hit message/send, got %d sends", n)
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

// Refreshing an already-cached identity must reuse its LRU node: the old code
// pushed a second node, so the list grew unboundedly and an eviction popping
// the orphan deleted the LIVE token entry. Two refreshes of one identity keep
// exactly one node, and evictions afterwards never drop the refreshed identity.
func TestTokenRefreshReusesLRUNode(t *testing.T) {
	fake := &scriptWecomAPI{}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := testChannel(t, srv.URL)
	ctx := t.Context()

	get := func(corpID, secretRef string) {
		t.Helper()
		if _, err := c.getAccessToken(ctx, corpID, secretRef); err != nil {
			t.Fatal(err)
		}
	}
	expire := func(key string) {
		t.Helper()
		c.tokenMu.Lock()
		defer c.tokenMu.Unlock()
		e := c.tokens[key]
		e.expiry = time.Now().Add(-time.Minute)
		c.tokens[key] = e
	}
	state := func() (listLen, mapLen int) {
		t.Helper()
		c.tokenMu.Lock()
		defer c.tokenMu.Unlock()
		return c.tokenOrder.Len(), len(c.tokens)
	}
	cached := func(key string) bool {
		t.Helper()
		c.tokenMu.Lock()
		defer c.tokenMu.Unlock()
		_, ok := c.tokens[key]
		return ok
	}

	key := testCorpID + "|secret"
	get(testCorpID, "secret")
	expire(key)
	get(testCorpID, "secret") // refresh path
	if listLen, mapLen := state(); listLen != 1 || mapLen != 1 {
		t.Fatalf("refreshing one identity must keep one LRU node, got list=%d map=%d", listLen, mapLen)
	}

	// Fill the cache to capacity with other identities.
	for i := 0; i < maxTokenCacheEntries-1; i++ {
		get(fmt.Sprintf("corp-%d", i), "secret")
	}
	// Refreshing the original identity while full must not evict an innocent
	// one — the eviction loop only runs for a NEW identity.
	expire(key)
	get(testCorpID, "secret")
	if listLen, mapLen := state(); listLen != maxTokenCacheEntries || mapLen != maxTokenCacheEntries {
		t.Fatalf("a refresh must not grow or shrink the cache, got list=%d map=%d", listLen, mapLen)
	}
	if !cached("corp-0|secret") {
		t.Fatal("refreshing a cached identity at capacity must not evict another identity")
	}

	// One genuinely new identity evicts the LRU back (corp-0), never the
	// just-refreshed identity.
	get("corp-new", "secret")
	if listLen, mapLen := state(); listLen != maxTokenCacheEntries || mapLen != maxTokenCacheEntries {
		t.Fatalf("eviction must keep the cache bounded, got list=%d map=%d", listLen, mapLen)
	}
	if !cached(key) {
		t.Fatal("eviction must not drop the recently refreshed identity")
	}
	if cached("corp-0|secret") {
		t.Fatal("the least recently used identity must be the eviction victim")
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
