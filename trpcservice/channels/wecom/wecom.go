// Package wecom implements the WeCom (企业微信) Channel adapter.
//
// Inbound: the IM platform posts AES-encrypted XML callbacks; the adapter
// verifies the signature and decrypts via wxbizmsgcrypt (vendored at
// ./wxbizmsgcrypt), normalizes the message and hands it to the Handler, which
// acks within the 5-second window (sync ack + async consume, design 决策一).
//
// Outbound: replies go through the app message/send API (direct chats) or
// appchat/send (group chats), with the access_token cached in process and
// refreshed on expiry. Texts longer than the platform limit are split into
// sequential segments (design 5.3.2).
package wecom

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/sbzhu/weworkapi_golang/wxbizmsgcrypt"
	"go.uber.org/zap"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
)

// callbackPath is the webhook path mounted on the platform mux; it must match
// the channel_binding.webhook_path row for tenant routing.
const callbackPath = "/wecom/callback"

// defaultAPIBase is the WeCom API endpoint; overridable for tests.
const defaultAPIBase = "https://qyapi.weixin.qq.com"

// maxTextBytes is the platform limit for one text message (design 5.3.2);
// longer replies are split into sequential segments.
const maxTextBytes = 2048

// tokenExpiryMargin refreshes the access token ahead of its stated TTL.
const tokenExpiryMargin = 5 * time.Minute

// Config holds the WeCom channel configuration. Secret material is carried
// as references and resolved through the SecretResolver, never logged.
type Config struct {
	CorpID    string // 企业 ID (corpid)
	AgentID   int    // 应用 ID (agentid)
	TokenRef  string // callback token secret ref
	AESKeyRef string // EncodingAESKey secret ref
	SecretRef string // corpsecret secret ref (for access_token)
	APIBase   string // default https://qyapi.weixin.qq.com
}

// Channel is the WeCom implementation of channels.Channel.
type Channel struct {
	cfg    Config
	crypt  *wxbizmsgcrypt.WXBizMsgCrypt
	secret config.SecretResolver
	client *http.Client

	tokenMu     sync.Mutex
	accessToken string
	tokenExpiry time.Time
}

// New creates the channel: the callback token and AES key are resolved and
// validated at startup (fail fast on misconfiguration).
func New(cfg Config, resolver config.SecretResolver) (*Channel, error) {
	if cfg.CorpID == "" || cfg.AgentID == 0 {
		return nil, fmt.Errorf("wecom: CorpID and AgentID are required")
	}
	if cfg.APIBase == "" {
		cfg.APIBase = defaultAPIBase
	}
	ctx := context.Background()
	token, err := resolver.Resolve(ctx, cfg.TokenRef)
	if err != nil {
		return nil, fmt.Errorf("wecom: resolve token: %w", err)
	}
	aesKey, err := resolver.Resolve(ctx, cfg.AESKeyRef)
	if err != nil {
		return nil, fmt.Errorf("wecom: resolve aes key: %w", err)
	}
	return &Channel{
		cfg:    cfg,
		crypt:  wxbizmsgcrypt.NewWXBizMsgCrypt(token, aesKey, cfg.CorpID, wxbizmsgcrypt.XmlType),
		secret: resolver,
		client: &http.Client{Timeout: 10 * time.Second},
	}, nil
}

// Name implements channels.Channel.
func (c *Channel) Name() string { return "wecom" }

// RegisterRoutes implements channels.Channel: GET verifies the callback URL,
// POST receives encrypted messages.
func (c *Channel) RegisterRoutes(mux *http.ServeMux, h channels.Handler) {
	mux.HandleFunc(callbackPath, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			c.verifyURL(w, r)
		case http.MethodPost:
			c.receive(w, r, h)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

// verifyURL answers the platform's URL-registration challenge.
func (c *Channel) verifyURL(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	echo, cerr := c.crypt.VerifyURL(q.Get("msg_signature"), q.Get("timestamp"), q.Get("nonce"), q.Get("echostr"))
	if cerr != nil {
		plog.Warnf("wecom url verification failed: %s", cerr.ErrMsg)
		http.Error(w, "verification failed", http.StatusForbidden)
		return
	}
	_, _ = w.Write(echo)
}

// receive handles one encrypted message callback.
func (c *Channel) receive(w http.ResponseWriter, r *http.Request, h channels.Handler) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	q := r.URL.Query()
	plain, cerr := c.crypt.DecryptMsg(q.Get("msg_signature"), q.Get("timestamp"), q.Get("nonce"), body)
	if cerr != nil {
		// Signature/decryption failures are not retryable: ack so the
		// platform does not redeliver, and log for investigation.
		plog.Warnf("wecom decrypt failed: %s", cerr.ErrMsg)
		writeSuccess(w)
		return
	}

	var cm callbackMessage
	if err := xml.Unmarshal(plain, &cm); err != nil {
		plog.Warnf("wecom parse callback xml: %v", err)
		writeSuccess(w)
		return
	}
	// Only text messages enter the pipeline; events (enter_agent, ...) and
	// media-only messages are acked and skipped (media handling is a 5.3.2
	// follow-up). Text without a MsgId cannot be deduplicated — skip it too.
	if cm.MsgType != "text" || cm.MsgID == "" {
		writeSuccess(w)
		return
	}

	msg := channels.InboundMessage{
		Channel:     c.Name(),
		MsgID:       cm.MsgID,
		SessionKey:  channels.SessionKey(c.Name(), cm.FromUserName, cm.ChatID),
		UserID:      cm.FromUserName,
		ChatID:      cm.ChatID,
		Text:        cm.Content,
		WebhookPath: r.URL.Path,
		ReceivedAt:  time.Now(),
	}
	if _, err := h.Handle(r.Context(), msg); err != nil {
		// 5xx makes the platform redeliver; the gateway's dedup key absorbs
		// the retry (design 5.1.4).
		plog.Errorf("wecom handle msg %s: %v", cm.MsgID, err)
		http.Error(w, "handle error", http.StatusInternalServerError)
		return
	}
	writeSuccess(w)
}

// callbackMessage is the decrypted inner XML of a WeCom callback.
type callbackMessage struct {
	XMLName      xml.Name `xml:"xml"`
	FromUserName string   `xml:"FromUserName"`
	CreateTime   int64    `xml:"CreateTime"`
	MsgType      string   `xml:"MsgType"`
	Content      string   `xml:"Content"`
	MsgID        string   `xml:"MsgId"`
	AgentID      int      `xml:"AgentID"`
	ChatID       string   `xml:"ChatId"` // group chat ID; empty for direct chats
	Event        string   `xml:"Event"`
}

func writeSuccess(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain")
	_, _ = w.Write([]byte("success"))
}

// Send implements channels.Channel: direct chats go to message/send, group
// chats to appchat/send. Long texts are split into sequential segments within
// this one call, so concurrent senders cannot interleave segments of the same
// reply (design 5.3.2).
func (c *Channel) Send(ctx context.Context, msg channels.OutboundMessage) error {
	segments := splitText(msg.Text, maxTextBytes)
	for i, seg := range segments {
		if err := c.sendSegment(ctx, msg, seg); err != nil {
			if i > 0 {
				return fmt.Errorf("send segment %d/%d (partial delivery): %w", i+1, len(segments), err)
			}
			return err
		}
	}
	return nil
}

func (c *Channel) sendSegment(ctx context.Context, msg channels.OutboundMessage, text string) error {
	token, err := c.getAccessToken(ctx)
	if err != nil {
		return err
	}
	errcode, err := c.postMessage(ctx, token, msg, text)
	if err != nil {
		return err
	}
	if errcode == 40014 || errcode == 42001 { // token expired/invalid: refresh once and retry
		c.invalidateToken()
		if token, err = c.getAccessToken(ctx); err != nil {
			return err
		}
		errcode, err = c.postMessage(ctx, token, msg, text)
		if err != nil {
			return err
		}
	}
	if errcode != 0 {
		return fmt.Errorf("wecom send: errcode %d", errcode)
	}
	return nil
}

// postMessage calls the send API and returns the platform errcode.
func (c *Channel) postMessage(ctx context.Context, token string, msg channels.OutboundMessage, text string) (int, error) {
	var apiURL string
	var payload map[string]any
	if msg.ChatID != "" {
		apiURL = c.cfg.APIBase + "/cgi-bin/appchat/send?access_token=" + url.QueryEscape(token)
		payload = map[string]any{
			"chatid":  msg.ChatID,
			"msgtype": "text",
			"text":    map[string]string{"content": text},
		}
	} else {
		apiURL = c.cfg.APIBase + "/cgi-bin/message/send?access_token=" + url.QueryEscape(token)
		payload = map[string]any{
			"touser":  msg.UserID,
			"msgtype": "text",
			"agentid": c.cfg.AgentID,
			"text":    map[string]string{"content": text},
		}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("wecom send request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var result struct {
		ErrCode int    `json:"errcode"`
		ErrMsg  string `json:"errmsg"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("wecom send decode: %w", err)
	}
	if result.ErrCode != 0 {
		zap.L().Warn("wecom send rejected",
			zap.Int("errcode", result.ErrCode), zap.String("errmsg", result.ErrMsg))
	}
	return result.ErrCode, nil
}

// getAccessToken returns the cached token, refreshing it when expired.
func (c *Channel) getAccessToken(ctx context.Context) (string, error) {
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()
	if c.accessToken != "" && time.Now().Before(c.tokenExpiry) {
		return c.accessToken, nil
	}
	secret, err := c.secret.Resolve(ctx, c.cfg.SecretRef)
	if err != nil {
		return "", fmt.Errorf("wecom: resolve corpsecret: %w", err)
	}
	apiURL := c.cfg.APIBase + "/cgi-bin/gettoken?corpid=" + url.QueryEscape(c.cfg.CorpID) +
		"&corpsecret=" + url.QueryEscape(secret)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return "", err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("wecom gettoken: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var result struct {
		ErrCode     int    `json:"errcode"`
		ErrMsg      string `json:"errmsg"`
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("wecom gettoken decode: %w", err)
	}
	if result.ErrCode != 0 || result.AccessToken == "" {
		return "", fmt.Errorf("wecom gettoken: errcode %d", result.ErrCode)
	}
	ttl := time.Duration(result.ExpiresIn)*time.Second - tokenExpiryMargin
	if ttl <= 0 {
		ttl = time.Hour
	}
	c.accessToken = result.AccessToken
	c.tokenExpiry = time.Now().Add(ttl)
	return c.accessToken, nil
}

func (c *Channel) invalidateToken() {
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()
	c.accessToken = ""
	c.tokenExpiry = time.Time{}
}

// splitText breaks s into segments of at most n bytes, on rune boundaries.
func splitText(s string, n int) []string {
	if len(s) <= n {
		return []string{s}
	}
	var out []string
	for len(s) > n {
		cut := n
		for cut > 0 && (s[cut]&0xC0) == 0x80 { // don't split inside a UTF-8 sequence
			cut--
		}
		out = append(out, s[:cut])
		s = s[cut:]
	}
	return append(out, s)
}
