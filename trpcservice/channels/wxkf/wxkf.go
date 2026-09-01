// Package wxkf implements the WeChat KF (微信客服) Channel adapter.
//
// Inbound: the IM platform posts AES-encrypted JSON callbacks
// ({"encrypt": "..."}); the adapter verifies the msg_signature and decrypts
// via wxbizmsgcrypt (vendored at ./wxbizmsgcrypt — same algorithm family as
// WeCom, different envelope format), normalizes the message and hands it to
// the Handler. GET callbacks carry the URL-verification challenge (echostr).
// Unlike WeCom there is no 5-second reply window; the async chain (design
// 决策一) is shared unchanged.
//
// Outbound: replies go through the customer-service send_msg API — the only
// reply path, allowed only within 48 hours of the user's last message; window
// violations surface as ordinary send errors for the sender to retry
// (design 5.3.1). The access_token (exchanged with the KF-specific secret, not
// the WeCom corpsecret) is cached in process and refreshed on expiry. Texts
// longer than the platform limit are split into sequential segments (5.3.2).
package wxkf

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
const callbackPath = "/wxkf/callback"

// defaultAPIBase is the WeCom API endpoint the KF APIs hang under; overridable
// for tests.
const defaultAPIBase = "https://qyapi.weixin.qq.com"

// maxTextBytes is the platform limit for one text message (design 5.3.2);
// longer replies are split into sequential segments.
const maxTextBytes = 2048

// tokenExpiryMargin refreshes the access token ahead of its stated TTL.
const tokenExpiryMargin = 5 * time.Minute

// Config holds the WeChat KF channel configuration. Secret material is
// carried as references and resolved through the SecretResolver, never logged.
type Config struct {
	CorpID    string // 企业 ID (corpid); the KF account lives under this corp
	KfAccount string // 客服账号 (open_kfid)
	TokenRef  string // callback token secret ref
	AESKeyRef string // EncodingAESKey secret ref
	SecretRef string // KF secret secret ref (for access_token; NOT the corpsecret)
	APIBase   string // default https://qyapi.weixin.qq.com
}

// Channel is the WeChat KF implementation of channels.Channel.
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
	if cfg.CorpID == "" || cfg.KfAccount == "" {
		return nil, fmt.Errorf("wxkf: CorpID and KfAccount are required")
	}
	if cfg.APIBase == "" {
		cfg.APIBase = defaultAPIBase
	}
	ctx := context.Background()
	token, err := resolver.Resolve(ctx, cfg.TokenRef)
	if err != nil {
		return nil, fmt.Errorf("wxkf: resolve token: %w", err)
	}
	aesKey, err := resolver.Resolve(ctx, cfg.AESKeyRef)
	if err != nil {
		return nil, fmt.Errorf("wxkf: resolve aes key: %w", err)
	}
	return &Channel{
		cfg:    cfg,
		crypt:  wxbizmsgcrypt.NewWXBizMsgCrypt(token, aesKey, cfg.CorpID, wxbizmsgcrypt.XmlType),
		secret: resolver,
		client: &http.Client{Timeout: 10 * time.Second},
	}, nil
}

// Name implements channels.Channel.
func (c *Channel) Name() string { return "wxkf" }

// RegisterRoutes implements channels.Channel: GET verifies the callback URL,
// POST receives encrypted messages.
func (c *Channel) RegisterRoutes(mux *http.ServeMux, h channels.Handler) {
	mux.HandleFunc(http.MethodGet+" "+callbackPath, c.verifyURL)
	mux.HandleFunc(http.MethodPost+" "+callbackPath, func(w http.ResponseWriter, r *http.Request) {
		c.receive(w, r, h)
	})
}

// verifyURL answers the platform's URL-registration challenge.
func (c *Channel) verifyURL(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	echo, cerr := c.crypt.VerifyURL(q.Get("msg_signature"), q.Get("timestamp"), q.Get("nonce"), q.Get("echostr"))
	if cerr != nil {
		plog.Warnf("wxkf url verification failed: %s", cerr.ErrMsg)
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
	var envelope struct {
		Encrypt string `json:"encrypt"`
	}
	// A malformed callback is not retryable: ack so the platform does not
	// redeliver, and log for investigation.
	if err := json.Unmarshal(body, &envelope); err != nil {
		plog.Warnf("wxkf bad callback json: %v", err)
		writeSuccess(w)
		return
	}
	if envelope.Encrypt == "" {
		plog.Warnf("wxkf callback missing encrypt field")
		writeSuccess(w)
		return
	}
	q := r.URL.Query()
	plain, cerr := c.crypt.DecryptMsg(q.Get("msg_signature"), q.Get("timestamp"), q.Get("nonce"), xmlEnvelope(envelope.Encrypt))
	if cerr != nil {
		// Signature/decryption failures are not retryable: ack so the
		// platform does not redeliver, and log for investigation.
		plog.Warnf("wxkf decrypt failed: %s", cerr.ErrMsg)
		writeSuccess(w)
		return
	}

	var cm callbackMessage
	if err := json.Unmarshal(plain, &cm); err != nil {
		plog.Warnf("wxkf parse callback json: %v", err)
		writeSuccess(w)
		return
	}
	// Only text messages enter the pipeline; events (enter_session, ...) and
	// media messages (image/voice/file/link/miniprogram) are acked and skipped
	// (media handling is a 5.3.2 follow-up). Text without a msgid cannot be
	// deduplicated — skip it too.
	if cm.MsgType != "text" || cm.MsgID == "" {
		plog.Infof("wxkf skip msgtype=%s msgid=%s (non-text/event or missing msgid)", cm.MsgType, cm.MsgID)
		writeSuccess(w)
		return
	}

	msg := channels.InboundMessage{
		Channel:     c.Name(),
		MsgID:       cm.MsgID,
		SessionKey:  channels.SessionKey(c.Name(), cm.OpenID, ""), // KF is direct-chat only
		UserID:      cm.OpenID,
		Text:        cm.Text.Content,
		WebhookPath: r.URL.Path,
		ReceivedAt:  time.Now(),
	}
	if _, err := h.Handle(r.Context(), msg); err != nil {
		if errors.Is(err, channels.ErrDuplicate) {
			// ErrDuplicate is a success outcome, not a failure: answer 200 so
			// the platform stops redelivering (see the Handler contract).
			plog.Warnf("wxkf duplicate message %s dropped", cm.MsgID)
			writeSuccess(w)
			return
		}
		// 5xx makes the platform redeliver; the gateway rolls the dedup key
		// back first so that retry is not swallowed (design 5.1.4).
		plog.Errorf("wxkf handle msg %s: %v", cm.MsgID, err)
		http.Error(w, "handle error", http.StatusInternalServerError)
		return
	}
	writeSuccess(w)
}

// callbackMessage is the decrypted inner JSON of a WeChat KF callback.
type callbackMessage struct {
	MsgID      string `json:"msgid"`
	OpenID     string `json:"openid"`
	MsgType    string `json:"msgtype"`
	CreateTime int64  `json:"create_time"`
	Text       struct {
		Content string `json:"content"`
	} `json:"text"`
}

// xmlEnvelope re-wraps the JSON ciphertext into the XML envelope the vendored
// wxbizmsgcrypt parses (base64 carries no XML-special characters). This keeps
// signature verification and AES decryption inside the vendored library,
// shared with the WeCom adapter.
func xmlEnvelope(encrypt string) []byte {
	return []byte("<xml><Encrypt>" + encrypt + "</Encrypt></xml>")
}

func writeSuccess(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain")
	_, _ = w.Write([]byte("success"))
}

// Send implements channels.Channel: KF is direct-chat only, so every reply
// goes to kf/send_msg. Long texts are split into sequential segments within
// this one call, so concurrent senders cannot interleave segments of the same
// reply (design 5.3.2). KF renders plain text only — markdown replies are
// downgraded (通道级渲染降级).
func (c *Channel) Send(ctx context.Context, msg channels.OutboundMessage) error {
	if msg.TextType == channels.TextTypeMarkdown {
		msg.Text = channels.RenderPlain(msg.Text)
	}
	segments := splitText(msg.Text, maxTextBytes)
	for i, seg := range segments {
		if err := c.sendSegment(ctx, msg, i, seg); err != nil {
			if i > 0 {
				return fmt.Errorf("send segment %d/%d (partial delivery): %w", i+1, len(segments), err)
			}
			return err
		}
	}
	return nil
}

func (c *Channel) sendSegment(ctx context.Context, msg channels.OutboundMessage, seg int, text string) error {
	token, err := c.getAccessToken(ctx)
	if err != nil {
		return err
	}
	errcode, err := c.postMessage(ctx, token, msg, seg, text)
	if err != nil {
		return err
	}
	if errcode == 40014 || errcode == 42001 { // token expired/invalid: refresh once and retry
		c.invalidateToken()
		if token, err = c.getAccessToken(ctx); err != nil {
			return err
		}
		errcode, err = c.postMessage(ctx, token, msg, seg, text)
		if err != nil {
			return err
		}
	}
	if errcode != 0 {
		// Platform rejections (e.g. 95020 outside the 48h window) are ordinary
		// errors: propagate and let the sender retry per its policy.
		return fmt.Errorf("wxkf send: errcode %d", errcode)
	}
	return nil
}

// postMessage calls the send_msg API and returns the platform errcode.
func (c *Channel) postMessage(ctx context.Context, token string, msg channels.OutboundMessage, seg int, text string) (int, error) {
	apiURL := c.cfg.APIBase + "/cgi-bin/kf/send_msg?access_token=" + url.QueryEscape(token)
	payload := map[string]any{
		"touser":    msg.UserID,
		"open_kfid": c.cfg.KfAccount,
		"msgid":     sendMsgID(msg, seg),
		"msgtype":   "text",
		"text":      map[string]string{"content": text},
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
		return 0, fmt.Errorf("wxkf send request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var result struct {
		ErrCode int    `json:"errcode"`
		ErrMsg  string `json:"errmsg"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("wxkf send decode: %w", err)
	}
	if result.ErrCode != 0 {
		zap.L().Warn("wxkf send rejected",
			zap.Int("errcode", result.ErrCode), zap.String("errmsg", result.ErrMsg))
	}
	return result.ErrCode, nil
}

// sendMsgID builds the unique msgid send_msg requires: the inbound MsgID keeps
// it traceable, nanotime + segment index keep it unique across segments and
// retries.
func sendMsgID(msg channels.OutboundMessage, seg int) string {
	return fmt.Sprintf("%s-%d-%d", msg.MsgID, time.Now().UnixNano(), seg)
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
		return "", fmt.Errorf("wxkf: resolve kf secret: %w", err)
	}
	apiURL := c.cfg.APIBase + "/cgi-bin/gettoken?corpid=" + url.QueryEscape(c.cfg.CorpID) +
		"&corpsecret=" + url.QueryEscape(secret)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return "", err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("wxkf gettoken: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var result struct {
		ErrCode     int    `json:"errcode"`
		ErrMsg      string `json:"errmsg"`
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("wxkf gettoken decode: %w", err)
	}
	if result.ErrCode != 0 || result.AccessToken == "" {
		return "", fmt.Errorf("wxkf gettoken: errcode %d", result.ErrCode)
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
// Deliberately a local copy of the WeCom rule: the two adapters evolve on
// different platform limits and must not couple.
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
