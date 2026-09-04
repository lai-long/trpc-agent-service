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

// maxCryptCacheEntries bounds the value-keyed crypt cache against unbounded
// growth under repeated key rotation.
const maxCryptCacheEntries = 32

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
	secret config.SecretResolver
	client *http.Client

	// crypts caches one WXBizMsgCrypt per credential set
	// (corp|tokenRef|aesKeyRef): multi-tenant callbacks arrive with
	// per-binding refs (design 5.3.1) and each verification needs the
	// matching crypt. Ref resolution rides the process-level cached
	// resolver, so rotation propagates within the cache TTL.
	cryptMu sync.Mutex
	crypts  map[string]*wxbizmsgcrypt.WXBizMsgCrypt

	tokenMu     sync.Mutex
	accessToken string
	tokenExpiry time.Time
}

// New creates the channel: the env callback token and AES key are resolved
// and validated at startup (fail fast on misconfiguration). They remain the
// single-binding default for the legacy /wxkf/callback path; per-binding
// credentials flow through CallbackHandler.
func New(cfg Config, resolver config.SecretResolver) (*Channel, error) {
	if cfg.CorpID == "" || cfg.KfAccount == "" {
		return nil, fmt.Errorf("wxkf: CorpID and KfAccount are required")
	}
	if cfg.APIBase == "" {
		cfg.APIBase = defaultAPIBase
	}
	ctx := context.Background()
	if _, err := resolver.Resolve(ctx, cfg.TokenRef); err != nil {
		return nil, fmt.Errorf("wxkf: resolve token: %w", err)
	}
	if _, err := resolver.Resolve(ctx, cfg.AESKeyRef); err != nil {
		return nil, fmt.Errorf("wxkf: resolve aes key: %w", err)
	}
	return &Channel{
		cfg:    cfg,
		secret: resolver,
		client: &http.Client{Timeout: 10 * time.Second},
		crypts: map[string]*wxbizmsgcrypt.WXBizMsgCrypt{},
	}, nil
}

// cryptFor resolves the credential set and returns its crypt. The cache is
// keyed by the RESOLVED values (not the refs): rotation behind a ref
// propagates within the secret resolver's cache TTL instead of living in a
// ref-keyed cache until process restart. Empty refs fall back to the
// env-configured single-binding default — the legacy path's privilege; the
// dispatcher refuses binding-scoped callbacks with empty refs.
func (c *Channel) cryptFor(corpID, tokenRef, aesKeyRef string) (*wxbizmsgcrypt.WXBizMsgCrypt, error) {
	if corpID == "" {
		corpID = c.cfg.CorpID
	}
	if tokenRef == "" {
		tokenRef = c.cfg.TokenRef
	}
	if aesKeyRef == "" {
		aesKeyRef = c.cfg.AESKeyRef
	}
	ctx := context.Background()
	token, err := c.secret.Resolve(ctx, tokenRef)
	if err != nil {
		return nil, fmt.Errorf("wxkf: resolve token %q: %w", tokenRef, err)
	}
	aesKey, err := c.secret.Resolve(ctx, aesKeyRef)
	if err != nil {
		return nil, fmt.Errorf("wxkf: resolve aes key %q: %w", aesKeyRef, err)
	}
	key := corpID + "|" + token + "|" + aesKey
	c.cryptMu.Lock()
	defer c.cryptMu.Unlock()
	if crypt, ok := c.crypts[key]; ok {
		return crypt, nil
	}
	if len(c.crypts) >= maxCryptCacheEntries {
		c.crypts = map[string]*wxbizmsgcrypt.WXBizMsgCrypt{}
	}
	crypt := wxbizmsgcrypt.NewWXBizMsgCrypt(token, aesKey, corpID, wxbizmsgcrypt.XmlType)
	c.crypts[key] = crypt
	return crypt, nil
}

// Name implements channels.Channel.
func (c *Channel) Name() string { return "wxkf" }

// RegisterRoutes implements channels.Channel: GET verifies the callback URL,
// POST receives encrypted messages. The path is the env-configured
// single-binding default (design 5.3.1); tenant bindings are served through
// CallbackHandler at /callback/{channel}/{binding_id}.
func (c *Channel) RegisterRoutes(mux *http.ServeMux, h channels.Handler) {
	handler, err := c.CallbackHandler(h, channels.BindingCredentials{
		CorpID:    c.cfg.CorpID,
		TokenRef:  c.cfg.TokenRef, // the env single-binding default, explicitly
		AESKeyRef: c.cfg.AESKeyRef,
	})
	if err != nil {
		// New already validated the env refs, so this is defensive: mount
		// nothing and let the path 404 instead of half-serving.
		plog.Errorf("wxkf callback mount failed: %v", err)
		return
	}
	mux.HandleFunc(http.MethodGet+" "+callbackPath, func(w http.ResponseWriter, r *http.Request) {
		handler(w, r)
	})
	mux.HandleFunc(http.MethodPost+" "+callbackPath, handler)
}

// CallbackHandler implements channels.BindingAware. A binding-scoped callback
// must verify under the binding's OWN credentials: empty refs are an error
// here (the dispatcher answers 503), because falling back to the env-global
// keys would let whoever holds them forge this tenant's callbacks. The
// legacy env path passes its refs explicitly via RegisterRoutes.
func (c *Channel) CallbackHandler(h channels.Handler, creds channels.BindingCredentials) (http.HandlerFunc, error) {
	if creds.TokenRef == "" || creds.AESKeyRef == "" {
		return nil, fmt.Errorf("wxkf: binding %s lacks callback credentials", creds.BindingID)
	}
	crypt, err := c.cryptFor(creds.CorpID, creds.TokenRef, creds.AESKeyRef)
	if err != nil {
		return nil, err
	}
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			c.verifyURL(w, r, crypt)
		case http.MethodPost:
			c.receive(w, r, crypt, h)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}, nil
}

// verifyURL answers the platform's URL-registration challenge.
func (c *Channel) verifyURL(w http.ResponseWriter, r *http.Request, crypt *wxbizmsgcrypt.WXBizMsgCrypt) {
	q := r.URL.Query()
	echo, cerr := crypt.VerifyURL(q.Get("msg_signature"), q.Get("timestamp"), q.Get("nonce"), q.Get("echostr"))
	if cerr != nil {
		plog.Warnf("wxkf url verification failed: %s", cerr.ErrMsg)
		http.Error(w, "verification failed", http.StatusForbidden)
		return
	}
	_, _ = w.Write(echo)
}

// receive handles one encrypted message callback.
func (c *Channel) receive(w http.ResponseWriter, r *http.Request, crypt *wxbizmsgcrypt.WXBizMsgCrypt, h channels.Handler) {
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
	plain, cerr := crypt.DecryptMsg(q.Get("msg_signature"), q.Get("timestamp"), q.Get("nonce"), xmlEnvelope(envelope.Encrypt))
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
		return 0, fmt.Errorf("wxkf send request: %w", channels.ScrubError(err))
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

// sendMsgID builds the msgid send_msg requires. It must be STABLE across
// retries: the platform dedups send_msg by msgid, so a retry after "request
// sent but response lost" is absorbed by WeChat instead of double-delivering
// to the user (review P1-7). Only the segment index varies, distinguishing
// the pieces of one split reply.
func sendMsgID(msg channels.OutboundMessage, seg int) string {
	return fmt.Sprintf("%s-%d", msg.MsgID, seg)
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
		return "", fmt.Errorf("wxkf gettoken: %w", channels.ScrubError(err))
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
