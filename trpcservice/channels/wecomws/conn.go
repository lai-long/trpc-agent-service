package wecomws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
)

// credentialError marks a subscribe rejection (errcode != 0): a bad secret
// never heals by spinning, so its backoff grows toward a higher cap.
type credentialError struct{ errcode int }

func (e *credentialError) Error() string {
	return fmt.Sprintf("wecomws: subscribe rejected (errcode %d)", e.errcode)
}

// kickedError marks disconnected_event: the platform kicked this connection
// because a newer subscription took over (leader handover).
type kickedError struct{}

func (*kickedError) Error() string { return "wecomws: disconnected by platform" }

// botConn owns one bot's connection lifecycle: dial → subscribe → heartbeat +
// read loop → jittered reconnect. run returns only when its ctx is canceled;
// every other failure reconnects internally with backoff.
type botConn struct {
	parent  *Channel
	binding Binding
	cfg     bindingConfig

	cancel func()
	done   chan struct{}

	mu      sync.Mutex
	conn    *websocket.Conn
	pingOut string // req_id of the heartbeat awaiting its pong
}

// run owns the connection until ctx is canceled, reconnecting on every
// failure with the backoff its error class earns.
func (c *botConn) run(ctx context.Context, h channels.Handler) {
	defer close(c.done)
	wait := time.Duration(0)
	for ctx.Err() == nil {
		started := time.Now()
		err := c.serve(ctx, h)
		if ctx.Err() != nil {
			return
		}
		// A connection that lived past the reconnect cap was healthy: its
		// successor's failures start a fresh backoff chain instead of
		// inheriting one.
		if time.Since(started) > c.parent.reconnectCap {
			wait = 0
		}
		wait = c.nextWait(err, wait)
		plog.Warnf("wecomws bot %s disconnected, reconnecting in ~%s: %v", c.cfg.BotID, wait, err)
		if !sleepJitter(ctx, wait) {
			return
		}
	}
}

// nextWait advances the reconnect backoff: the first failure waits exactly
// its class base, repeats double toward the class cap, and a platform kick
// always restarts at its base (a kick is the expected, rare leader handover
// and must not inherit a stale chain).
func (c *botConn) nextWait(err error, prev time.Duration) time.Duration {
	var kicked *kickedError
	if errors.As(err, &kicked) {
		return c.parent.kickedBase
	}
	limit := c.parent.reconnectCap
	var cred *credentialError
	if errors.As(err, &cred) {
		limit = c.parent.credentialCap
	}
	if prev < c.parent.reconnectBase {
		return c.parent.reconnectBase
	}
	return min(prev*2, limit)
}

// serve runs one connection attempt to completion: resolve the secret
// (rotation takes effect on the next reconnect), dial, subscribe, then
// heartbeat + read until something fails.
func (c *botConn) serve(ctx context.Context, h channels.Handler) error {
	// connCtx is this connection attempt's lifetime. The heartbeat kill and
	// any read failure cancel it, so work parked on this connection (an
	// inbound Handle retry, which would otherwise starve the read loop of
	// pongs forever) wakes up and serve returns — run() reconnects instead
	// of babysitting a dead socket.
	connCtx, cancelConn := context.WithCancel(ctx)
	defer cancelConn()

	secret, err := c.parent.secret.Resolve(ctx, c.cfg.SecretRef)
	if err != nil {
		return fmt.Errorf("wecomws: resolve bot secret: %w", err)
	}
	ws, _, err := websocket.Dial(ctx, c.parent.addr, nil)
	if err != nil {
		return fmt.Errorf("wecomws: dial: %w", err)
	}
	defer func() {
		c.clearConn(ws)
		_ = ws.CloseNow()
	}()

	subReqID, err := newReqID()
	if err != nil {
		return err
	}
	subFrame, err := subscribeFrame(c.cfg.BotID, secret, subReqID)
	if err != nil {
		return err
	}
	if err := c.writeTo(ctx, ws, subFrame); err != nil {
		return fmt.Errorf("wecomws: send subscribe: %w", err)
	}

	// Wait for the subscribe ack; callbacks racing ahead of it are buffered
	// and dispatched after. No ack inside the window: tear down and retry.
	subCtx, cancelSub := context.WithTimeout(connCtx, c.parent.subscribeTimeout)
	defer cancelSub()
	var pending []envelope
	acked := false
	for !acked {
		env, rerr := readEnvelope(subCtx, ws)
		if rerr != nil {
			if errors.Is(rerr, errBadFrame) {
				plog.Warnf("wecomws bot %s: %v", c.cfg.BotID, rerr)
				continue
			}
			return rerr
		}
		switch {
		case env.Headers.ReqID == subReqID || env.Cmd == cmdSubscribe:
			var ack errcodeBody
			if len(env.Body) > 0 {
				if uerr := json.Unmarshal(env.Body, &ack); uerr != nil {
					return fmt.Errorf("wecomws: subscribe ack body: %w", uerr)
				}
			}
			if ack.ErrCode != 0 {
				return &credentialError{errcode: ack.ErrCode}
			}
			acked = true
		case env.Cmd == cmdMsgCallback || env.Cmd == cmdEventCallback:
			if len(pending) < pendingBufferMax {
				pending = append(pending, env)
			} else {
				plog.Warnf("wecomws bot %s: dropping pre-ack callback (buffer full)", c.cfg.BotID)
			}
		default:
			plog.Warnf("wecomws bot %s: ignoring pre-ack frame cmd %q", c.cfg.BotID, env.Cmd)
		}
	}

	// Published only after the ack: until then Send sees "no live
	// connection" (the sender's PEL retries) instead of writing respond
	// frames the platform silently drops on an un-subscribed socket.
	c.setConn(ws)
	go c.heartbeat(connCtx, ws, cancelConn)

	for _, env := range pending {
		if err := c.handleFrame(connCtx, h, env); err != nil {
			return err
		}
	}
	return c.readLoop(connCtx, h, ws)
}

// heartbeat pings every interval and kills the connection when the previous
// ping got no matching pong within one interval. It exits silently once its
// connection is no longer the current one (reconnected or closed).
func (c *botConn) heartbeat(ctx context.Context, ws *websocket.Conn, kill func()) {
	ticker := time.NewTicker(c.parent.pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		c.mu.Lock()
		if c.conn != ws {
			c.mu.Unlock()
			return
		}
		if c.pingOut != "" {
			// The previous heartbeat got no answer within one interval: the
			// connection is half-dead — kill it AND cancel connCtx so
			// anything parked on this connection wakes up and run()
			// reconnects.
			c.mu.Unlock()
			plog.Warnf("wecomws bot %s: heartbeat unanswered for %s, closing connection", c.cfg.BotID, c.parent.pingInterval)
			kill()
			_ = ws.CloseNow()
			return
		}
		reqID, err := newReqID()
		if err == nil {
			var data []byte
			data, err = json.Marshal(pingFrame(reqID))
			if err == nil {
				err = c.writeLocked(ctx, data)
				if err == nil {
					c.pingOut = reqID
				}
			}
		}
		c.mu.Unlock()
		if err != nil {
			// Without a ping on the wire the connection is unmonitored: tear
			// it down like the unanswered-ping path so run() reconnects.
			plog.Warnf("wecomws bot %s: heartbeat failed (%v), closing connection", c.cfg.BotID, err)
			kill()
			_ = ws.CloseNow()
			return
		}
	}
}

// readLoop consumes frames until a connection error (or a platform kick).
func (c *botConn) readLoop(ctx context.Context, h channels.Handler, ws *websocket.Conn) error {
	for {
		env, err := readEnvelope(ctx, ws)
		if err != nil {
			if errors.Is(err, errBadFrame) {
				plog.Warnf("wecomws bot %s: %v", c.cfg.BotID, err)
				continue
			}
			return err
		}
		switch env.Cmd {
		case cmdPong:
			c.mu.Lock()
			if env.Headers.ReqID != "" && env.Headers.ReqID == c.pingOut {
				c.pingOut = ""
			}
			c.mu.Unlock()
		case cmdMsgCallback, cmdEventCallback:
			if err := c.handleFrame(ctx, h, env); err != nil {
				return err
			}
		default:
			plog.Warnf("wecomws bot %s: ignoring frame cmd %q", c.cfg.BotID, env.Cmd)
		}
	}
}

// handleFrame routes one callback frame; a kickedError propagates as a
// disconnect. Message callbacks never fail the connection: WS inbound has no
// platform redelivery, so Handle failures retry locally on a capped backoff
// until they land (duplicates are success — the message was seen before).
func (c *botConn) handleFrame(ctx context.Context, h channels.Handler, env envelope) error {
	if env.Cmd == cmdEventCallback {
		return c.handleEvent(env)
	}
	msg, err := c.normalize(env)
	if err != nil {
		plog.Warnf("wecomws bot %s: dropping callback (req_id %s): %v", c.cfg.BotID, env.Headers.ReqID, err)
		return nil
	}
	wait := c.parent.inboundRetryBase
	for {
		if _, err := h.Handle(ctx, msg); err == nil || errors.Is(err, channels.ErrDuplicate) {
			return nil
		} else if ctx.Err() != nil {
			return nil
		} else {
			plog.Errorf("wecomws bot %s handle msg %s: %v (retry in %s)", c.cfg.BotID, msg.MsgID, err, wait)
		}
		if !sleep(ctx, wait) {
			return nil
		}
		wait = min(wait*2, c.parent.inboundRetryCap)
	}
}

// handleEvent maps platform events; only the disconnect is a connection
// event. enter_chat / template_card_event are out of scope.
func (c *botConn) handleEvent(env envelope) error {
	var ev eventCallback
	if len(env.Body) > 0 {
		if err := json.Unmarshal(env.Body, &ev); err != nil {
			plog.Warnf("wecomws bot %s: bad event frame: %v", c.cfg.BotID, err)
			return nil
		}
	}
	switch ev.eventType() {
	case eventDisconnected:
		return &kickedError{}
	case eventEnterChat, eventTemplateCard:
		plog.Infof("wecomws bot %s: event %s skipped (out of scope)", c.cfg.BotID, ev.eventType())
	default:
		plog.Infof("wecomws bot %s: event %q skipped", c.cfg.BotID, ev.eventType())
	}
	return nil
}

// normalize turns one message callback into the platform-normalized message,
// mirroring the wecom adapter's inbound shape. Group chats carry the chat id
// (session key becomes group:), media messages degrade to a placeholder text.
func (c *botConn) normalize(env envelope) (channels.InboundMessage, error) {
	var body msgCallback
	if err := json.Unmarshal(env.Body, &body); err != nil {
		return channels.InboundMessage{}, fmt.Errorf("parse callback body: %w", err)
	}
	if body.MsgID == "" {
		return channels.InboundMessage{}, errors.New("callback without msgid")
	}
	if body.From.UserID == "" {
		return channels.InboundMessage{}, errors.New("callback without from.userid")
	}
	chatID := ""
	if body.ChatType == "group" {
		chatID = body.ChatID
	}
	text := body.Text.Content
	if body.MsgType != "text" {
		text = mediaPlaceholder(body.MsgType)
	}
	return channels.InboundMessage{
		Channel:     c.parent.Name(),
		MsgID:       body.MsgID,
		SessionKey:  channels.SessionKey(c.parent.Name(), body.From.UserID, chatID),
		UserID:      body.From.UserID,
		ChatID:      chatID,
		Text:        text,
		Type:        channels.TypeText,
		WebhookPath: c.binding.WebhookPath,
		BindingID:   c.binding.ID,
		ReplyToken:  env.Headers.ReqID,
		ReceivedAt:  time.Now(),
	}, nil
}

// setConn publishes the connection the read loop just dialed.
func (c *botConn) setConn(ws *websocket.Conn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.conn = ws
	c.pingOut = ""
}

// clearConn drops the connection on teardown, unless a newer one took over.
func (c *botConn) clearConn(ws *websocket.Conn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == ws {
		c.conn = nil
		c.pingOut = ""
	}
}

// writeLocked marshals and sends one frame on the current connection; the
// caller holds c.mu, the serializer shared by heartbeat and Send.
func (c *botConn) writeLocked(ctx context.Context, data []byte) error {
	return c.writeLockedOn(ctx, c.conn, data)
}

// writeLockedOn sends one pre-marshaled frame on ws under a bounded write
// timeout; the caller holds c.mu.
func (c *botConn) writeLockedOn(ctx context.Context, ws *websocket.Conn, data []byte) error {
	if ws == nil {
		return errors.New("wecomws: connection is down")
	}
	// A bounded write keeps a hung socket from holding the serializer (and
	// with it Send and the heartbeat) forever.
	wctx, cancel := context.WithTimeout(ctx, c.parent.writeTimeout)
	defer cancel()
	return ws.Write(wctx, websocket.MessageText, data)
}

// write is the Send-facing serializer: one frame, bounded write timeout.
func (c *botConn) write(ctx context.Context, env envelope) error {
	data, err := json.Marshal(env)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writeLocked(ctx, data)
}

// writeTo sends one frame on a specific connection under the same serializer;
// used for the subscribe frame, which precedes publication of c.conn.
func (c *botConn) writeTo(ctx context.Context, ws *websocket.Conn, env envelope) error {
	data, err := json.Marshal(env)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writeLockedOn(ctx, ws, data)
}

// pendingBufferMax bounds the pre-ack callback buffer.
const pendingBufferMax = 16

// errBadFrame marks frames that cannot be routed (binary, unparseable, no
// cmd): logged and skipped instead of tearing the connection down.
var errBadFrame = errors.New("bad frame")

func readEnvelope(ctx context.Context, ws *websocket.Conn) (envelope, error) {
	typ, data, err := ws.Read(ctx)
	if err != nil {
		return envelope{}, err
	}
	if typ != websocket.MessageText {
		return envelope{}, fmt.Errorf("%w: binary frame", errBadFrame)
	}
	var env envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return envelope{}, fmt.Errorf("%w: %v", errBadFrame, err)
	}
	if env.Cmd == "" {
		return envelope{}, fmt.Errorf("%w: missing cmd", errBadFrame)
	}
	return env, nil
}

// sleep waits for d exactly; false on ctx cancel.
func sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// sleepJitter waits for d with ±25% jitter so competing reconnects do not
// fire in lockstep; false on ctx cancel.
func sleepJitter(ctx context.Context, d time.Duration) bool {
	jitter := d / 4
	if jitter > 0 {
		//nolint:gosec // G404: reconnect jitter needs no cryptographic randomness
		d += time.Duration(rand.Int64N(int64(2*jitter))) - jitter
	}
	return sleep(ctx, d)
}
