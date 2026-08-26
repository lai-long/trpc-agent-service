// Package mock 提供一个不依赖任何真实 IM 平台的 Mock Channel。
//
// 用途（设计文档阶段 1 / 风险 7）：企微测试号申请可能阻塞联调，
// Mock Channel 保证「回调 → 归一化 → 处理 → 回复」全链路可以先跑通，
// 之后企微适配器只需实现同一个 channels.Channel 接口即可替换。
package mock

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
)

// callbackRequest 是模拟 IM 平台推送过来的回调报文。
type callbackRequest struct {
	MsgID  string `json:"msg_id"`  // 消息 ID，对应 IM 平台的 MsgId，用于幂等
	UserID string `json:"user_id"` // 发送人
	ChatID string `json:"chat_id"` // 群 ID，留空表示单聊
	Text   string `json:"text"`
}

// Channel 实现 channels.Channel 的 Mock 版本。
type Channel struct {
	mu   sync.Mutex
	seen map[string]struct{}        // 内存版 dedup:{channel}:{msg_id}，正式实现放 Redis
	sent []channels.OutboundMessage // 已发送记录，模拟 IM 侧收件箱，便于验证
}

// New 创建一个 Mock Channel。
func New() *Channel {
	return &Channel{seen: make(map[string]struct{})}
}

// Name implements channels.Channel.
func (c *Channel) Name() string { return "mock" }

// RegisterRoutes implements channels.Channel.
// POST /mock/callback：模拟 IM 的 webhook 回调。
func (c *Channel) RegisterRoutes(mux *http.ServeMux, h channels.Handler) {
	mux.HandleFunc("/mock/callback", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// 1. 解码 IM 回调报文（正式实现里这一步之前还有验签/解密）。
		var req callbackRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
			return
		}
		if req.MsgID == "" || req.UserID == "" {
			http.Error(w, "msg_id and user_id are required", http.StatusBadRequest)
			return
		}

		// 2. 幂等去重：对应设计 5.1.4 的 SETNX dedup:{channel}:{msg_id}。
		if !c.markSeen(req.MsgID) {
			zap.L().Warn("duplicate message dropped",
				zap.String(plog.FieldChannel, c.Name()), zap.String(plog.FieldMsgID, req.MsgID))
			// 重复消息照样回 success，让 IM 不再重推（设计 5.2.2）。
			writeReply(w, "duplicate", "")
			return
		}

		// 3. 消息归一化：外部报文 → 平台统一的 InboundMessage。
		msg := channels.InboundMessage{
			Channel:    c.Name(),
			MsgID:      req.MsgID,
			SessionKey: channels.SessionKey(c.Name(), req.UserID, req.ChatID),
			UserID:     req.UserID,
			ChatID:     req.ChatID,
			Text:       req.Text,
			TraceID:    req.MsgID, // 最基础版用 msg_id 充当 trace_id，正式实现接 OTel
			ReceivedAt: time.Now(),
		}

		// 4. 交给上层处理（正式实现：写 Redis Stream，由 Worker 异步消费；
		//    最基础版：同步调用，直接拿到回复）。
		out, err := h.Handle(r.Context(), msg)
		if err != nil {
			http.Error(w, "handle error: "+err.Error(), http.StatusInternalServerError)
			return
		}

		// 5. 发送回复（正式实现：出站队列 → IM message/send；Mock：记入内存收件箱）。
		if err := c.Send(r.Context(), out); err != nil {
			http.Error(w, "send error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		writeReply(w, "ok", out.Text)
	})
}

// Send implements channels.Channel.
// Mock 不真的调 IM 接口，只把回复存进内存，供测试和调试查看。
func (c *Channel) Send(_ context.Context, msg channels.OutboundMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sent = append(c.sent, msg)
	zap.L().Info("reply sent",
		zap.String(plog.FieldChannel, msg.Channel),
		zap.String(plog.FieldSessionKey, msg.SessionKey),
		zap.String(plog.FieldTraceID, msg.TraceID),
		zap.String("text", msg.Text))
	return nil
}

// Sent 返回所有已发送的回复，供测试断言。
func (c *Channel) Sent() []channels.OutboundMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]channels.OutboundMessage(nil), c.sent...)
}

// markSeen 实现「首次到达返回 true，重复返回 false」的去重语义。
func (c *Channel) markSeen(msgID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.seen[msgID]; ok {
		return false
	}
	c.seen[msgID] = struct{}{}
	return true
}

func writeReply(w http.ResponseWriter, status, text string) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": status, "reply": text})
}
