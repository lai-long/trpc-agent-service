// Package channels adapts IM platforms (WeCom, WeChat, Telegram, etc.)
// into tRPC-Agent-Go Runner inputs, following the OpenClaw Channel model.
//
// 设计文档第 3 节：Channel Adapter 负责验签/加解密、消息编解码、调 IM 主动发送接口。
// 本文件定义与具体 IM 无关的最小抽象，各通道（mock / wecom / ...）实现 Channel 接口。
package channels

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// InboundMessage 是归一化后的入站消息。
// 在完整架构中它是 stream:inbound 的载荷（设计 5.1.4），由 Gateway 写入队列；
// 最基础版本中由 Channel 直接同步交给 Handler。
type InboundMessage struct {
	Channel    string    // 渠道类型：mock / wecom / wechat_kf ...
	MsgID      string    // IM 平台消息 ID，用于幂等去重（dedup:{channel}:{msg_id}）
	SessionKey string    // 应用内会话唯一键，见 SessionKey()
	UserID     string    // IM 侧用户 ID（企微 external_userid / 微信 openid）
	ChatID     string    // 群聊 ID，单聊为空
	Text       string    // 文本内容（最基础版只支持文本）
	TraceID    string    // 链路追踪 ID，贯穿回调→Worker→回复
	ReceivedAt time.Time // 回调到达时间
}

// OutboundMessage 是归一化后的出站回复。
// 在完整架构中它由 Worker 写入 stream:outbound，发送方消费后调 IM 主动发送接口；
// 最基础版本中由 Handler 直接返回，Channel 同步调用 Send 发出。
type OutboundMessage struct {
	Channel    string // 回哪个渠道
	SessionKey string // 回复所属会话（出站幂等键 sent:{session_id}:{event_seq} 的组成部分）
	UserID     string // 接收人（单聊必填）
	ChatID     string // 接收群（群聊必填）
	Text       string
	TraceID    string
}

// SessionKey 生成会话唯一键（设计 5.1.3）：
//   - 单聊：dm:{channel}:{user_id}，同一用户跨天复用同一会话
//   - 群聊：group:{channel}:{chat_id}，机器人在群里的上下文按群共享
//
// 会话唯一性最终由 (app_id, session_key) 保证，app 隶属租户，跨租户天然隔离。
func SessionKey(channel, userID, chatID string) string {
	if chatID != "" {
		return fmt.Sprintf("group:%s:%s", channel, chatID)
	}
	return fmt.Sprintf("dm:%s:%s", channel, userID)
}

// Handler 处理一条归一化入站消息并产出回复。
// 后续版本中它由 Gateway（投递 Redis Stream）和 Worker（Runner 执行）协作实现；
// 最基础版本里它是一个同步函数，便于打通最小闭环。
type Handler interface {
	Handle(ctx context.Context, msg InboundMessage) (OutboundMessage, error)
}

// HandlerFunc 让普通函数可以直接用作 Handler。
type HandlerFunc func(ctx context.Context, msg InboundMessage) (OutboundMessage, error)

// Handle implements Handler.
func (f HandlerFunc) Handle(ctx context.Context, msg InboundMessage) (OutboundMessage, error) {
	return f(ctx, msg)
}

// Channel 对接一类 IM 平台。
// 每个通道实现两个方向：RegisterRoutes 接 IM 回调（进来），Send 主动发消息（出去）。
type Channel interface {
	// Name 返回渠道标识，如 "mock"、"wecom"。
	Name() string
	// RegisterRoutes 把 IM 回调路由挂到 HTTP mux；收到消息后归一化并交给 h 处理。
	RegisterRoutes(mux *http.ServeMux, h Handler)
	// Send 调用 IM 主动发送接口（企微 message/send、微信客服 kf 消息等），把回复送达用户。
	Send(ctx context.Context, msg OutboundMessage) error
}
