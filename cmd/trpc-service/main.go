package main

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"go.uber.org/zap"

	"github.com/liuzengh/trpc-agent-service/trpcservice"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/mock"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
)

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "serve":
			serve()
			return
		case "-h", "--help":
			fmt.Fprintf(os.Stderr, "usage: %s [serve]\n", os.Args[0])
			return
		}
	}

	fmt.Printf("trpc-agent-service %s\n", trpcservice.Version)
	fmt.Println("multi-tenant node-based agent platform on tRPC-Agent-Go")
	fmt.Fprintf(os.Stderr, "usage: %s [serve]\n", os.Args[0])
}

// serve 启动最基础的单进程服务：Mock Channel + echo Handler。
// 后续演进：Handler 换成「写 Redis Stream」，另起 Worker 消费并调 Runner。
func serve() {
	cfg := config.Load()
	plog.Init(cfg.LogLevel, cfg.LogFormat != "json")
	defer plog.Sync()

	// echo Handler 站在将来 Worker/Runner 的位置，先用来打通链路。
	echo := channels.HandlerFunc(func(_ context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
		return channels.OutboundMessage{
			Channel:    msg.Channel,
			SessionKey: msg.SessionKey,
			UserID:     msg.UserID,
			ChatID:     msg.ChatID,
			Text:       "echo: " + msg.Text,
			TraceID:    msg.TraceID,
		}, nil
	})

	ch := mock.New()
	mux := http.NewServeMux()
	ch.RegisterRoutes(mux, echo)

	zap.L().Info("listening",
		zap.String("addr", cfg.HTTPAddr), zap.String("mock_callback", "POST /mock/callback"))
	if err := http.ListenAndServe(cfg.HTTPAddr, mux); err != nil {
		zap.L().Fatal("server exited", zap.Error(err))
	}
}
