package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/liuzengh/trpc-agent-service/trpcservice"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/mock"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "serve" {
		serve()
		return
	}

	fmt.Printf("trpc-agent-service %s\n", trpcservice.Version)
	fmt.Println("multi-tenant node-based agent platform on tRPC-Agent-Go")
	fmt.Fprintf(os.Stderr, "usage: %s [serve]\n", os.Args[0])
}

// serve 启动最基础的单进程服务：Mock Channel + echo Handler。
// 后续演进：Handler 换成「写 Redis Stream」，另起 Worker 消费并调 Runner。
func serve() {
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

	addr := ":8080"
	log.Printf("listening on %s (mock callback: POST /mock/callback)", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
