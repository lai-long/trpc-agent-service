package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/liuzengh/trpc-agent-service/trpcservice"
	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/mock"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/web"
	sessionredis "trpc.group/trpc-go/trpc-agent-go/session/redis"
)

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "serve":
			if err := serve(); err != nil {
				zap.L().Fatal("serve exited", zap.Error(err))
			}
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

// serve runs the all-in-one role: gateway + worker + sender in one process,
// backed by the Redis Streams from docker-compose.
//
// Chain (sync ack + async consume):
//
//	mock callback → EnqueueHandler → stream:inbound → Worker(Runner) → stream:outbound → Sender → channel.Send
func serve() error {
	cfg := config.Load()
	plog.Init(cfg.LogLevel, cfg.LogFormat != "json")
	defer plog.Sync()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Tracing goes up before anything that emits spans. Endpoint comes from
	// OTEL_EXPORTER_OTLP_ENDPOINT; the platform adds spans around the callback,
	// the Stream hop and the send, on top of the framework's own spans.
	shutdownTrace, err := metrics.InitTracing(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = shutdownTrace()
	}()

	// Infra connections are created here at the entry point, then injected;
	// business packages never dial by themselves.
	rdb, err := storage.NewRedis(ctx, cfg.RedisAddr)
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()

	stream := storage.NewStream(rdb)
	if err := stream.EnsureGroup(ctx, storage.StreamInbound, "workers"); err != nil {
		return err
	}
	if err := stream.EnsureGroup(ctx, storage.StreamOutbound, "senders"); err != nil {
		return err
	}

	// PG serves two consumers: the Auditor (async batch lane for routine
	// events, sync lane for critical decisions) and the tenant Resolver
	// (gateway routing). PG down at startup degrades both to off with a
	// warning — the message pipeline must not depend on them.
	auditor, resolver, pgCleanup := startPGConsumers(ctx, cfg)
	defer pgCleanup()

	ch := mock.New()
	mux := http.NewServeMux()
	ch.RegisterRoutes(mux, web.EnqueueHandler{
		Stream: stream,
		Dedup:  storage.NewDeduper(rdb),
		Routes: resolver,
	})

	metricsHandler, err := metrics.InitMetrics()
	if err != nil {
		return err
	}
	mux.Handle("GET /metrics", metricsHandler)

	srv := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: mux,
		// Timeouts against slow/lazy clients (Slowloris): callback bodies are
		// small and replies are immediate in the async chain, so tight limits
		// are safe. WriteTimeout stays unset to leave room for future SSE.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	processor, cleanup := buildProcessor(ctx, cfg)
	defer cleanup()

	consumer := fmt.Sprintf("%s-%d", "allinone", os.Getpid())
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		zap.L().Info("listening",
			zap.String("addr", cfg.HTTPAddr), zap.String("mock_callback", "POST /mock/callback"))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	worker := &agent.Worker{Stream: stream, Lock: storage.NewLock(rdb), Auditor: auditor, Processor: processor, Name: consumer + "-w"}
	g.Go(func() error { return worker.Run(gctx) })

	sender := &channels.Sender{
		Stream:   stream,
		Sent:     storage.NewSentMarker(rdb),
		Channels: map[string]channels.Channel{ch.Name(): ch},
		Name:     consumer + "-s",
	}
	g.Go(func() error { return sender.Run(gctx) })

	// Graceful shutdown: stop pulling new messages first, let in-flight
	// processing finish, then close the HTTP server.
	g.Go(func() error {
		<-gctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	})

	return g.Wait()
}

// startPGConsumers connects to PG and starts the consumers that depend on it:
// the audit flush loop and the tenant resolver used for gateway routing. PG
// being down degrades both to off with a warning; the message pipeline does
// not depend on them (audit catches up from logs/traces during the outage,
// and a nil resolver disables tenant routing). The returned cleanup stops the
// auditor and closes the shared pool.
func startPGConsumers(ctx context.Context, cfg config.Config) (*storage.Auditor, *tenant.Resolver, func()) {
	noop := func() {}
	pool, err := storage.NewPG(ctx, cfg.PGDSN)
	if err != nil {
		plog.Warnf("PG unreachable, audit and tenant routing disabled: %v", err)
		return nil, nil, noop
	}
	a := storage.NewAuditor(pool)
	a.Start()
	plog.Infof("audit and tenant routing enabled")
	return a, tenant.NewResolver(tenant.NewPGStore(pool)), func() {
		a.Close()
		pool.Close()
	}
}

// buildProcessor assembles the Runner-backed processor (llmagent + session/redis).
// Falls back to EchoProcessor when the model key is missing, so the pipeline
// stays demoable without LLM access. The returned cleanup closes resources.
func buildProcessor(ctx context.Context, cfg config.Config) (agent.Processor, func()) {
	noop := func() {}

	resolver := config.NewFileResolver(cfg.SecretsDir)
	apiKey, err := resolver.Resolve(ctx, cfg.ModelAPIKeyRef)
	if err != nil {
		plog.Warnf("model key %q unavailable (%v), falling back to echo processor", cfg.ModelAPIKeyRef, err)
		return agent.EchoProcessor{}, noop
	}

	// WithEnableTracing is required beyond observability: with tracing disabled,
	// the session service's startSpan falls back to the caller's active span and
	// its defer span.End() would end OUR worker span prematurely (framework quirk).
	sess, err := sessionredis.NewService(
		sessionredis.WithRedisClientURL("redis://"+cfg.RedisAddr),
		sessionredis.WithEnableTracing(true),
	)
	if err != nil {
		plog.Warnf("session service unavailable (%v), falling back to echo processor", err)
		return agent.EchoProcessor{}, noop
	}

	p := agent.NewRunnerProcessor(agent.RunnerConfig{
		AppName:        "trpc-service",
		BaseURL:        cfg.ModelBaseURL,
		APIKey:         apiKey,
		ModelName:      cfg.ModelName,
		SessionService: sess,
	})
	plog.Infof("runner processor ready (model=%s)", cfg.ModelName)
	return p, func() { _ = p.Close(); _ = sess.Close() }
}
