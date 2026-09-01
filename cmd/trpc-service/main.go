package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"syscall"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/liuzengh/trpc-agent-service/trpcservice"
	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/mock"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecom"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/web"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/embedder"
	openaiembed "trpc.group/trpc-go/trpc-agent-go/knowledge/embedder/openai"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/model/openai"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessionredis "trpc.group/trpc-go/trpc-agent-go/session/redis"
	sessionsummary "trpc.group/trpc-go/trpc-agent-go/session/summary"
	ttool "trpc.group/trpc-go/trpc-agent-go/tool"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
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

	// PG serves three consumers: the Auditor (async batch lane for routine
	// events, sync lane for critical decisions), the tenant Resolver (gateway
	// routing), and the session store when SessionBackend=postgres. PG down at
	// startup degrades audit and routing to off with a warning — the message
	// pipeline must not depend on them.
	auditor, resolver, pgPool, pgCleanup := startPGConsumers(ctx, cfg)
	defer pgCleanup()

	// Session services: one per supported backend (design 5.1.1 受控菜单).
	// The tenant's storage_config.session.type routes its apps;
	// TRPC_SESSION_BACKEND picks the platform default.
	sessByType, defaultSess, sessErr := buildSessionServices(ctx, cfg, pgPool)
	if sessErr != nil {
		plog.Warnf("session services unavailable (%v)", sessErr)
	}
	defer func() {
		for _, s := range sessByType {
			_ = s.Close()
		}
	}()

	// Artifact storage (S3-compatible, MinIO locally); optional.
	artifacts := buildArtifact(cfg)

	ch := mock.New()
	enqueue := web.EnqueueHandler{
		Stream:       stream,
		Dedup:        storage.NewDeduper(rdb),
		Routes:       resolver,
		Limiter:      storage.NewLimiter(rdb),
		DefaultQPS:   parseFloat(cfg.GatewayRateQPS, 50),
		DefaultBurst: parseInt(cfg.GatewayRateBurst, 100),
	}
	mux := http.NewServeMux()
	ch.RegisterRoutes(mux, enqueue)

	// Channel registry for the outbound sender: mock always, wecom when
	// configured. Channel misconfiguration disables only that channel.
	channelSet := map[string]channels.Channel{ch.Name(): ch}
	if wc := startWecom(cfg); wc != nil {
		wc.RegisterRoutes(mux, enqueue)
		channelSet[wc.Name()] = wc
	}

	metricsHandler, err := metrics.InitMetrics()
	if err != nil {
		return err
	}
	mux.Handle("GET /metrics", metricsHandler)

	// Knowledge base: enabled only with an embeddings-capable endpoint
	// (TRPC_EMBEDDER_*); the default DeepSeek chat endpoint has none.
	kb := buildKnowledge(ctx, cfg)

	// Admin API needs PG; it also starts the resolver's invalidation watch so
	// publish/rollback reaches workers in seconds (design 5.2.3). The storage
	// migration executor lives on the same dependency.
	if pgPool != nil {
		adminAPI := web.NewAdminAPI(pgPool, auditor, rdb, cfg.AdminToken)
		adminAPI.Knowledge = kb
		adminAPI.DefaultSessionBackend = defaultSess
		adminAPI.RegisterRoutes(mux)
		resolver.WatchInvalidations(ctx, rdb)
		if cfg.AdminToken == "" {
			plog.Warnf("admin API unprotected (TRPC_ADMIN_TOKEN unset) — dev mode only")
		}
		plog.Infof("admin API enabled (/admin/...)")
	}

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

	processor, cleanup := buildProcessor(ctx, cfg, rdb, auditor, pgPool, kb, resolver, sessByType, defaultSess, artifacts)
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
	worker := &agent.Worker{Stream: stream, Lock: storage.NewLock(rdb), Processor: processor, Name: consumer + "-w"}
	g.Go(func() error { return worker.Run(gctx) })

	sender := &channels.Sender{
		Stream:    stream,
		Sent:      storage.NewSentMarker(rdb),
		Channels:  channelSet,
		Name:      consumer + "-s",
		Limiter:   storage.NewLimiter(rdb),
		SendQPS:   parseFloat(cfg.SendRateQPS, 20),
		SendBurst: parseInt(cfg.SendRateBurst, 40),
	}
	g.Go(func() error { return sender.Run(gctx) })

	// Monthly-ish archival (design 5.1.3): move old session_event / audit_log
	// rows to the archive tables so the hot tables stay small.
	if pgPool != nil {
		archiver := storage.NewArchiver(pgPool,
			parseDuration(cfg.ArchiveRetention, 30*24*time.Hour),
			parseDuration(cfg.ArchiveInterval, 24*time.Hour))
		g.Go(func() error { archiver.Run(gctx); return nil })
	}

	// Queue depth / pending gauges feeding the alerts of design 5.2.4.
	metrics.StartStreamCollector(gctx, stream, 15*time.Second)

	// Storage migration executor (design 5.2.6): advances active migrations
	// through backfilling → read switch → observation → done.
	if pgPool != nil {
		migrator := storage.NewMigrator(pgPool, rdb, sessByType,
			parseDuration(cfg.MigrationObserve, 24*time.Hour))
		g.Go(func() error { migrator.Run(gctx); return nil })
	}

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
func startPGConsumers(ctx context.Context, cfg config.Config) (*storage.Auditor, *tenant.Resolver, *pgxpool.Pool, func()) {
	noop := func() {}
	pool, err := storage.NewPG(ctx, cfg.PGDSN)
	if err != nil {
		plog.Warnf("PG unreachable, audit and tenant routing disabled: %v", err)
		return nil, nil, nil, noop
	}
	a := storage.NewAuditor(pool)
	a.Start()
	plog.Infof("audit and tenant routing enabled")
	return a, tenant.NewResolver(tenant.NewPGStore(pool)), pool, func() {
		a.Close()
		pool.Close()
	}
}

// startWecom builds the WeCom channel from env config; it returns nil (with a
// warning) when the channel is not configured or its secrets are missing, so
// the rest of the platform keeps serving the other channels.
func startWecom(cfg config.Config) *wecom.Channel {
	if cfg.WecomCorpID == "" {
		return nil
	}
	agentID, err := strconv.Atoi(cfg.WecomAgentID)
	if err != nil || agentID == 0 {
		plog.Warnf("wecom channel disabled: invalid TRPC_WECOM_AGENT_ID %q", cfg.WecomAgentID)
		return nil
	}
	wc, err := wecom.New(wecom.Config{
		CorpID:    cfg.WecomCorpID,
		AgentID:   agentID,
		TokenRef:  cfg.WecomTokenRef,
		AESKeyRef: cfg.WecomAESKeyRef,
		SecretRef: cfg.WecomSecretRef,
		APIBase:   cfg.WecomAPIBase,
	}, config.NewFileResolver(cfg.SecretsDir))
	if err != nil {
		plog.Warnf("wecom channel disabled: %v", err)
		return nil
	}
	plog.Infof("wecom channel enabled (callback: POST /wecom/callback)")
	return wc
}

// buildSessionServices builds one session service per supported backend
// (design 5.1.1 受控菜单) plus the platform default selection. WithEnableTracing
// is required beyond observability: with tracing disabled, the redis session
// service's startSpan falls back to the caller's active span and its defer
// span.End() would end OUR worker span prematurely (framework quirk).
func buildSessionServices(ctx context.Context, cfg config.Config, pgPool *pgxpool.Pool) (map[string]session.Service, string, error) {
	byType := map[string]session.Service{}
	rs, err := sessionredis.NewService(
		sessionredis.WithRedisClientURL("redis://"+cfg.RedisAddr),
		sessionredis.WithEnableTracing(true),
	)
	if err != nil {
		return nil, "", fmt.Errorf("redis session service: %w", err)
	}
	byType["redis"] = rs
	if pgPool != nil {
		var sessOpts []storage.PGSessionOption
		if sm := buildSummarizer(ctx, cfg); sm != nil {
			sessOpts = append(sessOpts, storage.WithSummarizer(sm))
		}
		byType["postgres"] = storage.NewPGSessionService(pgPool, sessOpts...)
	} else if cfg.SessionBackend == "postgres" {
		plog.Warnf("TRPC_SESSION_BACKEND=postgres but PG is unreachable, defaulting to redis")
	}
	def := cfg.SessionBackend
	if _, ok := byType[def]; !ok {
		plog.Warnf("session backend %q unavailable, defaulting to redis", def)
		def = "redis"
	}
	return byType, def, nil
}

// buildArtifact builds the S3-compatible artifact store (design: Artifact =
// S3, MinIO locally); unreachable endpoints degrade to nil with a warning.
func buildArtifact(cfg config.Config) artifact.Service {
	if cfg.S3Endpoint == "" {
		return nil
	}
	svc, err := storage.NewS3ArtifactService(storage.S3ArtifactConfig{
		Endpoint:     cfg.S3Endpoint,
		AccessKeyRef: cfg.S3AccessKeyRef,
		SecretKeyRef: cfg.S3SecretKeyRef,
		Bucket:       cfg.S3Bucket,
		Secure:       cfg.S3Secure == "true",
		Prefix:       "artifact/",
		Secrets:      config.NewFileResolver(cfg.SecretsDir),
	})
	if err != nil {
		plog.Warnf("artifact store unavailable (%v), artifacts disabled", err)
		return nil
	}
	plog.Infof("artifact store enabled (s3 %s, bucket %s)", cfg.S3Endpoint, cfg.S3Bucket)
	return svc
}

// buildProcessor assembles the processing chain: the platform tool registry,
// the dangerous-tool Approver, the per-app Assembler (one Runner per
// agent_app, rebuilt when the resolver reports a config change), and the
// Guarded guardrail wrapper that owns input/output checks and message-level
// auditing. Apps whose model key is missing are served by the echo fallback,
// so the pipeline stays demoable without LLM access.
// The returned cleanup closes resources.
func buildProcessor(ctx context.Context, cfg config.Config, rdb *redis.Client, auditor *storage.Auditor, pgPool *pgxpool.Pool, kb *knowledge.BuiltinKnowledge, resolver *tenant.Resolver, sessByType map[string]session.Service, defaultSess string, artifacts artifact.Service) (agent.Processor, func()) {
	noop := func() {}

	registry := tool.DemoTools()
	approver := agent.NewApprover(rdb, registry, 0)

	// Cost accounting: token counts are always recorded in audit; prices turn
	// them into cost where configured.
	if cfg.ModelPrices != "" {
		var table map[string][2]float64
		if err := json.Unmarshal([]byte(cfg.ModelPrices), &table); err != nil {
			plog.Warnf("invalid TRPC_MODEL_PRICES, cost tracking disabled: %v", err)
		} else {
			agent.SetModelPricing(table)
		}
	}
	wrap := func(inner agent.Processor) agent.Processor {
		return &agent.Guarded{
			Inner:    inner,
			Approver: approver,
			Auditor:  auditor,
			Input:    []agent.InputChecker{agent.SensitiveWordInput(agent.DefaultBlockedWords)},
			Output:   []agent.OutputChecker{agent.RedactOutput()},
		}
	}

	if len(sessByType) == 0 {
		return wrap(agent.EchoProcessor{}), noop
	}

	timeout, err := time.ParseDuration(cfg.ModelTimeout)
	if err != nil || timeout <= 0 {
		plog.Warnf("invalid TRPC_MODEL_TIMEOUT %q, defaulting to %s", cfg.ModelTimeout, agent.DefaultRunTimeout)
		timeout = agent.DefaultRunTimeout
	}

	// Memory service over the PG memory_item table (two-level scope, soft
	// delete), shared by all per-app runners; with an embeddings-capable
	// endpoint the service gains semantic recall (async embedding worker).
	var memService memory.Service
	if pgPool != nil {
		if emb, _, err := buildEmbedder(ctx, cfg); err == nil && emb != nil {
			memService = storage.NewPGMemoryService(pgPool, storage.WithMemoryEmbedder(emb))
		} else {
			memService = storage.NewPGMemoryService(pgPool)
		}
	}

	callbacks := ttool.NewCallbacks().RegisterBeforeTool(approver.BeforeTool)
	// Guard the typed-nil interfaces: a nil *tenant.Resolver / nil
	// *BuiltinKnowledge must stay nil behind the interface, or the assembler
	// would route to nothing / wire a knowledge tool onto nothing.
	var apps agent.AppProvider
	if resolver != nil {
		apps = resolver
	}
	var kbIface knowledge.Knowledge
	if kb != nil {
		kbIface = kb
	}
	assembler := agent.NewAssembler(agent.AssemblerConfig{
		Apps:      apps,
		Secrets:   config.NewFileResolver(cfg.SecretsDir),
		Registry:  registry,
		Memory:    memService,
		Knowledge: kbIface,
		Callbacks: callbacks,
		// Session backends: the tenant's storage_config routes its apps
		// between them (migration flow only); the env default serves the rest.
		SessionsByType: sessByType,
		DefaultSession: defaultSess,
		Artifact:       artifacts,
		Defaults: agent.ModelSpec{
			Name:      cfg.ModelName,
			BaseURL:   cfg.ModelBaseURL,
			APIKeyRef: cfg.ModelAPIKeyRef,
		},
		DefaultApp: cfg.AppName,
		Timeout:    timeout,
		Retries:    1,
	})
	plog.Infof("per-app runner assembler ready (model default=%s, timeout=%s, session backends=%v)",
		cfg.ModelName, timeout, slices.Sorted(maps.Keys(sessByType)))
	return wrap(assembler), func() { _ = assembler.Close() }
}

// buildSummarizer builds the framework session summarizer on the platform
// default model. Summarization is a background maintenance job, so it uses
// the env default model rather than per-tenant model config. Without a model
// key, summaries are disabled and sessions always replay in full.
func buildSummarizer(ctx context.Context, cfg config.Config) sessionsummary.SessionSummarizer {
	resolver := config.NewFileResolver(cfg.SecretsDir)
	key, err := resolver.Resolve(ctx, cfg.ModelAPIKeyRef)
	if err != nil {
		plog.Warnf("model key %q unavailable (%v), session summaries disabled", cfg.ModelAPIKeyRef, err)
		return nil
	}
	threshold := parseInt(cfg.SummaryEventThreshold, 20)
	m := openai.New(cfg.ModelName,
		openai.WithBaseURL(cfg.ModelBaseURL),
		openai.WithAPIKey(key),
	)
	plog.Infof("session summarizer enabled (event threshold=%d)", threshold)
	return sessionsummary.NewSummarizer(m, sessionsummary.WithEventThreshold(threshold))
}

// parseInt / parseFloat / parseDuration parse env string values, falling back
// to def with a warning on invalid input.
func parseInt(s string, def int) int {
	v, err := strconv.Atoi(s)
	if err != nil || v <= 0 {
		plog.Warnf("invalid integer %q, using default %d", s, def)
		return def
	}
	return v
}

func parseFloat(s string, def float64) float64 {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || v <= 0 {
		plog.Warnf("invalid number %q, using default %v", s, def)
		return def
	}
	return v
}

func parseDuration(s string, def time.Duration) time.Duration {
	v, err := time.ParseDuration(s)
	if err != nil || v <= 0 {
		plog.Warnf("invalid duration %q, using default %s", s, def)
		return def
	}
	return v
}

// buildEmbedder builds the OpenAI-compatible embeddings client shared by
// Knowledge and memory semantic recall; (nil, 0, nil) when TRPC_EMBEDDER_MODEL
// is unset — the default chat endpoint (DeepSeek) has no embeddings API.
func buildEmbedder(ctx context.Context, cfg config.Config) (embedder.Embedder, int, error) {
	if cfg.EmbedderModel == "" {
		return nil, 0, nil
	}
	resolver := config.NewFileResolver(cfg.SecretsDir)
	key, err := resolver.Resolve(ctx, cfg.EmbedderKeyRef)
	if err != nil {
		return nil, 0, fmt.Errorf("embedder key %q: %w", cfg.EmbedderKeyRef, err)
	}
	dim, err := strconv.Atoi(cfg.EmbedderDim)
	if err != nil || dim <= 0 {
		return nil, 0, fmt.Errorf("invalid TRPC_EMBEDDER_DIMENSION %q", cfg.EmbedderDim)
	}
	return openaiembed.New(
		openaiembed.WithModel(cfg.EmbedderModel),
		openaiembed.WithAPIKey(key),
		openaiembed.WithBaseURL(cfg.EmbedderBaseURL),
		openaiembed.WithDimensions(dim),
	), dim, nil
}

// buildKnowledge builds the pgvector-backed knowledge base when an
// embeddings-capable endpoint is configured; otherwise it returns nil and the
// agent runs without knowledge retrieval.
func buildKnowledge(ctx context.Context, cfg config.Config) *knowledge.BuiltinKnowledge {
	emb, dim, err := buildEmbedder(ctx, cfg)
	if err != nil {
		plog.Warnf("embedder unavailable (%v), knowledge disabled", err)
		return nil
	}
	if emb == nil {
		return nil
	}
	kb, err := agent.NewKnowledgeBase(cfg.PGDSN, cfg.KnowledgeTable, dim, emb)
	if err != nil {
		plog.Warnf("knowledge base unavailable (%v), knowledge disabled", err)
		return nil
	}
	plog.Infof("knowledge base enabled (model=%s, dim=%d)", cfg.EmbedderModel, dim)
	return kb
}
