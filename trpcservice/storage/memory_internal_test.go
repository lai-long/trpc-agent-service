package storage

import (
	"context"
	"errors"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/memory"
)

// noopEmbedder always returns a fixed-dimension vector; it exists so the
// internal enqueue paths can run without an embeddings API.
type noopEmbedder struct{ dim int }

func (n noopEmbedder) GetEmbedding(context.Context, string) ([]float64, error) {
	return make([]float64, n.dim), nil
}
func (n noopEmbedder) GetEmbeddingWithUsage(ctx context.Context, text string) ([]float64, map[string]any, error) {
	v, _ := n.GetEmbedding(ctx, text)
	return v, nil, nil
}
func (n noopEmbedder) GetDimensions() int { return n.dim }

// The dimension guard protects the vector(1536) column: empty and
// wrong-width embeddings are rejected before they reach PG.
func TestCheckEmbeddingDim(t *testing.T) {
	if err := checkEmbeddingDim(nil); err == nil {
		t.Fatal("an empty embedding must be rejected")
	}
	if err := checkEmbeddingDim([]float64{1, 2, 3}); err == nil {
		t.Fatal("a wrong-dimension embedding must be rejected")
	}
	if err := checkEmbeddingDim(make([]float64, MemoryEmbeddingDimension)); err != nil {
		t.Fatalf("a matching embedding must pass: %v", err)
	}
}

// A full embed queue drops the job with a warning: the next write of the same
// memory re-enqueues, so the drop must not block or error.
func TestEnqueueEmbeddingDropsWhenQueueFull(t *testing.T) {
	svc := &PGMemoryService{
		embedder: noopEmbedder{dim: MemoryEmbeddingDimension},
		jobs:     make(chan memoryEmbedJob, 1),
	}
	svc.enqueueEmbedding("m1", "t1", "c1")
	svc.enqueueEmbedding("m2", "t2", "c2") // dropped, queue full

	if len(svc.jobs) != 1 {
		t.Fatalf("queue capacity must stay 1, got %d", len(svc.jobs))
	}
	job := <-svc.jobs
	if job.memoryID != "m1" || job.tenantID != "t1" || job.content != "c1" {
		t.Fatalf("unexpected queued job: %+v", job)
	}
}

// Without an embedder the enqueue is a straight no-op.
func TestEnqueueEmbeddingNoopWithoutEmbedder(t *testing.T) {
	svc := &PGMemoryService{jobs: make(chan memoryEmbedJob, 1)}
	svc.enqueueEmbedding("m1", "t1", "c1")
	if len(svc.jobs) != 0 {
		t.Fatalf("no embedder configured, job must not be queued, got %d", len(svc.jobs))
	}
}

// The app-key variant resolves the tenant first; an unknown app must drop the
// job with a warning instead of queueing garbage.
func TestEnqueueEmbeddingForAppResolveFailure(t *testing.T) {
	pool, err := NewPG(context.Background(), testPGDSN)
	if err != nil {
		t.Skipf("postgres unavailable (%v), skipping integration test", err)
	}
	defer pool.Close()

	svc := &PGMemoryService{
		pool:     pool,
		tenants:  newAppTenantResolver(pool),
		embedder: noopEmbedder{dim: MemoryEmbeddingDimension},
		jobs:     make(chan memoryEmbedJob, 4),
	}
	svc.enqueueEmbeddingForApp(context.Background(), "no-such-app", "m1", "c1")
	if len(svc.jobs) != 0 {
		t.Fatalf("an unresolvable app must not queue a job, got %d", len(svc.jobs))
	}
}

// A failing embedder must not crash the loop worker: the job is warned and
// dropped (guarded here by calling processEmbedJob directly).
func TestProcessEmbedJobEmbedderError(t *testing.T) {
	pool, err := NewPG(context.Background(), testPGDSN)
	if err != nil {
		t.Skipf("postgres unavailable (%v), skipping integration test", err)
	}
	defer pool.Close()

	svc := &PGMemoryService{
		pool:     pool,
		tenants:  newAppTenantResolver(pool),
		embedder: failingEmbedder{},
	}
	// Must return without panicking despite the embedder failure.
	svc.processEmbedJob(memoryEmbedJob{memoryID: "00000000-0000-0000-0000-000000000000", tenantID: zeroTenant, content: "x"})
}

// A mis-dimensioned or failed write degrades to a warn: the job never
// crashes the worker, and the recall path falls back to keywords.
func TestProcessEmbedJobDegradesOnBadDimAndClosedPool(t *testing.T) {
	pool, err := NewPG(context.Background(), testPGDSN)
	if err != nil {
		t.Skipf("postgres unavailable (%v), skipping integration test", err)
	}
	job := memoryEmbedJob{memoryID: "00000000-0000-0000-0000-000000000000", tenantID: zeroTenant, content: "x"}

	// Wrong dimension: rejected before touching PG.
	badSvc := &PGMemoryService{pool: pool, embedder: noopEmbedder{dim: 8}}
	badSvc.processEmbedJob(job)

	// Right dimension but dead pool: the upsert fails, warn and return.
	pool.Close()
	deadSvc := &PGMemoryService{pool: pool, embedder: noopEmbedder{dim: MemoryEmbeddingDimension}}
	deadSvc.processEmbedJob(job)
}

// The embedding worker's resolve step for UpdateMemory callers fails cleanly
// when the pool is gone.
func TestMemoryClosedPoolErrors(t *testing.T) {
	pool, err := NewPG(context.Background(), testPGDSN)
	if err != nil {
		t.Skipf("postgres unavailable (%v), skipping integration test", err)
	}
	svc := NewPGMemoryService(pool)
	pool.Close()
	ctx := context.Background()

	userKey := memory.UserKey{AppName: "no-such-app", UserID: "u"}
	if _, err := svc.ReadMemories(ctx, userKey, 10); err == nil {
		t.Fatal("ReadMemories must fail when the tenant cannot be resolved")
	}
	if _, err := svc.SearchMemories(ctx, userKey, "q"); err == nil {
		t.Fatal("SearchMemories must fail when the tenant cannot be resolved")
	}
	if err := svc.AddMemory(ctx, userKey, "m", nil); err == nil {
		t.Fatal("AddMemory must fail when the tenant cannot be resolved")
	}
	if err := svc.UpdateMemory(ctx, memory.Key{AppName: "no-such-app", UserID: "u", MemoryID: "00000000-0000-0000-0000-000000000000"}, "m", nil); err == nil {
		t.Fatal("UpdateMemory must fail when the tenant cannot be resolved")
	}
	if err := svc.DeleteMemory(ctx, memory.Key{AppName: "no-such-app", UserID: "u", MemoryID: "00000000-0000-0000-0000-000000000000"}); err == nil {
		t.Fatal("DeleteMemory must fail when the tenant cannot be resolved")
	}
	if err := svc.ClearMemories(ctx, userKey); err == nil {
		t.Fatal("ClearMemories must fail when the tenant cannot be resolved")
	}
}

// failingEmbedder always errors.
type failingEmbedder struct{}

func (failingEmbedder) GetEmbedding(context.Context, string) ([]float64, error) {
	return nil, errors.New("embedder unavailable")
}
func (failingEmbedder) GetEmbeddingWithUsage(context.Context, string) ([]float64, map[string]any, error) {
	return nil, nil, errors.New("embedder unavailable")
}
func (failingEmbedder) GetDimensions() int { return 0 }
