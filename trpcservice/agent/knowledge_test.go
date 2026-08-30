package agent_test

import (
	"context"
	"hash/fnv"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/knowledge"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

// fakeEmbedder produces deterministic bag-of-runes vectors: texts sharing
// runes are close, so retrieval is testable without an embeddings API.
type fakeEmbedder struct{ dim int }

func (f fakeEmbedder) GetEmbedding(_ context.Context, text string) ([]float64, error) {
	v := make([]float64, f.dim)
	for _, r := range text {
		h := fnv.New32a()
		_, _ = h.Write([]byte(string(r)))
		v[int(h.Sum32())%f.dim]++
	}
	return v, nil
}

func (f fakeEmbedder) GetEmbeddingWithUsage(ctx context.Context, text string) ([]float64, map[string]any, error) {
	v, err := f.GetEmbedding(ctx, text)
	return v, nil, err
}

func (f fakeEmbedder) GetDimensions() int { return f.dim }

// TestKnowledgeBaseRoundTrip ingests a document through the platform source
// and retrieves it via vector search. Needs the compose PG (pgvector image);
// skips when unreachable.
func TestKnowledgeBaseRoundTrip(t *testing.T) {
	ctx := context.Background()
	pool, err := storage.NewPG(ctx, "postgres://trpc:trpc-dev-only@localhost:5432/trpc?sslmode=disable")
	if err != nil {
		t.Skipf("postgres unavailable (%v), skipping integration test", err)
	}
	defer pool.Close()

	const dim = 64
	table := "knowledge_test_" + "roundtrip"
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS `+table)
	})

	kb, err := agent.NewKnowledgeBase(
		"postgres://trpc:trpc-dev-only@localhost:5432/trpc?sslmode=disable",
		table, dim, fakeEmbedder{dim: dim})
	if err != nil {
		t.Fatal(err)
	}

	src := &agent.DocSource{
		DocName:  "退款政策",
		Content:  "我们的退款政策：签收后七天内支持无理由退款，运费由平台承担。",
		Metadata: map[string]any{"tenant_id": "t1", "app_id": "a1"},
	}
	if err := kb.AddSource(ctx, src); err != nil {
		t.Fatal(err)
	}

	result, err := kb.Search(ctx, &knowledge.SearchRequest{Query: "怎么退款"})
	if err != nil {
		t.Fatal(err)
	}
	if result == nil || result.Text == "" {
		t.Fatal("search returned no document")
	}
	t.Logf("search hit: score=%.3f text=%q", result.Score, result.Text)
}
