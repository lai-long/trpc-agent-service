package agent_test

import (
	"context"
	"hash/fnv"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/knowledge"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/testenv"
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

// docID reads the ID a DocSource assigns its single document.
func docID(t *testing.T, s *agent.DocSource) string {
	t.Helper()
	docs, err := s.ReadDocuments(context.Background())
	if err != nil {
		t.Fatalf("ReadDocuments: %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("want 1 document, got %d", len(docs))
	}
	return docs[0].ID
}

// The document ID is the pgvector upsert key, so it has to carry the tenant
// scope: with the ID derived from name+content alone, two tenants ingesting
// the same document landed on one row, and the second ingest rewrote the
// first's content, embedding and tenant_id.
func TestDocSourceIDScopesByTenantAndApp(t *testing.T) {
	doc := func(tenantID, appID, name, content string) *agent.DocSource {
		md := map[string]any{}
		if tenantID != "" {
			md[agent.MetadataTenantID] = tenantID
		}
		if appID != "" {
			md[agent.MetadataAppID] = appID
		}
		return &agent.DocSource{DocName: name, Content: content, Metadata: md}
	}

	t1 := docID(t, doc("t1", "a1", "退款政策", "七天内无理由退款"))
	t2 := docID(t, doc("t2", "a1", "退款政策", "七天内无理由退款"))
	if t1 == t2 {
		t.Fatalf("the same document under two tenants must not share an ID: %s", t1)
	}

	// Re-ingesting the same document is an upsert, not a duplicate.
	if again := docID(t, doc("t1", "a1", "退款政策", "七天内无理由退款")); again != t1 {
		t.Fatalf("re-ingest must be idempotent: %s != %s", again, t1)
	}

	// A second app of the same tenant is a separate document, and so is
	// different content under the same name.
	if other := docID(t, doc("t1", "a2", "退款政策", "七天内无理由退款")); other == t1 {
		t.Fatal("a different app must not share the document ID")
	}
	if edited := docID(t, doc("t1", "a1", "退款政策", "三十天内无理由退款")); edited == t1 {
		t.Fatal("different content must not share the document ID")
	}

	// Field boundaries are length-prefixed: a shift that concatenates to the
	// same bytes must still hash differently.
	if a, b := docID(t, doc("t1", "a1x", "n", "c")), docID(t, doc("t1a", "1x", "n", "c")); a == b {
		t.Fatalf("shifted scope boundaries must not collide: %s", a)
	}

	// An unscoped document (the env-only dev fallback) keeps a stable ID
	// instead of failing, and stays distinct from a scoped one.
	unscoped := docID(t, doc("", "", "退款政策", "七天内无理由退款"))
	if unscoped == "" || unscoped == t1 {
		t.Fatalf("unscoped ID must be stable and distinct from the scoped one: %q", unscoped)
	}
	if unscoped != docID(t, &agent.DocSource{DocName: "退款政策", Content: "七天内无理由退款"}) {
		t.Fatal("a nil metadata map must hash like an empty one")
	}
}

// TestKnowledgeBaseRoundTrip ingests a document through the platform source
// and retrieves it via vector search. Needs the compose PG (pgvector image);
// skips when unreachable.
func TestKnowledgeBaseRoundTrip(t *testing.T) {
	ctx := context.Background()
	pool := testenv.PG(t)
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
		Metadata: map[string]any{agent.MetadataTenantID: "t1", agent.MetadataAppID: "a1"},
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

// A knowledge base on an unreachable or malformed DSN fails at construction
// instead of producing a half-initialized agent.
func TestKnowledgeBaseRejectsBadDSN(t *testing.T) {
	if _, err := agent.NewKnowledgeBase("not a valid dsn", "knowledge_test_bad", 64, fakeEmbedder{dim: 64}); err == nil {
		t.Fatal("malformed DSN must fail NewKnowledgeBase")
	}
}
