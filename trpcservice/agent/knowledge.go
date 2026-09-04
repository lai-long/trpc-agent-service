package agent

import (
	"context"
	"fmt"
	"hash/fnv"
	"strconv"

	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/embedder"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore/pgvector"
)

// NewKnowledgeBase builds the platform knowledge base on pgvector; the store
// auto-creates the vector extension, its table and the HNSW index. Documents
// carry tenant/app metadata, and the agent searches through a metadata filter
// so knowledge stays tenant-isolated inside the shared table.
func NewKnowledgeBase(pgDSN, table string, dim int, emb embedder.Embedder) (*knowledge.BuiltinKnowledge, error) {
	vs, err := pgvector.New(
		pgvector.WithPGVectorClientDSN(pgDSN),
		pgvector.WithTable(table),
		pgvector.WithIndexDimension(dim),
	)
	if err != nil {
		return nil, fmt.Errorf("pgvector store: %w", err)
	}
	return knowledge.New(
		knowledge.WithVectorStore(vs),
		knowledge.WithEmbedder(emb),
	), nil
}

// Metadata keys that scope a document to one tenant's app. The write side
// (DocSource), the search filter (assemble) and the admin ingestion endpoint
// all use these, so a document is always filed and found under the same pair.
const (
	MetadataTenantID = "tenant_id"
	MetadataAppID    = "app_id"
)

// knowledgeFilter scopes one agent's knowledge search to a tenant/app pair.
//
// Both keys are always present: the framework reads an empty filter as "no
// predicate", so omitting it for want of a resolved tenant would search the
// whole shared table and answer from every tenant's documents. The empty pair
// matches nothing instead — admin ingestion always files a document under a
// real tenant and app, so no row satisfies an empty tenant_id.
func knowledgeFilter(tenantID, appID string) map[string]any {
	return map[string]any{MetadataTenantID: tenantID, MetadataAppID: appID}
}

// DocSource is an inline single-document source for admin ingestion: the
// document comes from the request body instead of a file or URL.
type DocSource struct {
	DocName  string
	Content  string
	Metadata map[string]any
}

// ReadDocuments implements source.Source. The document ID is a content hash
// scoped to the owning tenant and app: re-ingesting the same document upserts
// instead of duplicating, while the same name and content under a different
// tenant is a different document.
//
// The scope has to be in the ID, not just the metadata: the vector store
// upserts with ON CONFLICT (id) DO UPDATE over every column, metadata
// included, so an unscoped ID let one tenant's ingest silently rewrite
// another tenant's row — content, embedding and the tenant_id that decides
// who can still retrieve it.
func (s *DocSource) ReadDocuments(context.Context) ([]*document.Document, error) {
	h := fnv.New64a()
	for _, part := range []string{
		metadataString(s.Metadata, MetadataTenantID),
		metadataString(s.Metadata, MetadataAppID),
		s.DocName,
		s.Content,
	} {
		// A length prefix keeps the boundaries unambiguous: without it
		// ("t1","a1x") and ("t1a","1x") would hash alike.
		_, _ = h.Write([]byte(strconv.Itoa(len(part))))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(part))
	}
	return []*document.Document{{
		ID:       strconv.FormatUint(h.Sum64(), 16),
		Name:     s.DocName,
		Content:  s.Content,
		Metadata: s.Metadata,
	}}, nil
}

// metadataString reads one metadata key as a string, tolerating the numeric
// and nil shapes a JSON round trip can produce.
func metadataString(md map[string]any, key string) string {
	switch v := md[key].(type) {
	case string:
		return v
	case nil:
		return ""
	default:
		return fmt.Sprint(v)
	}
}

// Name implements source.Source.
func (s *DocSource) Name() string { return s.DocName }

// Type implements source.Source.
func (s *DocSource) Type() string { return "inline" }

// GetMetadata implements source.Source.
func (s *DocSource) GetMetadata() map[string]any { return s.Metadata }
