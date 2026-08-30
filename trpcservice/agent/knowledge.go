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

// DocSource is an inline single-document source for admin ingestion: the
// document comes from the request body instead of a file or URL.
type DocSource struct {
	DocName  string
	Content  string
	Metadata map[string]any
}

// ReadDocuments implements source.Source. The document ID is a content hash:
// re-ingesting the same content upserts instead of duplicating.
func (s *DocSource) ReadDocuments(context.Context) ([]*document.Document, error) {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s.DocName))
	_, _ = h.Write([]byte(s.Content))
	return []*document.Document{{
		ID:       strconv.FormatUint(h.Sum64(), 16),
		Name:     s.DocName,
		Content:  s.Content,
		Metadata: s.Metadata,
	}}, nil
}

// Name implements source.Source.
func (s *DocSource) Name() string { return s.DocName }

// Type implements source.Source.
func (s *DocSource) Type() string { return "inline" }

// GetMetadata implements source.Source.
func (s *DocSource) GetMetadata() map[string]any { return s.Metadata }
