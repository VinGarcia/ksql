package kissorm

import (
	"context"
)

// ORMProvider describes the public behavior of this ORM
type ORMProvider interface {
	Find(ctx context.Context, item interface{}, query string, params ...interface{}) error
	Insert(ctx context.Context, items ...interface{}) error
	Delete(ctx context.Context, ids ...interface{}) error
	Update(ctx context.Context, items ...interface{}) error
	QueryChunks(ctx context.Context, parser ChunkParser) error
}

type ChunkParser struct {
	// The Query and Params are used together to build a query with
	// protection from injection, just like when using the Find function.
	Query  string
	Params []interface{}

	ChunkSize int
	Chunk     interface{} // Must be a pointer to a slice of structs

	// The closure that will be called right after
	// filling the Chunk with ChunkSize items
	//
	// Each chunk consecutively parsed will overwrite the
	// same slice, so don't keep references to it, if you
	// need some data to be preserved after all chunks are
	// processed copy the items by value.
	ForEachChunk func() error
}
