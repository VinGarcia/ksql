package kissorm

import (
	"context"
	"fmt"
)

// EntityNotFoundErr ...
var EntityNotFoundErr error = fmt.Errorf("kissorm: the query returned no results")

// AbortIteration ...
var AbortIteration error = fmt.Errorf("kissorm: abort iteration, should only be used inside QueryChunks function")

// ORMProvider describes the public behavior of this ORM
type ORMProvider interface {
	Insert(ctx context.Context, records ...interface{}) error
	Delete(ctx context.Context, ids ...interface{}) error
	Update(ctx context.Context, records ...interface{}) error

	Query(ctx context.Context, records interface{}, query string, params ...interface{}) error
	QueryOne(ctx context.Context, record interface{}, query string, params ...interface{}) error
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
	// filling the Chunk with ChunkSize records
	//
	// Each chunk consecutively parsed will overwrite the
	// same slice, so don't keep references to it, if you
	// need some data to be preserved after all chunks are
	// processed copy the records by value.
	ForEachChunk func() error
}
