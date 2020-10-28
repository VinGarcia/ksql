package kissorm

import (
	"context"
	"fmt"
)

// ErrRecordNotFound ...
var ErrRecordNotFound error = fmt.Errorf("kissorm: the query returned no results")

// ErrAbortIteration ...
var ErrAbortIteration error = fmt.Errorf("kissorm: abort iteration, should only be used inside QueryChunks function")

// ORMProvider describes the public behavior of this ORM
type ORMProvider interface {
	Insert(ctx context.Context, records ...interface{}) error
	Delete(ctx context.Context, ids ...interface{}) error
	Update(ctx context.Context, records ...interface{}) error

	Query(ctx context.Context, records interface{}, query string, params ...interface{}) error
	QueryOne(ctx context.Context, record interface{}, query string, params ...interface{}) error
	QueryChunks(ctx context.Context, parser ChunkParser) error
}

// ChunkParser stores the arguments of the QueryChunks function
type ChunkParser struct {
	// The Query and Params are used together to build a query with
	// protection from injection, just like when using the Find function.
	Query  string
	Params []interface{}

	ChunkSize int

	// This attribute must be a func(chunk []<Record>) error,
	// where the actual Record should be a struct
	// representing the rows you are expecting to receive.
	ForEachChunk interface{}
}
