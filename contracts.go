package ksql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/pkg/errors"
)

// ErrRecordNotFound ...
var ErrRecordNotFound error = errors.Wrap(sql.ErrNoRows, "ksql: the query returned no results")

// ErrAbortIteration ...
var ErrAbortIteration error = fmt.Errorf("ksql: abort iteration, should only be used inside QueryChunks function")

// SQLProvider describes the public behavior of this ORM
type SQLProvider interface {
	Insert(ctx context.Context, record interface{}) error
	Update(ctx context.Context, record interface{}) error
	Delete(ctx context.Context, idsOrRecords ...interface{}) error

	Query(ctx context.Context, records interface{}, query string, params ...interface{}) error
	QueryOne(ctx context.Context, record interface{}, query string, params ...interface{}) error
	QueryChunks(ctx context.Context, parser ChunkParser) error

	Exec(ctx context.Context, query string, params ...interface{}) error
	Transaction(ctx context.Context, fn func(SQLProvider) error) error
}

// ChunkParser stores the arguments of the QueryChunks function
type ChunkParser struct {
	// The Query and Params are used together to build a query with
	// protection from injection, just like when using the Find function.
	Query  string
	Params []interface{}

	ChunkSize int

	// This attribute must be a function with the following signature:
	//
	// `func(chunk []<Record>) error`.
	//
	// Where the actual Record type should be of a struct
	// representing the rows you are expecting to receive.
	ForEachChunk interface{}
}
