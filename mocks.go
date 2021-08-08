package ksql

import "context"

var _ Provider = Mock{}

// Mock ...
type Mock struct {
	InsertFn func(ctx context.Context, table Table, record interface{}) error
	UpdateFn func(ctx context.Context, table Table, record interface{}) error
	DeleteFn func(ctx context.Context, table Table, ids ...interface{}) error

	QueryFn       func(ctx context.Context, records interface{}, query string, params ...interface{}) error
	QueryOneFn    func(ctx context.Context, record interface{}, query string, params ...interface{}) error
	QueryChunksFn func(ctx context.Context, parser ChunkParser) error

	ExecFn        func(ctx context.Context, query string, params ...interface{}) error
	TransactionFn func(ctx context.Context, fn func(db Provider) error) error
}

// Insert ...
func (m Mock) Insert(ctx context.Context, table Table, record interface{}) error {
	return m.InsertFn(ctx, table, record)
}

// Update ...
func (m Mock) Update(ctx context.Context, table Table, record interface{}) error {
	return m.UpdateFn(ctx, table, record)
}

// Delete ...
func (m Mock) Delete(ctx context.Context, table Table, ids ...interface{}) error {
	return m.DeleteFn(ctx, table, ids...)
}

// Query ...
func (m Mock) Query(ctx context.Context, records interface{}, query string, params ...interface{}) error {
	return m.QueryFn(ctx, records, query, params...)
}

// QueryOne ...
func (m Mock) QueryOne(ctx context.Context, record interface{}, query string, params ...interface{}) error {
	return m.QueryOneFn(ctx, record, query, params...)
}

// QueryChunks ...
func (m Mock) QueryChunks(ctx context.Context, parser ChunkParser) error {
	return m.QueryChunksFn(ctx, parser)
}

// Exec ...
func (m Mock) Exec(ctx context.Context, query string, params ...interface{}) error {
	return m.ExecFn(ctx, query, params...)
}

// Transaction ...
func (m Mock) Transaction(ctx context.Context, fn func(db Provider) error) error {
	return m.TransactionFn(ctx, fn)
}
