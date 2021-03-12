package ksql

import "context"

var _ SQLProvider = MockSQLProvider{}

// MockSQLProvider ...
type MockSQLProvider struct {
	InsertFn func(ctx context.Context, record interface{}) error
	UpdateFn func(ctx context.Context, record interface{}) error
	DeleteFn func(ctx context.Context, ids ...interface{}) error

	QueryFn       func(ctx context.Context, records interface{}, query string, params ...interface{}) error
	QueryOneFn    func(ctx context.Context, record interface{}, query string, params ...interface{}) error
	QueryChunksFn func(ctx context.Context, parser ChunkParser) error

	ExecFn        func(ctx context.Context, query string, params ...interface{}) error
	TransactionFn func(ctx context.Context, fn func(db SQLProvider) error) error
}

// Insert ...
func (m MockSQLProvider) Insert(ctx context.Context, record interface{}) error {
	return m.InsertFn(ctx, record)
}

// Update ...
func (m MockSQLProvider) Update(ctx context.Context, record interface{}) error {
	return m.UpdateFn(ctx, record)
}

// Delete ...
func (m MockSQLProvider) Delete(ctx context.Context, ids ...interface{}) error {
	return m.DeleteFn(ctx, ids...)
}

// Query ...
func (m MockSQLProvider) Query(ctx context.Context, records interface{}, query string, params ...interface{}) error {
	return m.QueryFn(ctx, records, query, params...)
}

// QueryOne ...
func (m MockSQLProvider) QueryOne(ctx context.Context, record interface{}, query string, params ...interface{}) error {
	return m.QueryOneFn(ctx, record, query, params...)
}

// QueryChunks ...
func (m MockSQLProvider) QueryChunks(ctx context.Context, parser ChunkParser) error {
	return m.QueryChunksFn(ctx, parser)
}

// Exec ...
func (m MockSQLProvider) Exec(ctx context.Context, query string, params ...interface{}) error {
	return m.ExecFn(ctx, query, params...)
}

// Transaction ...
func (m MockSQLProvider) Transaction(ctx context.Context, fn func(db SQLProvider) error) error {
	return m.TransactionFn(ctx, fn)
}
