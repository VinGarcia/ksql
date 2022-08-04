package ksql

import "context"

// mockTxBeginner mocks the ksql.TxBeginner interface
type mockTxBeginner struct {
	DBAdapter
	BeginTxFn func(ctx context.Context) (Tx, error)
}

func (b mockTxBeginner) BeginTx(ctx context.Context) (Tx, error) {
	return b.BeginTxFn(ctx)
}

// mockDBAdapter mocks the ksql.DBAdapter interface
type mockDBAdapter struct {
	ExecContextFn  func(ctx context.Context, query string, args ...interface{}) (Result, error)
	QueryContextFn func(ctx context.Context, query string, args ...interface{}) (Rows, error)
}

func (m mockDBAdapter) ExecContext(ctx context.Context, query string, args ...interface{}) (Result, error) {
	return m.ExecContextFn(ctx, query, args...)
}

func (m mockDBAdapter) QueryContext(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	return m.QueryContextFn(ctx, query, args...)
}

// mockTx mocks the ksql.Tx interface
type mockTx struct {
	DBAdapter
	RollbackFn func(ctx context.Context) error
	CommitFn   func(ctx context.Context) error
}

func (m mockTx) Rollback(ctx context.Context) error {
	return m.RollbackFn(ctx)
}

func (m mockTx) Commit(ctx context.Context) error {
	return m.CommitFn(ctx)
}

// mockCloser mocks the io.Closer interface
type mockCloser struct {
	CloseFn func() error
}

func (m mockCloser) Close() error {
	return m.CloseFn()
}
