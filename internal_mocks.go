package ksql

import (
	"context"
)

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

type mockRows struct {
	ScanFn    func(...interface{}) error
	CloseFn   func() error
	NextFn    func() bool
	ErrFn     func() error
	ColumnsFn func() ([]string, error)
}

func (m mockRows) Scan(values ...interface{}) error {
	return m.ScanFn(values...)
}

func (m mockRows) Close() error {
	if m.CloseFn == nil {
		return nil
	}
	return m.CloseFn()
}

func (m mockRows) Next() bool {
	return m.NextFn()
}

func (m mockRows) Err() error {
	if m.ErrFn == nil {
		return nil
	}
	return m.ErrFn()
}

func (m mockRows) Columns() ([]string, error) {
	return m.ColumnsFn()
}

// mockResult mocks the ksql.Result interface
type mockResult struct {
	LastInsertIdFn func() (int64, error)
	RowsAffectedFn func() (int64, error)
}

func (m mockResult) LastInsertId() (int64, error) {
	if m.LastInsertIdFn != nil {
		return m.LastInsertIdFn()
	}
	return 0, nil
}

func (m mockResult) RowsAffected() (int64, error) {
	if m.RowsAffectedFn != nil {
		return m.RowsAffectedFn()
	}
	return 0, nil
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
