package internal

import (
	"context"

	"github.com/vingarcia/ksql"
)

// MockDBAdapter mocks the ksql.DBAdapter interface
type MockDBAdapter struct {
	ExecContextFn  func(ctx context.Context, query string, args ...interface{}) (ksql.Result, error)
	QueryContextFn func(ctx context.Context, query string, args ...interface{}) (ksql.Rows, error)
}

func (m MockDBAdapter) ExecContext(ctx context.Context, query string, args ...interface{}) (ksql.Result, error) {
	return m.ExecContextFn(ctx, query, args...)
}

func (m MockDBAdapter) QueryContext(ctx context.Context, query string, args ...interface{}) (ksql.Rows, error) {
	return m.QueryContextFn(ctx, query, args...)
}

type MockRows struct {
	ScanFn    func(...interface{}) error
	CloseFn   func() error
	NextFn    func() bool
	ErrFn     func() error
	ColumnsFn func() ([]string, error)
}

func (m MockRows) Scan(values ...interface{}) error {
	return m.ScanFn(values...)
}

func (m MockRows) Close() error {
	if m.CloseFn == nil {
		return nil
	}
	return m.CloseFn()
}

func (m MockRows) Next() bool {
	return m.NextFn()
}

func (m MockRows) Err() error {
	if m.ErrFn == nil {
		return nil
	}
	return m.ErrFn()
}

func (m MockRows) Columns() ([]string, error) {
	return m.ColumnsFn()
}

// MockResult mocks the ksql.Result interface
type MockResult struct {
	LastInsertIdFn func() (int64, error)
	RowsAffectedFn func() (int64, error)
}

func (m MockResult) LastInsertId() (int64, error) {
	if m.LastInsertIdFn != nil {
		return m.LastInsertIdFn()
	}
	return 0, nil
}

func (m MockResult) RowsAffected() (int64, error) {
	if m.RowsAffectedFn != nil {
		return m.RowsAffectedFn()
	}
	return 0, nil
}
