package ksql

import (
	"context"
	"database/sql"
)

// SQLAdapter adapts the sql.DB type to be compatible with the `DBAdapter` interface
type SQLAdapter struct {
	*sql.DB
}

var _ DBAdapter = SQLAdapter{}

// ExecContext implements the DBAdapter interface
func (s SQLAdapter) ExecContext(ctx context.Context, query string, args ...interface{}) (Result, error) {
	return s.DB.ExecContext(ctx, query, args...)
}

// QueryContext implements the DBAdapter interface
func (s SQLAdapter) QueryContext(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	return s.DB.QueryContext(ctx, query, args...)
}

// SQLTx is used to implement the DBAdapter interface and implements
// the Tx interface
type SQLTx struct {
	*sql.Tx
}

// ExecContext implements the Tx interface
func (s SQLTx) ExecContext(ctx context.Context, query string, args ...interface{}) (Result, error) {
	return s.Tx.ExecContext(ctx, query, args...)
}

// QueryContext implements the Tx interface
func (s SQLTx) QueryContext(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	return s.Tx.QueryContext(ctx, query, args...)
}

var _ Tx = SQLTx{}

// BeginTx implements the Tx interface
func (s SQLAdapter) BeginTx(ctx context.Context) (Tx, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	return SQLTx{Tx: tx}, err
}
