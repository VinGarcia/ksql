package benchmarks

import (
	"context"
	"database/sql"

	"github.com/vingarcia/ksql"
)

// SQLAdapter adapts the sql.DB type to be compatible with the `DBAdapter` interface
type SQLAdapter struct {
	*sql.DB
}

var _ ksql.DBAdapter = SQLAdapter{}

// NewSQLAdapter returns a new instance of SQLAdapter with
// the provided database instance.
func NewSQLAdapter(db *sql.DB) SQLAdapter {
	return SQLAdapter{
		DB: db,
	}
}

// ExecContext implements the DBAdapter interface
func (s SQLAdapter) ExecContext(ctx context.Context, query string, args ...interface{}) (ksql.Result, error) {
	return s.DB.ExecContext(ctx, query, args...)
}

// QueryContext implements the DBAdapter interface
func (s SQLAdapter) QueryContext(ctx context.Context, query string, args ...interface{}) (ksql.Rows, error) {
	return s.DB.QueryContext(ctx, query, args...)
}

// BeginTx implements the Tx interface
func (s SQLAdapter) BeginTx(ctx context.Context) (ksql.Tx, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	return SQLTx{Tx: tx}, err
}

// SQLTx is used to implement the DBAdapter interface and implements
// the Tx interface
type SQLTx struct {
	*sql.Tx
}

// ExecContext implements the Tx interface
func (s SQLTx) ExecContext(ctx context.Context, query string, args ...interface{}) (ksql.Result, error) {
	return s.Tx.ExecContext(ctx, query, args...)
}

// QueryContext implements the Tx interface
func (s SQLTx) QueryContext(ctx context.Context, query string, args ...interface{}) (ksql.Rows, error) {
	return s.Tx.QueryContext(ctx, query, args...)
}

// Rollback implements the Tx interface
func (s SQLTx) Rollback(ctx context.Context) error {
	return s.Tx.Rollback()
}

// Commit implements the Tx interface
func (s SQLTx) Commit(ctx context.Context) error {
	return s.Tx.Commit()
}

var _ ksql.Tx = SQLTx{}
