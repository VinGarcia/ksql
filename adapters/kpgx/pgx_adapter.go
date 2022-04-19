package kpgx

import (
	"context"
	"fmt"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/vingarcia/ksql"
)

// PGXAdapter adapts the sql.DB type to be compatible with the `DBAdapter` interface
type PGXAdapter struct {
	db *pgxpool.Pool
}

// NewPGXAdapter instantiates a new pgx adapter
func NewPGXAdapter(db *pgxpool.Pool) PGXAdapter {
	return PGXAdapter{
		db: db,
	}
}

var _ ksql.DBAdapter = PGXAdapter{}

// ExecContext implements the DBAdapter interface
func (p PGXAdapter) ExecContext(ctx context.Context, query string, args ...interface{}) (ksql.Result, error) {
	result, err := p.db.Exec(ctx, query, args...)
	return PGXResult{result}, err
}

// QueryContext implements the DBAdapter interface
func (p PGXAdapter) QueryContext(ctx context.Context, query string, args ...interface{}) (ksql.Rows, error) {
	rows, err := p.db.Query(ctx, query, args...)
	return PGXRows{rows}, err
}

// BeginTx implements the Tx interface
func (p PGXAdapter) BeginTx(ctx context.Context) (ksql.Tx, error) {
	tx, err := p.db.Begin(ctx)
	return PGXTx{tx}, err
}

// Close implements the io.Closer interface
func (p PGXAdapter) Close() error {
	p.db.Close()
	return nil
}

// PGXResult is used to implement the DBAdapter interface and implements
// the Result interface
type PGXResult struct {
	tag pgconn.CommandTag
}

// RowsAffected implements the Result interface
func (p PGXResult) RowsAffected() (int64, error) {
	return p.tag.RowsAffected(), nil
}

// LastInsertId implements the Result interface
func (p PGXResult) LastInsertId() (int64, error) {
	return 0, fmt.Errorf(
		"LastInsertId is not implemented in the pgx adapter, use the `RETURNING` statement instead",
	)
}

// PGXTx is used to implement the DBAdapter interface and implements
// the Tx interface
type PGXTx struct {
	tx pgx.Tx
}

// ExecContext implements the Tx interface
func (p PGXTx) ExecContext(ctx context.Context, query string, args ...interface{}) (ksql.Result, error) {
	result, err := p.tx.Exec(ctx, query, args...)
	return PGXResult{result}, err
}

// QueryContext implements the Tx interface
func (p PGXTx) QueryContext(ctx context.Context, query string, args ...interface{}) (ksql.Rows, error) {
	rows, err := p.tx.Query(ctx, query, args...)
	return PGXRows{rows}, err
}

// Rollback implements the Tx interface
func (p PGXTx) Rollback(ctx context.Context) error {
	return p.tx.Rollback(ctx)
}

// Commit implements the Tx interface
func (p PGXTx) Commit(ctx context.Context) error {
	return p.tx.Commit(ctx)
}

var _ ksql.Tx = PGXTx{}

// PGXRows implements the Rows interface and is used to help
// the PGXAdapter to implement the DBAdapter interface.
type PGXRows struct {
	pgx.Rows
}

var _ ksql.Rows = PGXRows{}

// Columns implements the Rows interface
func (p PGXRows) Columns() ([]string, error) {
	var names []string
	for _, desc := range p.Rows.FieldDescriptions() {
		names = append(names, string(desc.Name))
	}
	return names, nil
}

// Close implements the Rows interface
func (p PGXRows) Close() error {
	p.Rows.Close()
	return nil
}
