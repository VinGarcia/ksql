package kpostgres

import (
	"context"
	"database/sql"

	"github.com/vingarcia/ksql"
	"strconv"
	"strings"
	"unicode"
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
	rows, err := s.DB.QueryContext(ctx, query, args...)
	return SQLRows{rows}, err
}

// BeginTx implements the Tx interface
func (s SQLAdapter) BeginTx(ctx context.Context) (ksql.Tx, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	return SQLTx{Tx: tx}, err
}

// Close implements the io.Closer interface
func (s SQLAdapter) Close() error {
	return s.DB.Close()
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
	rows, err := s.Tx.QueryContext(ctx, query, args...)
	return SQLRows{rows}, err
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

// SQLRows implements the ksql.Rows interface and is used to help
// the SQLAdapter to implement the ksql.DBAdapter interface.
type SQLRows struct {
	*sql.Rows
}

var _ ksql.Rows = SQLRows{}

// Scan implements the ksql.Rows interface
func (p SQLRows) Scan(args ...interface{}) error {
	err := p.Rows.Scan(args...)
	if err != nil {
		// Since this is the error flow we decided it would be ok
		// to spend a little bit more time parsing this error in order
		// to produce better error messages.
		//
		// If the parsing fails we just return the error unchanged.
		const scanErrPrefix = "sql: Scan error on column index "
		var errMsg = err.Error()
		if strings.HasPrefix(errMsg, scanErrPrefix) {
			i := len(scanErrPrefix)
			for unicode.IsDigit(rune(errMsg[i])) {
				i++
			}
			colIndex, convErr := strconv.Atoi(errMsg[len(scanErrPrefix):i])
			if convErr == nil {
				return ksql.ScanArgError{
					ColumnIndex: colIndex,
					Err:         err,
				}
			}
		}
	}

	return err
}
