package kpostgres

import (
	"database/sql"

	"github.com/vingarcia/ksql"
	"github.com/vingarcia/ksql/sqldialect"
)

// NewFromSQLDB builds a ksql.DB from a *sql.DB instance
func NewFromSQLDB(db *sql.DB) (ksql.DB, error) {
	return ksql.NewWithAdapter(NewSQLAdapter(db), sqldialect.PostgresDialect{})
}
