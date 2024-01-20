package kpostgres

import (
	"database/sql"

	"github.com/vingarcia/ksql"
	"github.com/vingarcia/ksql/sqldialect"
)

func NewFromSQLDB(db *sql.DB) (ksql.DB, error) {
	return ksql.NewWithAdapter(NewSQLAdapter(db), sqldialect.PostgresDialect{})
}
