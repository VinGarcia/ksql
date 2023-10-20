package ksqlite

import (
	"database/sql"
	"io"
	"testing"

	"github.com/vingarcia/ksql"
	"github.com/vingarcia/ksql/sqldialect"
)

func TestAdapter(t *testing.T) {
	ksql.RunTestsForAdapter(t, "modernc-ksqlite", sqldialect.Sqlite3Dialect{}, "/tmp/modernc-ksqlite.db", func(t *testing.T) (ksql.DBAdapter, io.Closer) {
		db, err := sql.Open("sqlite", "/tmp/modernc-ksqlite.db")
		if err != nil {
			t.Fatal(err.Error())
		}
		return SQLAdapter{db}, db
	})
}
