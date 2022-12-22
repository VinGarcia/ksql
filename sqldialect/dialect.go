package sqldialect

import (
	"strconv"
)

type InsertMethod int

const (
	InsertWithReturning InsertMethod = iota
	InsertWithOutput
	InsertWithLastInsertID
	InsertWithNoIDRetrieval
)

var SupportedDialects = map[string]Provider{
	"postgres":  &PostgresDialect{},
	"sqlite3":   &Sqlite3Dialect{},
	"mysql":     &MysqlDialect{},
	"sqlserver": &SqlserverDialect{},
}

// Provider or dialect.Provider represents one particular
// way of writing SQL queries.
//
// Different DBAdapters will use require different dialects to work.
type Provider interface {
	InsertMethod() InsertMethod
	Escape(str string) string
	Placeholder(idx int) string
	DriverName() string
}

type PostgresDialect struct{}

func (PostgresDialect) DriverName() string {
	return "postgres"
}

func (PostgresDialect) InsertMethod() InsertMethod {
	return InsertWithReturning
}

func (PostgresDialect) Escape(str string) string {
	return `"` + str + `"`
}

func (PostgresDialect) Placeholder(idx int) string {
	return "$" + strconv.Itoa(idx+1)
}

type Sqlite3Dialect struct{}

func (Sqlite3Dialect) DriverName() string {
	return "sqlite3"
}

func (Sqlite3Dialect) InsertMethod() InsertMethod {
	return InsertWithLastInsertID
}

func (Sqlite3Dialect) Escape(str string) string {
	return "`" + str + "`"
}

func (Sqlite3Dialect) Placeholder(idx int) string {
	return "?"
}

type MysqlDialect struct{}

func (MysqlDialect) DriverName() string {
	return "mysql"
}

func (MysqlDialect) InsertMethod() InsertMethod {
	return InsertWithLastInsertID
}

func (MysqlDialect) Escape(str string) string {
	return "`" + str + "`"
}

func (MysqlDialect) Placeholder(idx int) string {
	return "?"
}

type SqlserverDialect struct{}

func (SqlserverDialect) DriverName() string {
	return "sqlserver"
}

func (SqlserverDialect) InsertMethod() InsertMethod {
	return InsertWithOutput
}

func (SqlserverDialect) Escape(str string) string {
	return `[` + str + `]`
}

func (SqlserverDialect) Placeholder(idx int) string {
	return "@p" + strconv.Itoa(idx+1)
}
