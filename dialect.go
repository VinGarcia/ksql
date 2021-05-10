package ksql

import "strconv"

type insertMethod int

const (
	insertWithReturning insertMethod = iota
	insertWithOutput
	insertWithLastInsertID
	insertWithNoIDRetrieval
)

var supportedDialects = map[string]dialect{
	"postgres":  &postgresDialect{},
	"sqlite3":   &sqlite3Dialect{},
	"mysql":     &mysqlDialect{},
	"sqlserver": &sqlserverDialect{},
}

type dialect interface {
	InsertMethod() insertMethod
	Escape(str string) string
	Placeholder(idx int) string
	DriverName() string
}

type postgresDialect struct{}

func (postgresDialect) DriverName() string {
	return "postgres"
}

func (postgresDialect) InsertMethod() insertMethod {
	return insertWithReturning
}

func (postgresDialect) Escape(str string) string {
	return `"` + str + `"`
}

func (postgresDialect) Placeholder(idx int) string {
	return "$" + strconv.Itoa(idx+1)
}

type sqlite3Dialect struct{}

func (sqlite3Dialect) DriverName() string {
	return "sqlite3"
}

func (sqlite3Dialect) InsertMethod() insertMethod {
	return insertWithLastInsertID
}

func (sqlite3Dialect) Escape(str string) string {
	return "`" + str + "`"
}

func (sqlite3Dialect) Placeholder(idx int) string {
	return "?"
}

type mysqlDialect struct{}

func (mysqlDialect) DriverName() string {
	return "mysql"
}

func (mysqlDialect) InsertMethod() insertMethod {
	return insertWithLastInsertID
}

func (mysqlDialect) Escape(str string) string {
	return "`" + str + "`"
}

func (mysqlDialect) Placeholder(idx int) string {
	return "?"
}

type sqlserverDialect struct{}

func (sqlserverDialect) DriverName() string {
	return "sqlserver"
}

func (sqlserverDialect) InsertMethod() insertMethod {
	return insertWithOutput
}

func (sqlserverDialect) Escape(str string) string {
	return `[` + str + `]`
}

func (sqlserverDialect) Placeholder(idx int) string {
	return "@p" + strconv.Itoa(idx+1)
}
