package ksql

import "strconv"

type insertMethod int

const (
	insertWithReturning insertMethod = iota
	insertWithLastInsertID
	insertWithNoIDRetrieval
)

var supportedDialects = map[string]dialect{
	"postgres": &postgresDialect{},
	"sqlite3":  &sqlite3Dialect{},
	"mysql":    &mysqlDialect{},
}

type dialect interface {
	InsertMethod() insertMethod
	Escape(str string) string
	Placeholder(idx int) string
}

type postgresDialect struct{}

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

func (mysqlDialect) InsertMethod() insertMethod {
	return insertWithLastInsertID
}

func (mysqlDialect) Escape(str string) string {
	return "`" + str + "`"
}

func (mysqlDialect) Placeholder(idx int) string {
	return "?"
}
