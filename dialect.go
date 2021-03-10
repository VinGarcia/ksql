package ksql

import "strconv"

type dialect interface {
	Escape(str string) string
	Placeholder(idx int) string
}

type postgresDialect struct{}

func (postgresDialect) Escape(str string) string {
	return `"` + str + `"`
}

func (postgresDialect) Placeholder(idx int) string {
	return "$" + strconv.Itoa(idx+1)
}

type sqlite3Dialect struct{}

func (sqlite3Dialect) Escape(str string) string {
	return "`" + str + "`"
}

func (sqlite3Dialect) Placeholder(idx int) string {
	return "?"
}

func getDriverDialect(driver string) dialect {
	return map[string]dialect{
		"postgres": &postgresDialect{},
		"sqlite3":  &sqlite3Dialect{},
	}[driver]
}
