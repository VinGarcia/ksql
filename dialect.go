package ksql

import (
	"fmt"
	"strconv"
)

// Dialect is used to represent the different ways
// of writing SQL queries used by each SQL driver.
type Dialect interface {
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

// GetDriverDialect instantiantes the dialect for the
// provided driver string, if the drive is not supported
// it returns an error
func GetDriverDialect(driver string) (Dialect, error) {
	dialect, found := map[string]Dialect{
		"postgres": &postgresDialect{},
		"sqlite3":  &sqlite3Dialect{},
	}[driver]
	if !found {
		return nil, fmt.Errorf("unsupported driver `%s`", driver)
	}

	return dialect, nil
}
