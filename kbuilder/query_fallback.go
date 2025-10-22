//go:build !ksql_enable_kbuilder_experiment

package kbuilder

import (
	"github.com/vingarcia/ksql/sqldialect"
)

// This is the fallback file for kbuilder package when the ksql_enable_kbuilder_experiment
// build tag is not enabled, check the file named `query.go` to see the actual implementation.

// Query is is the struct template for building SELECT queries.
type Query struct {
	// Select expects either a struct using the `ksql` tags
	// or a string listing the column names using SQL syntax,
	// e.g.: `id, username, address`
	Select interface{}

	// From expects the FROM clause from an SQL query, e.g. `users JOIN posts USING(post_id)`
	From string

	// Where expects a list of WhereQuery instances built
	// by the public Where() function.
	Where WhereQueries

	Limit   int
	Offset  int
	OrderBy OrderByQuery
}

// Build is a utility function for finding the dialect based on the driver and
// then calling BuildQuery(dialect)
func (q Query) Build(driver string) (sqlQuery string, params []interface{}, _ error) {
	panic("kbuilder is an experimental package and needs to be explicitly enabled see github.com/vingarcia/ksql/tree/master/kbuilder/README.md")
}

// BuildQuery implements the queryBuilder interface
func (q Query) BuildQuery(dialect sqldialect.Provider) (sqlQuery string, params []interface{}, _ error) {
	panic("kbuilder is an experimental package and needs to be explicitly enabled see github.com/vingarcia/ksql/tree/master/kbuilder/README.md")
}

// WhereQuery represents a single condition in a WHERE expression.
type WhereQuery struct{}

// WhereQueries is the helper for creating complex WHERE queries
// in a dynamic way.
type WhereQueries []WhereQuery

// Where adds a new boolean condition to an existing
// WhereQueries helper.
func (w WhereQueries) Where(cond string, params ...interface{}) WhereQueries {
	panic("kbuilder is an experimental package and needs to be explicitly enabled see github.com/vingarcia/ksql/tree/master/kbuilder/README.md")
}

// WhereIf conditionally adds a new boolean expression to the WhereQueries helper.
func (w WhereQueries) WhereIf(ifConf bool, cond string, params ...interface{}) WhereQueries {
	panic("kbuilder is an experimental package and needs to be explicitly enabled see github.com/vingarcia/ksql/tree/master/kbuilder/README.md")
}

// Where adds a new boolean condition to an existing
// WhereQueries helper.
func Where(cond string, params ...interface{}) WhereQueries {
	panic("kbuilder is an experimental package and needs to be explicitly enabled see github.com/vingarcia/ksql/tree/master/kbuilder/README.md")
}

// WhereIf conditionally adds a new boolean expression to the WhereQueries helper
func WhereIf(ifCond bool, cond string, params ...interface{}) WhereQueries {
	panic("kbuilder is an experimental package and needs to be explicitly enabled see github.com/vingarcia/ksql/tree/master/kbuilder/README.md")
}

// OrderByQuery represents the ORDER BY part of the query
type OrderByQuery struct{}

// Desc is a setter function for configuring the
// ORDER BY part of the query as DESC
func (o OrderByQuery) Desc() OrderByQuery {
	panic("kbuilder is an experimental package and needs to be explicitly enabled see github.com/vingarcia/ksql/tree/master/kbuilder/README.md")
}

// OrderBy is a helper for building the ORDER BY
// part of the query.
func OrderBy(fields string) OrderByQuery {
	panic("kbuilder is an experimental package and needs to be explicitly enabled see github.com/vingarcia/ksql/tree/master/kbuilder/README.md")
}
