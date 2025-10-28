//go:build !ksql_enable_kbuilder_experiment

package kbuilder

import (
	"github.com/vingarcia/ksql/sqldialect"
)

// This is the fallback file for kbuilder package when the ksql_enable_kbuilder_experiment
// build tag is not enabled, check the file named `insert.go` to see the actual implementation.

// Insert is the struct template for building INSERT queries
type Insert struct {
	// Into expects a table name, e.g. "users"
	Into string

	// Data expected either a single record annotated with `ksql` tags
	// or a list of records annotated likewise.
	Data interface{}

	// OmitColumns informs kbuilder of a set of columns not to use during the insertion
	OmitColumns []string

	// Returning causes the query to be built in a way that the selected attributes
	// will be returned after the insertion.
	Returning []string
}

// Build is a utility function for finding the dialect based on the driver and
// then calling BuildQuery(dialect)
func (i Insert) Build(driver string) (sqlQuery string, params []interface{}, _ error) {
	_ = driver

	panic("kbuilder is an experimental package and needs to be explicitly enabled see github.com/vingarcia/ksql/tree/master/kbuilder/README.md")
}

// BuildQuery implements the queryBuilder interface
func (i Insert) BuildQuery(dialect sqldialect.Provider) (sqlQuery string, params []interface{}, _ error) {
	_ = dialect

	panic("kbuilder is an experimental package and needs to be explicitly enabled see github.com/vingarcia/ksql/tree/master/kbuilder/README.md")
}
