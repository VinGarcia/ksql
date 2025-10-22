package kbuilder

import (
	"fmt"

	"github.com/vingarcia/ksql/sqldialect"
)

// Builder is the basic container for injecting
// query builder configurations.
//
// All the Query structs can also be called
// directly without this builder, but we kept it
// here for convenience.
type Builder struct {
	dialect sqldialect.Provider
}

type queryBuilder interface {
	BuildQuery(dialect sqldialect.Provider) (sqlQuery string, params []interface{}, _ error)
}

// New creates a new Builder container.
func New(driver string) (Builder, error) {
	dialect, ok := sqldialect.SupportedDialects[driver]
	if !ok {
		return Builder{}, fmt.Errorf("unsupported driver `%s`", driver)
	}

	return Builder{
		dialect: dialect,
	}, nil
}

// Build receives a query builder struct, injects it with the configurations
// build the query according to its arguments.
func (builder *Builder) Build(query queryBuilder) (sqlQuery string, params []interface{}, _ error) {
	return query.BuildQuery(builder.dialect)
}
