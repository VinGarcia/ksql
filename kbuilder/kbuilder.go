package kbuilder

import (
	"github.com/vingarcia/ksql"
)

// Builder is the basic container for injecting
// query builder configurations.
//
// All the Query structs can also be called
// directly without this builder, but we kept it
// here for convenience.
type Builder struct {
	dialect ksql.Dialect
}

type queryBuilder interface {
	BuildQuery(dialect ksql.Dialect) (sqlQuery string, params []interface{}, _ error)
}

// New creates a new Builder container.
func New(driver string) (Builder, error) {
	dialect, err := ksql.GetDriverDialect(driver)
	return Builder{
		dialect: dialect,
	}, err
}

// Build receives a query builder struct, injects it with the configurations
// build the query according to its arguments.
func (builder *Builder) Build(query queryBuilder) (sqlQuery string, params []interface{}, _ error) {
	return query.BuildQuery(builder.dialect)
}
