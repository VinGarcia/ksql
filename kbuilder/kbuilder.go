package kbuilder

import (
	"context"
	"fmt"

	"github.com/vingarcia/ksql"
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

// RunAndCount runs a paginated input query once returning the values
// and then runs it again just for counting the total number of results.
func (builder *Builder) RunAndCount(ctx context.Context, db ksql.Provider, targetPtr interface{}, queryBuilder Query) (count int, err error) {
	query, params, err := builder.Build(queryBuilder)
	if err != nil {
		return 0, err
	}

	err = db.Query(ctx, targetPtr, query, params...)
	if err != nil {
		return 0, err
	}

	queryBuilder.Select = "count(*) AS c"
	queryBuilder.Offset = 0
	queryBuilder.Limit = 0
	queryBuilder.OrderBy = ""

	query, params, err = builder.Build(queryBuilder)
	if err != nil {
		return 0, err
	}

	var row struct {
		C int `ksql:"c"`
	}
	err = db.QueryOne(ctx, &row, query, params...)
	if err != nil {
		return 0, err
	}

	return row.C, nil
}
