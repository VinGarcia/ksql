package kbuilder

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

type Builder struct {
	driver string
}

func New(driver string) Builder {
	return Builder{
		driver: driver,
	}
}

func (_ *Builder) Build(query Query) (sqlQuery string, params []interface{}, _ error) {
	var b strings.Builder
	// TODO: Actually build the Select from the struct:
	b.WriteString(fmt.Sprint("SELECT", query.Select))

	b.WriteString(" FROM " + query.From)

	if len(query.Where) > 0 {
		b.WriteString(" WHERE " + query.Where.build())
	}

	if query.OrderBy.fields != "" {
		b.WriteString(" ORDER BY " + query.OrderBy.fields)
		if query.OrderBy.desc {
			b.WriteString(" DESC")
		}
	}

	if query.Limit > 0 {
		b.WriteString(" LIMIT " + strconv.Itoa(query.Limit))
	}

	if query.Offset > 0 {
		b.WriteString(" OFFSET " + strconv.Itoa(query.Limit))
	}

	return b.String(), []interface{}{}, nil
}

type Query struct {
	// Select expects a struct using the `ksql` tags
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

type WhereQuery struct {
	// Accepts any SQL boolean expression
	// This expression may optionally contain
	// string formatting directives %s and only %s.
	//
	// For each of these directives we expect a new param
	// on the params list below.
	//
	// In the resulting query each %s will be properly replaced
	// by placeholders according to the database driver, e.g. `$1`
	// for postgres or `?` for sqlite3.
	cond   string
	params []interface{}
}

type WhereQueries []WhereQuery

func (w WhereQueries) build() string {
	// TODO: Implement this
	return ""
}

func (w WhereQueries) Where(cond string, params ...interface{}) WhereQueries {
	return append(w, WhereQuery{
		cond:   cond,
		params: params,
	})
}

func (w WhereQueries) WhereIf(cond string, param interface{}) WhereQueries {
	if param == nil || reflect.ValueOf(param).IsNil() {
		return w
	}

	return append(w, WhereQuery{
		cond:   cond,
		params: []interface{}{param},
	})
}

func Where(cond string, params ...interface{}) WhereQueries {
	return WhereQueries{{
		cond:   cond,
		params: params,
	}}
}

func WhereIf(cond string, param interface{}) WhereQueries {
	if param == nil || reflect.ValueOf(param).IsNil() {
		return WhereQueries{}
	}

	return WhereQueries{{
		cond:   cond,
		params: []interface{}{param},
	}}
}

type OrderByQuery struct {
	fields string
	desc   bool
}

func (o OrderByQuery) Desc() OrderByQuery {
	return OrderByQuery{
		fields: o.fields,
		desc:   true,
	}
}

func OrderBy(fields string) OrderByQuery {
	return OrderByQuery{
		fields: fields,
		desc:   false,
	}
}
