package kbuilder

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/vingarcia/ksql"
	"github.com/vingarcia/ksql/internal/structs"
)

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
	dialect, err := ksql.GetDriverDialect(driver)
	if err != nil {
		return "", nil, err
	}

	return q.BuildQuery(dialect)
}

// BuildQuery implements the queryBuilder interface
func (q Query) BuildQuery(dialect ksql.Dialect) (sqlQuery string, params []interface{}, _ error) {
	var b strings.Builder

	switch v := q.Select.(type) {
	case string:
		b.WriteString("SELECT " + v)
	default:
		selectQuery, err := buildSelectQuery(v, dialect)
		if err != nil {
			return "", nil, fmt.Errorf("error reading the Select field: %w", err)
		}
		b.WriteString("SELECT " + selectQuery)
	}

	b.WriteString(" FROM " + q.From)

	if len(q.Where) > 0 {
		var whereQuery string
		whereQuery, params = q.Where.build(dialect)
		b.WriteString(" WHERE " + whereQuery)
	}

	if strings.TrimSpace(q.From) == "" {
		return "", nil, fmt.Errorf("the From field is mandatory for every query")
	}

	if q.OrderBy.fields != "" {
		b.WriteString(" ORDER BY " + q.OrderBy.fields)
		if q.OrderBy.desc {
			b.WriteString(" DESC")
		}
	}

	if q.Limit > 0 {
		b.WriteString(" LIMIT " + strconv.Itoa(q.Limit))
	}

	if q.Offset > 0 {
		b.WriteString(" OFFSET " + strconv.Itoa(q.Offset))
	}

	return b.String(), params, nil
}

// WhereQuery represents a single condition in a WHERE expression.
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

// WhereQueries is the helper for creating complex WHERE queries
// in a dynamic way.
type WhereQueries []WhereQuery

func (w WhereQueries) build(dialect ksql.Dialect) (query string, params []interface{}) {
	var conds []string
	for _, whereQuery := range w {
		var placeholders []interface{}
		for i := range whereQuery.params {
			placeholders = append(placeholders, dialect.Placeholder(len(params)+i))
		}

		conds = append(conds, fmt.Sprintf(whereQuery.cond, placeholders...))
		params = append(params, whereQuery.params...)
	}

	return strings.Join(conds, " AND "), params
}

// Where adds a new boolean condition to an existing
// WhereQueries helper.
func (w WhereQueries) Where(cond string, params ...interface{}) WhereQueries {
	return append(w, WhereQuery{
		cond:   cond,
		params: params,
	})
}

// WhereIf conditionally adds a new boolean expression to the WhereQueries helper.
func (w WhereQueries) WhereIf(cond string, param interface{}) WhereQueries {
	if param == nil || reflect.ValueOf(param).IsNil() {
		return w
	}

	return append(w, WhereQuery{
		cond:   cond,
		params: []interface{}{param},
	})
}

// Where adds a new boolean condition to an existing
// WhereQueries helper.
func Where(cond string, params ...interface{}) WhereQueries {
	return WhereQueries{{
		cond:   cond,
		params: params,
	}}
}

// WhereIf conditionally adds a new boolean expression to the WhereQueries helper
func WhereIf(cond string, param interface{}) WhereQueries {
	if param == nil || reflect.ValueOf(param).IsNil() {
		return WhereQueries{}
	}

	return WhereQueries{{
		cond:   cond,
		params: []interface{}{param},
	}}
}

// OrderByQuery represents the ORDER BY part of the query
type OrderByQuery struct {
	fields string
	desc   bool
}

// Desc is a setter function for configuring the
// ORDER BY part of the query as DESC
func (o OrderByQuery) Desc() OrderByQuery {
	return OrderByQuery{
		fields: o.fields,
		desc:   true,
	}
}

// OrderBy is a helper for building the ORDER BY
// part of the query.
func OrderBy(fields string) OrderByQuery {
	return OrderByQuery{
		fields: fields,
		desc:   false,
	}
}

var cachedSelectQueries = &sync.Map{}

// Builds the select query using cached info so that its efficient
func buildSelectQuery(obj interface{}, dialect ksql.Dialect) (string, error) {
	t := reflect.TypeOf(obj)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return "", fmt.Errorf("expected to receive a pointer to struct, but got: %T", obj)
	}

	if data, found := cachedSelectQueries.Load(t); found {
		if query, ok := data.(string); !ok {
			return "", fmt.Errorf("invalid cache entry, expected type string, found %T", data)
		} else {
			return query, nil
		}
	}

	info, err := structs.GetTagInfo(t)
	if err != nil {
		return "", err
	}

	var escapedNames []string
	for i := 0; i < info.NumFields(); i++ {
		name := info.ByIndex(i).Name
		escapedNames = append(escapedNames, dialect.Escape(name))
	}

	query := strings.Join(escapedNames, ", ")
	cachedSelectQueries.Store(t, query)
	return query, nil
}
