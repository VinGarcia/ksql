package kbuilder

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/vingarcia/ksql/internal/structs"
	"github.com/vingarcia/ksql/sqldialect"
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
	OrderBy string
}

// Build is a utility function for finding the dialect based on the driver and
// then calling BuildQuery(dialect)
func (q Query) Build(driver string) (sqlQuery string, params []interface{}, _ error) {
	dialect, ok := sqldialect.SupportedDialects[driver]
	if !ok {
		return "", nil, fmt.Errorf("unsupported driver `%s`", driver)
	}

	return q.BuildQuery(dialect)
}

// BuildQuery implements the queryBuilder interface
func (q Query) BuildQuery(dialect sqldialect.Provider) (sqlQuery string, params []interface{}, _ error) {
	var b strings.Builder

	switch v := q.Select.(type) {
	case nil:
		// Omit the select part if Select is nil, i.e. start from the FROM part.
		// This is useful when kbuilder is used alongside KSQL since the functions `ksql.Query*()`
		// will automatically build the SELECT part for you, you should probably not do this when using
		// kbuilder with other database tools.

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

	if q.OrderBy != "" {
		b.WriteString(" ORDER BY " + q.OrderBy)
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

func (w WhereQueries) build(dialect sqldialect.Provider) (query string, params []interface{}) {
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
func (w WhereQueries) WhereIf(ifCond bool, cond string, params ...interface{}) WhereQueries {
	if !ifCond {
		return w
	}

	return append(w, WhereQuery{
		cond:   cond,
		params: params,
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
func WhereIf(ifCond bool, cond string, params ...interface{}) WhereQueries {
	if !ifCond {
		return WhereQueries{}
	}

	return WhereQueries{{
		cond:   cond,
		params: params,
	}}
}

var cachedSelectQueries = &sync.Map{}

// Builds the select query using cached info so that its efficient
func buildSelectQuery(obj interface{}, dialect sqldialect.Provider) (string, error) {
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
	for _, field := range info.Fields {
		escapedNames = append(escapedNames, dialect.Escape(field.ColumnName))
	}

	query := strings.Join(escapedNames, ", ")
	cachedSelectQueries.Store(t, query)
	return query, nil
}
