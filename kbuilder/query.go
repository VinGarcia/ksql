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

	// GroupBy expects any string compatible with the contents of `GROUP BY <contents>`
	GroupBy string

	// Having expects a list of HavingQuery instances built
	// by the public Having() function, rendered after the GROUP BY clause.
	Having HavingQueries

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
		whereQuery, params = q.Where.build(dialect, 0)
		b.WriteString(" WHERE " + whereQuery)
	}

	if strings.TrimSpace(q.From) == "" {
		return "", nil, fmt.Errorf("the From field is mandatory for every query")
	}

	if q.GroupBy != "" {
		b.WriteString(" GROUP BY " + q.GroupBy)
	}

	if len(q.Having) > 0 {
		havingQuery, havingParams := q.Having.build(dialect, len(params))
		b.WriteString(" HAVING " + havingQuery)
		params = append(params, havingParams...)
	}

	q.OrderBy = strings.TrimSpace(q.OrderBy)
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

// build renders the conditions joined by AND. numPreviousParams is the number of
// placeholders already present in the surrounding query, so that numbered
// placeholders (e.g. postgres `$N`) continue from the right index.
func (w WhereQueries) build(dialect sqldialect.Provider, numPreviousParams int) (query string, params []interface{}) {
	var conds []string
	for _, whereQuery := range w {
		var placeholders []interface{}
		for i := range whereQuery.params {
			placeholders = append(placeholders, dialect.Placeholder(numPreviousParams+len(params)+i))
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

// Where creates a new WhereQueries helper containing a single boolean condition.
func Where(cond string, params ...interface{}) WhereQueries {
	return WhereQueries{{
		cond:   cond,
		params: params,
	}}
}

// WhereIf creates a new WhereQueries helper containing a single
// boolean condition, but only if ifCond is true.
func WhereIf(ifCond bool, cond string, params ...interface{}) WhereQueries {
	if !ifCond {
		return WhereQueries{}
	}

	return WhereQueries{{
		cond:   cond,
		params: params,
	}}
}

// HavingQuery represents a single condition in a HAVING expression.
type HavingQuery = WhereQuery

// HavingQueries is the helper for creating complex HAVING
// conditions in a dynamic way. It mirrors the WhereQueries API,
// so new WhereQueries features should get a counterpart here.
type HavingQueries []HavingQuery

// build delegates to WhereQueries.build; HAVING conditions render identically to WHERE ones.
func (h HavingQueries) build(dialect sqldialect.Provider, numPreviousParams int) (query string, params []interface{}) {
	return WhereQueries(h).build(dialect, numPreviousParams)
}

// Having adds a new boolean condition to an existing
// HavingQueries helper.
func (h HavingQueries) Having(cond string, params ...interface{}) HavingQueries {
	return HavingQueries(WhereQueries(h).Where(cond, params...))
}

// HavingIf conditionally adds a new boolean expression to the HavingQueries helper.
func (h HavingQueries) HavingIf(ifCond bool, cond string, params ...interface{}) HavingQueries {
	return HavingQueries(WhereQueries(h).WhereIf(ifCond, cond, params...))
}

// Having creates a new HavingQueries helper containing a single boolean condition.
func Having(cond string, params ...interface{}) HavingQueries {
	return HavingQueries(Where(cond, params...))
}

// HavingIf creates a new HavingQueries helper containing a single
// boolean condition, but only if ifCond is true.
func HavingIf(ifCond bool, cond string, params ...interface{}) HavingQueries {
	return HavingQueries(WhereIf(ifCond, cond, params...))
}

// selectQueryCacheKey partitions the cache per dialect because column escaping varies between them.
type selectQueryCacheKey struct {
	structType reflect.Type
	driver     string
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

	cacheKey := selectQueryCacheKey{structType: t, driver: dialect.DriverName()}
	if data, found := cachedSelectQueries.Load(cacheKey); found {
		query, _ := data.(string)
		return query, nil
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
	cachedSelectQueries.Store(cacheKey, query)
	return query, nil
}
