package kbuilder

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/vingarcia/ksql"
	"github.com/vingarcia/ksql/internal/structs"
)

// Insert is the struct template for building INSERT queries
type Insert struct {
	// Into expects a table name, e.g. "users"
	Into string

	// Data expected either a single record annotated with `ksql` tags
	// or a list of records annotated likewise.
	Data interface{}
}

// Build is a utility function for finding the dialect based on the driver and
// then calling BuildQuery(dialect)
func (i Insert) Build(driver string) (sqlQuery string, params []interface{}, _ error) {
	dialect, err := ksql.GetDriverDialect(driver)
	if err != nil {
		return "", nil, err
	}

	return i.BuildQuery(dialect)
}

// BuildQuery implements the queryBuilder interface
func (i Insert) BuildQuery(dialect ksql.Dialect) (sqlQuery string, params []interface{}, _ error) {
	var b strings.Builder
	b.WriteString("INSERT INTO " + dialect.Escape(i.Into))

	if i.Into == "" {
		return "", nil, fmt.Errorf(
			"expected the Into attr to contain the tablename, but got an empty string instead",
		)
	}

	if i.Data == nil {
		return "", nil, fmt.Errorf(
			"expected the Data attr to contain a struct or a list of structs, but got `%v`",
			i.Data,
		)
	}

	v := reflect.ValueOf(i.Data)
	t := v.Type()
	if t.Kind() != reflect.Slice {
		// Convert it to a slice of a single element:
		v = reflect.Append(reflect.MakeSlice(reflect.SliceOf(t), 0, 1), v)
	} else {
		t = t.Elem()
	}

	if v.Len() == 0 {
		return "", nil, fmt.Errorf(
			"can't create an insertion query from an empty list of values",
		)
	}

	isPtr := false
	if t.Kind() == reflect.Ptr {
		isPtr = true
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return "", nil, fmt.Errorf("expected Data attr to be a struct or slice of structs but got: %v", t)
	}

	info, err := structs.GetTagInfo(t)
	if err != nil {
		return "", nil, err
	}

	b.WriteString(" (")
	var escapedNames []string
	for i := 0; i < info.NumFields(); i++ {
		name := info.ByIndex(i).ColumnName
		escapedNames = append(escapedNames, dialect.Escape(name))
	}
	b.WriteString(strings.Join(escapedNames, ", "))
	b.WriteString(") VALUES ")

	params = []interface{}{}
	values := []string{}
	for i := 0; i < v.Len(); i++ {
		record := v.Index(i)
		if isPtr {
			record = record.Elem()
		}

		placeholders := []string{}
		for j := 0; j < info.NumFields(); j++ {
			placeholders = append(placeholders, dialect.Placeholder(len(params)))
			params = append(params, record.Field(j).Interface())
		}
		values = append(values, "("+strings.Join(placeholders, ", ")+")")
	}
	b.WriteString(strings.Join(values, ", "))

	return b.String(), params, nil
}
