//go:build ksql_enable_kbuilder_experiment

package kbuilder

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/vingarcia/ksql/internal/structs"
	"github.com/vingarcia/ksql/sqldialect"
)

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
	dialect, ok := sqldialect.SupportedDialects[driver]
	if !ok {
		return "", nil, fmt.Errorf("unsupported driver `%s`", driver)
	}

	return i.BuildQuery(dialect)
}

// BuildQuery implements the queryBuilder interface
func (i Insert) BuildQuery(dialect sqldialect.Provider) (sqlQuery string, params []interface{}, _ error) {
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

	shouldOmit := map[string]bool{}
	for _, name := range i.OmitColumns {
		shouldOmit[name] = true
	}

	var b strings.Builder
	b.WriteString("INSERT INTO " + dialect.Escape(i.Into))

	b.WriteString(" (")
	var escapedNames []string
	for i := 0; i < info.NumFields(); i++ {
		name := info.ByIndex(i).ColumnName

		if shouldOmit[name] {
			continue
		}

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
			if shouldOmit[info.ByIndex(j).ColumnName] {
				continue
			}

			placeholders = append(placeholders, dialect.Placeholder(len(params)))
			params = append(params, record.Field(j).Interface())
		}
		values = append(values, "("+strings.Join(placeholders, ", ")+")")
	}
	b.WriteString(strings.Join(values, ", "))

	if len(i.Returning) > 0 {
		if dialect.InsertMethod() != sqldialect.InsertWithReturning {
			return "", nil, fmt.Errorf(
				"kbuilder: invalid option: driver '%s' does not support Returning values after an insert statement",
				dialect.DriverName(),
			)
		}

		escapedNames := []string{}
		for _, name := range i.Returning {
			escapedNames = append(escapedNames, dialect.Escape(name))
		}

		b.WriteString(" RETURNING " + strings.Join(escapedNames, ", "))
	}

	return b.String(), params, nil
}
