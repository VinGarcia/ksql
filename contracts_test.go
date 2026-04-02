package ksql

import (
	"fmt"
	"testing"

	tt "github.com/vingarcia/ksql/internal/testtools"
	"github.com/vingarcia/ksql/sqldialect"
)

func TestTable(t *testing.T) {
	t.Run("test validate()", func(t *testing.T) {
		tests := []struct {
			desc        string
			table       Table
			expectedErr error
		}{
			{
				desc:        "should return nil when table is valid with single ID column",
				table:       Table{name: "users", idColumns: []string{"id"}},
				expectedErr: nil,
			},
			{
				desc:        "should return nil when table is valid with multiple ID columns",
				table:       Table{name: "users", idColumns: []string{"id", "tenant_id"}},
				expectedErr: nil,
			},
			{
				desc:        "should return error when table name is empty",
				table:       Table{name: ""},
				expectedErr: fmt.Errorf("table name cannot be an empty string"),
			},
			{
				desc:        "should return error when ID column is empty string",
				table:       Table{name: "users", idColumns: []string{""}},
				expectedErr: fmt.Errorf("ID columns cannot be empty strings"),
			},
			{
				desc:        "should return error when one of multiple ID columns is empty",
				table:       Table{name: "users", idColumns: []string{"id", "", "tenant_id"}},
				expectedErr: fmt.Errorf("ID columns cannot be empty strings"),
			},
		}

		for _, test := range tests {
			t.Run(test.desc, func(t *testing.T) {
				err := test.table.validate()
				tt.AssertEqual(t, err, test.expectedErr)
			})
		}
	})

	t.Run("test Name()", func(t *testing.T) {
		tests := []struct {
			desc         string
			table        Table
			expectedName string
		}{
			{
				desc:         "should return table name",
				table:        Table{name: "users"},
				expectedName: "users",
			},
			{
				desc:         "should return table name with special characters",
				table:        Table{name: "user_accounts"},
				expectedName: "user_accounts",
			},
		}

		for _, test := range tests {
			t.Run(test.desc, func(t *testing.T) {
				name := test.table.Name()
				tt.AssertEqual(t, name, test.expectedName)
			})
		}
	})

	t.Run("test insertMethodFor()", func(t *testing.T) {
		tests := []struct {
			desc                 string
			table                Table
			dialectInsertMethod  sqldialect.InsertMethod
			expectedInsertMethod sqldialect.InsertMethod
		}{
			{
				desc:                 "should return dialect's InsertMethod when table has single ID column",
				table:                Table{name: "users", idColumns: []string{"id"}},
				dialectInsertMethod:  sqldialect.InsertWithLastInsertID,
				expectedInsertMethod: sqldialect.InsertWithLastInsertID,
			},
			{
				desc:                 "should return dialect's InsertMethod when table has single ID column (returning variant)",
				table:                Table{name: "users", idColumns: []string{"id"}},
				dialectInsertMethod:  sqldialect.InsertWithReturning,
				expectedInsertMethod: sqldialect.InsertWithReturning,
			},
			{
				desc:                 "should return InsertWithNoIDRetrieval when table has multiple ID columns and dialect uses LastInsertID",
				table:                Table{name: "users", idColumns: []string{"id", "tenant_id"}},
				dialectInsertMethod:  sqldialect.InsertWithLastInsertID,
				expectedInsertMethod: sqldialect.InsertWithNoIDRetrieval,
			},
			{
				desc:                 "should return dialect's InsertMethod when table has multiple ID columns and dialect uses Returning",
				table:                Table{name: "users", idColumns: []string{"id", "tenant_id"}},
				dialectInsertMethod:  sqldialect.InsertWithReturning,
				expectedInsertMethod: sqldialect.InsertWithReturning,
			},
			{
				desc:                 "should return dialect's InsertMethod when table has multiple ID columns and dialect uses NoIDRetrieval",
				table:                Table{name: "users", idColumns: []string{"id", "tenant_id"}},
				dialectInsertMethod:  sqldialect.InsertWithNoIDRetrieval,
				expectedInsertMethod: sqldialect.InsertWithNoIDRetrieval,
			},
		}

		for _, test := range tests {
			t.Run(test.desc, func(t *testing.T) {
				mockDialect := &mockDialectProvider{
					InsertMethodFn: func() sqldialect.InsertMethod {
						return test.dialectInsertMethod
					},
				}

				result := test.table.insertMethodFor(mockDialect)
				tt.AssertEqual(t, result, test.expectedInsertMethod)
			})
		}
	})
}
