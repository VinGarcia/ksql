package kbuilder_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	tt "github.com/vingarcia/ksql/internal/testtools"
	"github.com/vingarcia/ksql/kbuilder"
)

type User struct {
	Name string `ksql:"name"`
	Age  int    `ksql:"age"`
}

var nullField *int

func TestSelectQuery(t *testing.T) {
	tests := []struct {
		desc           string
		query          kbuilder.Query
		expectedQuery  string
		expectedParams []interface{}
		expectedErr    bool
	}{
		{
			desc: "should build queries correctly",
			query: kbuilder.Query{
				Select: &User{},
				From:   "users",
				Where: kbuilder.
					Where("foo < %s", 42).
					Where("bar LIKE %s", "%ending").
					WhereIf(nullField != nil, "foobar = %s", nullField),

				OrderBy: "id DESC",
				Offset:  100,
				Limit:   10,
			},
			expectedQuery:  `SELECT "name", "age" FROM users WHERE foo < $1 AND bar LIKE $2 ORDER BY id DESC LIMIT 10 OFFSET 100`,
			expectedParams: []interface{}{42, "%ending"},
		},
		{
			desc: "should build queries omitting the OFFSET",
			query: kbuilder.Query{
				Select: &User{},
				From:   "users",
				Where: kbuilder.
					Where("foo < %s", 42).
					Where("bar LIKE %s", "%ending").
					WhereIf(nullField != nil, "foobar = %s", nullField),

				OrderBy: "id DESC",
				Limit:   10,
			},
			expectedQuery:  `SELECT "name", "age" FROM users WHERE foo < $1 AND bar LIKE $2 ORDER BY id DESC LIMIT 10`,
			expectedParams: []interface{}{42, "%ending"},
		},
		{
			desc: "should build queries omitting the LIMIT",
			query: kbuilder.Query{
				Select: &User{},
				From:   "users",
				Where: kbuilder.
					Where("foo < %s", 42).
					Where("bar LIKE %s", "%ending").
					WhereIf(nullField != nil, "foobar = %s", nullField),

				OrderBy: "id DESC",
				Offset:  100,
			},
			expectedQuery:  `SELECT "name", "age" FROM users WHERE foo < $1 AND bar LIKE $2 ORDER BY id DESC OFFSET 100`,
			expectedParams: []interface{}{42, "%ending"},
		},
		{
			desc: "should build queries omitting the ORDER BY clause",
			query: kbuilder.Query{
				Select: &User{},
				From:   "users",
				Where: kbuilder.
					Where("foo < %s", 42).
					Where("bar LIKE %s", "%ending").
					WhereIf(nullField != nil, "foobar = %s", nullField),

				Offset: 100,
				Limit:  10,
			},
			expectedQuery:  `SELECT "name", "age" FROM users WHERE foo < $1 AND bar LIKE $2 LIMIT 10 OFFSET 100`,
			expectedParams: []interface{}{42, "%ending"},
		},
		{
			desc: "should build queries omitting the WHERE clause",
			query: kbuilder.Query{
				Select: &User{},
				From:   "users",

				OrderBy: "id DESC",
				Offset:  100,
				Limit:   10,
			},
			expectedQuery: `SELECT "name", "age" FROM users ORDER BY id DESC LIMIT 10 OFFSET 100`,
		},
		{
			desc: "should build queries using the WhereIf statement",
			query: kbuilder.Query{
				Select: &User{},
				From:   "users",
				Where: kbuilder.
					WhereIf(true, "foo < %s", 42).
					WhereIf(false, "wont_show != %s", 42).
					WhereIf(true, "bar LIKE %s OR bar LIKE %s", "%ending", "something").
					WhereIf(false, "wont_show LIKE %s", "%ending").
					WhereIf(nullField != nil, "foobar = %s", nullField),

				Offset: 100,
				Limit:  10,
			},
			expectedQuery:  `SELECT "name", "age" FROM users WHERE foo < $1 AND bar LIKE $2 OR bar LIKE $3 LIMIT 10 OFFSET 100`,
			expectedParams: []interface{}{42, "%ending", "something"},
		},
		{
			desc: "should omit the SELECT part of the query if Select is nil",
			query: kbuilder.Query{
				Select: nil,
				From:   "users",
				Where: kbuilder.
					Where("foo < %s", 42).
					Where("bar LIKE %s", "%ending").
					WhereIf(nullField != nil, "foobar = %s", nullField),

				OrderBy: "id DESC",
				Offset:  100,
				Limit:   10,
			},
			expectedQuery:  ` FROM users WHERE foo < $1 AND bar LIKE $2 ORDER BY id DESC LIMIT 10 OFFSET 100`,
			expectedParams: []interface{}{42, "%ending"},
		},

		/* * * * * Testing error cases: * * * * */
		{
			desc: "should report error if the FROM clause is missing",
			query: kbuilder.Query{
				Select: &User{},
				Where: kbuilder.
					Where("foo < %s", 42).
					Where("bar LIKE %s", "%ending").
					WhereIf(nullField != nil, "foobar = %s", nullField),

				OrderBy: "id DESC",
				Offset:  100,
				Limit:   10,
			},

			expectedErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			b, err := kbuilder.New("postgres")
			tt.AssertNoErr(t, err)

			query, params, err := b.Build(test.query)

			expectError(t, test.expectedErr, err)
			tt.AssertEqual(t, query, test.expectedQuery)
			tt.AssertEqual(t, params, test.expectedParams)
		})
	}
}

func expectError(t *testing.T, expect bool, err error) {
	if expect {
		require.Equal(t, true, err != nil, "expected an error, but got nothing")
	} else {
		require.Equal(t, false, err != nil, fmt.Sprintf("unexpected error %s", err))
	}
}
