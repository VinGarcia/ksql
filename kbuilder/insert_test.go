package kbuilder_test

import (
	"context"
	"testing"

	"github.com/vingarcia/ksql"
	tt "github.com/vingarcia/ksql/internal/testtools"
	"github.com/vingarcia/ksql/kbuilder"
	"github.com/vingarcia/ksql/kbuilder/internal"
	"github.com/vingarcia/ksql/sqldialect"
)

func TestInsertQuery(t *testing.T) {
	tests := []struct {
		desc           string
		query          kbuilder.Insert
		expectedQuery  string
		expectedParams []interface{}
		expectedErr    bool
	}{
		{
			desc: "should build queries witha single record correctly",
			query: kbuilder.Insert{
				Into: "users",
				Data: &User{
					Name: "foo",
					Age:  42,
				},
			},
			expectedQuery:  `INSERT INTO "users" ("name", "age") VALUES ($1, $2)`,
			expectedParams: []interface{}{"foo", 42},
		},
		{
			desc: "should build queries with multiple records correctly",
			query: kbuilder.Insert{
				Into: "users",
				Data: []User{
					{Name: "foo", Age: 42},
					{Name: "bar", Age: 43},
				},
			},
			expectedQuery:  `INSERT INTO "users" ("name", "age") VALUES ($1, $2), ($3, $4)`,
			expectedParams: []interface{}{"foo", 42, "bar", 43},
		},
		{
			desc: "should omit attributes correctly",
			query: kbuilder.Insert{
				Into: "users",
				Data: []User{
					{Name: "foo", Age: 42},
					{Name: "bar", Age: 43},
				},
				OmitColumns: []string{"age"},
			},
			expectedQuery:  `INSERT INTO "users" ("name") VALUES ($1), ($2)`,
			expectedParams: []interface{}{"foo", "bar"},
		},
		{
			desc: "should insert returning selected attributes",
			query: kbuilder.Insert{
				Into: "users",
				Data: []User{
					{Name: "foo", Age: 42},
					{Name: "bar", Age: 43},
				},
				Returning: []string{"age", "name"},
			},
			expectedQuery:  `INSERT INTO "users" ("name", "age") VALUES ($1, $2), ($3, $4) RETURNING "age", "name"`,
			expectedParams: []interface{}{"foo", 42, "bar", 43},
		},

		/* * * * * Testing error cases: * * * * */
		{
			desc: "should report error if the `Data` attribute is missing",
			query: kbuilder.Insert{
				Into: "users",
			},

			expectedErr: true,
		},
		{
			desc: "should report error if the `Into` attribute is missing",
			query: kbuilder.Insert{
				Data: &User{
					Name: "foo",
					Age:  42,
				},
			},

			expectedErr: true,
		},
		{
			desc: "should report error if `Data` contains an empty list",
			query: kbuilder.Insert{
				Into: "users",
				Data: []User{},
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

func TestExecFromBuilder(t *testing.T) {
	ctx := context.Background()

	var capturedQuery string
	var capturedArgs []interface{}

	db, err := ksql.NewWithAdapter(
		internal.MockDBAdapter{
			ExecContextFn: func(ctx context.Context, query string, args ...interface{}) (ksql.Result, error) {
				capturedQuery = query
				capturedArgs = args
				return internal.MockResult{}, nil
			},
		},
		sqldialect.SupportedDialects["postgres"],
	)
	tt.AssertNoErr(t, err)

	_, err = db.ExecFromBuilder(ctx, kbuilder.Insert{
		Into: "users",
		Data: &User{
			Name: "foo",
			Age:  42,
		},
	})
	tt.AssertNoErr(t, err)

	tt.AssertEqual(t, capturedQuery, `INSERT INTO "users" ("name", "age") VALUES ($1, $2)`)
	tt.AssertEqual(t, capturedArgs, []interface{}{"foo", 42})
}

func TestInsertBuild(t *testing.T) {
	t.Run("should build insert query correctly with valid driver", func(t *testing.T) {
		i := kbuilder.Insert{
			Into: "users",
			Data: &User{
				Name: "john",
				Age:  30,
			},
		}

		query, params, err := i.Build("postgres")

		tt.AssertNoErr(t, err)
		tt.AssertEqual(t, query, `INSERT INTO "users" ("name", "age") VALUES ($1, $2)`)
		tt.AssertEqual(t, params, []interface{}{"john", 30})
	})

	t.Run("should return error for unsupported driver", func(t *testing.T) {
		i := kbuilder.Insert{
			Into: "users",
			Data: &User{
				Name: "john",
				Age:  30,
			},
		}

		_, _, err := i.Build("invalid-driver")

		tt.AssertErrContains(t, err, "unsupported driver", "invalid-driver")
	})

	t.Run("should work with different dialects", func(t *testing.T) {
		i := kbuilder.Insert{
			Into: "users",
			Data: &User{
				Name: "jane",
				Age:  25,
			},
		}

		// Test MySQL (uses ? and backticks)
		query, params, err := i.Build("mysql")
		tt.AssertNoErr(t, err)
		tt.AssertEqual(t, query, "INSERT INTO `users` (`name`, `age`) VALUES (?, ?)")
		tt.AssertEqual(t, params, []interface{}{"jane", 25})

		// Test SQLServer (uses @p1, @p2, etc. and square brackets)
		query, params, err = i.Build("sqlserver")
		tt.AssertNoErr(t, err)
		tt.AssertEqual(t, query, `INSERT INTO [users] ([name], [age]) VALUES (@p1, @p2)`)
		tt.AssertEqual(t, params, []interface{}{"jane", 25})
	})

	t.Run("should build insert with multiple records", func(t *testing.T) {
		i := kbuilder.Insert{
			Into: "users",
			Data: []User{
				{Name: "alice", Age: 28},
				{Name: "bob", Age: 32},
			},
		}

		query, params, err := i.Build("postgres")

		tt.AssertNoErr(t, err)
		tt.AssertEqual(t, query, `INSERT INTO "users" ("name", "age") VALUES ($1, $2), ($3, $4)`)
		tt.AssertEqual(t, params, []interface{}{"alice", 28, "bob", 32})
	})

	t.Run("should omit columns specified in OmitColumns", func(t *testing.T) {
		i := kbuilder.Insert{
			Into: "users",
			Data: &User{
				Name: "charlie",
				Age:  35,
			},
			OmitColumns: []string{"age"},
		}

		query, params, err := i.Build("postgres")

		tt.AssertNoErr(t, err)
		tt.AssertEqual(t, query, `INSERT INTO "users" ("name") VALUES ($1)`)
		tt.AssertEqual(t, params, []interface{}{"charlie"})
	})

	t.Run("should omit multiple columns specified in OmitColumns", func(t *testing.T) {
		type UserWithEmail struct {
			Name  string `ksql:"name"`
			Age   int    `ksql:"age"`
			Email string `ksql:"email"`
		}

		i := kbuilder.Insert{
			Into: "users",
			Data: []UserWithEmail{
				{Name: "dave", Age: 40, Email: "dave@example.com"},
				{Name: "eve", Age: 22, Email: "eve@example.com"},
			},
			OmitColumns: []string{"age", "email"},
		}

		query, params, err := i.Build("postgres")

		tt.AssertNoErr(t, err)
		tt.AssertEqual(t, query, `INSERT INTO "users" ("name") VALUES ($1), ($2)`)
		tt.AssertEqual(t, params, []interface{}{"dave", "eve"})
	})
}
