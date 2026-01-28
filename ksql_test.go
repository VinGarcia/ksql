package ksql

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	tt "github.com/vingarcia/ksql/internal/testtools"
	"github.com/vingarcia/ksql/sqldialect"
)

func TestScanArgError(t *testing.T) {
	err := ScanArgError{
		ColumnIndex: 12,
		Err:         io.EOF,
	}

	tt.AssertErrContains(t, err, "input attribute", "index 12", "EOF")
}

func TestConfigSetDefaultValues(t *testing.T) {
	config := Config{}
	config.SetDefaultValues()

	tt.AssertEqual(t, config, Config{
		MaxOpenConns: 1,
	})
}

func TestNewAdapterWith(t *testing.T) {
	t.Run("should build new instances correctly", func(t *testing.T) {
		for _, dialect := range sqldialect.SupportedDialects {
			db, err := NewWithAdapter(
				DBAdapter(nil),
				dialect,
			)

			tt.AssertNoErr(t, err)
			tt.AssertEqual(t, db.dialect, dialect)
		}
	})

	t.Run("should report invalid dialects correctly", func(t *testing.T) {
		_, err := NewWithAdapter(
			DBAdapter(nil),
			nil,
		)

		tt.AssertErrContains(t, err, "expected a valid", "Provider", "nil")
	})
}

func TestClose(t *testing.T) {
	t.Run("should close the adapter if it implements the io.Closer interface", func(t *testing.T) {
		c := DB{
			db: struct {
				DBAdapter
				io.Closer
			}{
				DBAdapter: mockDBAdapter{},
				Closer: mockCloser{
					CloseFn: func() error {
						return nil
					},
				},
			},
		}

		err := c.Close()
		tt.AssertNoErr(t, err)
	})

	t.Run("should exit normally if the adapter does not implement the io.Closer interface", func(t *testing.T) {
		c := DB{
			db: mockDBAdapter{},
		}

		err := c.Close()
		tt.AssertNoErr(t, err)
	})

	t.Run("should report an error if the adapter.Close() returns one", func(t *testing.T) {
		c := DB{
			db: struct {
				DBAdapter
				io.Closer
			}{
				DBAdapter: mockDBAdapter{},
				Closer: mockCloser{
					CloseFn: func() error {
						return fmt.Errorf("fakeCloseErrMsg")
					},
				},
			},
		}

		err := c.Close()
		tt.AssertErrContains(t, err, "fakeCloseErrMsg")
	})
}

func TestInjectLogger(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		desc       string
		methodCall func(ctx context.Context, db Provider) error
		queryErr   error

		expectLoggedQueryToContain []string
		expectLoggedParams         map[interface{}]bool
		expectLoggedErrToContain   []string
	}{
		{
			desc: "should work for the Query function",
			methodCall: func(ctx context.Context, db Provider) error {
				var row []struct {
					Count int `ksql:"count"`
				}
				return db.Query(ctx, &row, `FROM users WHERE type = $1 AND age < $2`, "fakeType", 42)
			},

			expectLoggedQueryToContain: []string{"SELECT", "count", "type = $1"},
			expectLoggedParams:         map[interface{}]bool{"fakeType": true, 42: true},
		},
		{
			desc: "should work for the Query function when an error is returned",
			methodCall: func(ctx context.Context, db Provider) error {
				var row []struct {
					Count int `ksql:"count"`
				}
				return db.Query(ctx, &row, `FROM users WHERE type = $1 AND age < $2`, "fakeType", 42)
			},
			queryErr: errors.New("fakeErrMsg"),

			expectLoggedQueryToContain: []string{"SELECT", "count", "type = $1"},
			expectLoggedParams:         map[interface{}]bool{"fakeType": true, 42: true},
			expectLoggedErrToContain:   []string{"fakeErrMsg"},
		},
		{
			desc: "should work for the QueryOne function",
			methodCall: func(ctx context.Context, db Provider) error {
				var row struct {
					Count int `ksql:"count"`
				}
				return db.QueryOne(ctx, &row, `FROM users WHERE type = $1 AND age < $2`, "fakeType", 42)
			},

			expectLoggedQueryToContain: []string{"SELECT", "count", "type = $1"},
			expectLoggedParams:         map[interface{}]bool{"fakeType": true, 42: true},
		},
		{
			desc: "should work for the QueryOne function when an error is returned",
			methodCall: func(ctx context.Context, db Provider) error {
				var row struct {
					Count int `ksql:"count"`
				}
				return db.QueryOne(ctx, &row, `FROM users WHERE type = $1 AND age < $2`, "fakeType", 42)
			},
			queryErr: errors.New("fakeErrMsg"),

			expectLoggedQueryToContain: []string{"SELECT", "count", "type = $1"},
			expectLoggedParams:         map[interface{}]bool{"fakeType": true, 42: true},
			expectLoggedErrToContain:   []string{"fakeErrMsg"},
		},
		{
			desc: "should work for the QueryChunks function",
			methodCall: func(ctx context.Context, db Provider) error {
				type Row struct {
					Count int `ksql:"count"`
				}
				return db.QueryChunks(ctx, ChunkParser{
					Query:     `FROM users WHERE type = $1 AND age < $2`,
					Params:    []interface{}{"fakeType", 42},
					ChunkSize: 100,
					ForEachChunk: func(row []Row) error {
						return nil
					},
				})
			},

			expectLoggedQueryToContain: []string{"SELECT", "count", "type = $1"},
			expectLoggedParams:         map[interface{}]bool{"fakeType": true, 42: true},
		},
		{
			desc: "should work for the QueryChunks function when an error is returned",
			methodCall: func(ctx context.Context, db Provider) error {
				type Row struct {
					Count int `ksql:"count"`
				}
				return db.QueryChunks(ctx, ChunkParser{
					Query:     `FROM users WHERE type = $1 AND age < $2`,
					Params:    []interface{}{"fakeType", 42},
					ChunkSize: 100,
					ForEachChunk: func(row []Row) error {
						return nil
					},
				})
			},
			queryErr: errors.New("fakeErrMsg"),

			expectLoggedQueryToContain: []string{"SELECT", "count", "type = $1"},
			expectLoggedParams:         map[interface{}]bool{"fakeType": true, 42: true},
			expectLoggedErrToContain:   []string{"fakeErrMsg"},
		},
		{
			desc: "should work for the Insert function",
			methodCall: func(ctx context.Context, db Provider) error {
				fakeRecord := struct {
					ID    int `ksql:"id"`
					Count int `ksql:"count"`
				}{
					ID:    42,
					Count: 43,
				}
				return db.Insert(ctx, NewTable("fakeTable"), &fakeRecord)
			},

			expectLoggedQueryToContain: []string{"INSERT", "fakeTable", `"id"`},
			expectLoggedParams:         map[interface{}]bool{42: true, 43: true},
		},
		{
			desc: "should work for the Insert function when an error is returned",
			methodCall: func(ctx context.Context, db Provider) error {
				fakeRecord := struct {
					ID    int `ksql:"id"`
					Count int `ksql:"count"`
				}{
					ID:    42,
					Count: 43,
				}
				return db.Insert(ctx, NewTable("fakeTable"), &fakeRecord)
			},
			queryErr: errors.New("fakeErrMsg"),

			expectLoggedQueryToContain: []string{"INSERT", "fakeTable", `"id"`},
			expectLoggedParams:         map[interface{}]bool{42: true, 43: true},
			expectLoggedErrToContain:   []string{"fakeErrMsg"},
		},
		{
			desc: "should work for the Patch function",
			methodCall: func(ctx context.Context, db Provider) error {
				fakeRecord := struct {
					ID    int `ksql:"id"`
					Count int `ksql:"count"`
				}{
					ID:    42,
					Count: 43,
				}
				return db.Patch(ctx, NewTable("fakeTable"), &fakeRecord)
			},

			expectLoggedQueryToContain: []string{"UPDATE", "fakeTable", `"id"`},
			expectLoggedParams:         map[interface{}]bool{42: true, 43: true},
		},
		{
			desc: "should work for the Patch function when an error is returned",
			methodCall: func(ctx context.Context, db Provider) error {
				fakeRecord := struct {
					ID    int `ksql:"id"`
					Count int `ksql:"count"`
				}{
					ID:    42,
					Count: 43,
				}
				return db.Patch(ctx, NewTable("fakeTable"), &fakeRecord)
			},
			queryErr: errors.New("fakeErrMsg"),

			expectLoggedQueryToContain: []string{"UPDATE", "fakeTable", `"id"`},
			expectLoggedParams:         map[interface{}]bool{42: true, 43: true},
			expectLoggedErrToContain:   []string{"fakeErrMsg"},
		},
		{
			desc: "should work for the Delete function",
			methodCall: func(ctx context.Context, db Provider) error {
				fakeRecord := struct {
					ID    int `ksql:"id"`
					Count int `ksql:"count"`
				}{
					ID:    42,
					Count: 43,
				}
				return db.Delete(ctx, NewTable("fakeTable"), &fakeRecord)
			},

			expectLoggedQueryToContain: []string{"DELETE", "fakeTable", `"id"`},
			expectLoggedParams:         map[interface{}]bool{42: true},
		},
		{
			desc: "should work for the Delete function when an error is returned",
			methodCall: func(ctx context.Context, db Provider) error {
				fakeRecord := struct {
					ID    int `ksql:"id"`
					Count int `ksql:"count"`
				}{
					ID:    42,
					Count: 43,
				}
				return db.Delete(ctx, NewTable("fakeTable"), &fakeRecord)
			},
			queryErr: errors.New("fakeErrMsg"),

			expectLoggedQueryToContain: []string{"DELETE", "fakeTable", `"id"`},
			expectLoggedParams:         map[interface{}]bool{42: true},
			expectLoggedErrToContain:   []string{"fakeErrMsg"},
		},
		{
			desc: "should work for the Exec function",
			methodCall: func(ctx context.Context, db Provider) error {
				_, err := db.Exec(ctx, `DELETE FROM fakeTable WHERE type = $1 OR age >= $2`, "fakeType", 142)
				return err
			},

			expectLoggedQueryToContain: []string{"DELETE", "fakeTable", "type", "age"},
			expectLoggedParams:         map[interface{}]bool{"fakeType": true, 142: true},
		},
		{
			desc: "should work for the Exec function when an error is returned",
			methodCall: func(ctx context.Context, db Provider) error {
				_, err := db.Exec(ctx, `DELETE FROM fakeTable WHERE type = $1 OR age >= $2`, "fakeType", 142)
				return err
			},
			queryErr: errors.New("fakeErrMsg"),

			expectLoggedQueryToContain: []string{"DELETE", "fakeTable", "type", "age"},
			expectLoggedParams:         map[interface{}]bool{"fakeType": true, 142: true},
			expectLoggedErrToContain:   []string{"fakeErrMsg"},
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			var inputQuery string
			var inputParams []interface{}
			numRows := 1
			c := DB{
				dialect: sqldialect.SupportedDialects["postgres"],
				db: mockDBAdapter{
					QueryContextFn: func(ctx context.Context, query string, params ...interface{}) (Rows, error) {
						inputQuery = query
						inputParams = params

						return mockRows{
							ScanFn: func(args ...interface{}) error {
								return nil
							},
							// Make sure this mock will return a single row
							// for the purposes of this test:
							NextFn: func() bool {
								numRows--
								return numRows >= 0
							},
							ColumnsFn: func() ([]string, error) { return []string{"count"}, nil },
						}, test.queryErr
					},
					ExecContextFn: func(ctx context.Context, query string, params ...interface{}) (Result, error) {
						inputQuery = query
						inputParams = params

						return mockResult{
							// Make sure this mock will return a single row
							// for the purposes of this test:
							RowsAffectedFn: func() (int64, error) {
								return 1, nil
							},
						}, test.queryErr
					},
				},
			}

			var loggedQuery string
			var loggedParams []interface{}
			var loggedErr error
			ctx := InjectLogger(ctx, func(ctx context.Context, values LogValues) {
				loggedQuery = values.Query
				loggedParams = values.Params
				loggedErr = values.Err
			})

			err := test.methodCall(ctx, c)
			if test.expectLoggedErrToContain != nil {
				tt.AssertErrContains(t, err, test.expectLoggedErrToContain...)
				tt.AssertErrContains(t, loggedErr, test.expectLoggedErrToContain...)
			} else {
				tt.AssertNoErr(t, err)
				tt.AssertEqual(t, loggedErr, nil)
			}

			tt.AssertEqual(t, loggedQuery, inputQuery)
			tt.AssertEqual(t, loggedParams, inputParams)

			tt.AssertContains(t, loggedQuery, test.expectLoggedQueryToContain...)

			paramsMap := map[interface{}]bool{}
			for _, param := range loggedParams {
				paramsMap[param] = true
			}
			tt.AssertEqual(t, paramsMap, test.expectLoggedParams)
		})
	}
}

func TestQueryFromBuilder(t *testing.T) {
	ctx := context.Background()

	type testUser struct {
		Name string `ksql:"name"`
		Age  int    `ksql:"age"`
	}

	t.Run("should build query and pass correct query and params to the adapter", func(t *testing.T) {
		var capturedQuery string
		var capturedParams []interface{}

		c := DB{
			dialect: sqldialect.SupportedDialects["postgres"],
			db: mockDBAdapter{
				QueryContextFn: func(ctx context.Context, query string, args ...interface{}) (Rows, error) {
					capturedQuery = query
					capturedParams = args
					return mockRows{
						NextFn: func() bool { return false },
					}, nil
				},
			},
		}

		testBuilder := MockQueryBuilder{
			BuildQueryFn: func(dialect sqldialect.Provider) (string, []interface{}, error) {
				return `SELECT "name", "age" FROM users WHERE id = $1`, []interface{}{42}, nil
			},
		}

		var results []testUser
		err := c.QueryFromBuilder(ctx, &results, testBuilder)

		tt.AssertNoErr(t, err)
		tt.AssertEqual(t, capturedQuery, `SELECT "name", "age" FROM users WHERE id = $1`)
		tt.AssertEqual(t, capturedParams, []interface{}{42})
	})

	t.Run("should propagate errors from BuildQuery", func(t *testing.T) {
		c := DB{
			dialect: sqldialect.SupportedDialects["postgres"],
			db: mockDBAdapter{
				QueryContextFn: func(ctx context.Context, query string, args ...interface{}) (Rows, error) {
					return mockRows{
						NextFn: func() bool { return false },
					}, nil
				},
			},
		}

		testBuilder := MockQueryBuilder{
			BuildQueryFn: func(dialect sqldialect.Provider) (string, []interface{}, error) {
				return "", nil, fmt.Errorf("build query error")
			},
		}

		var results []testUser
		err := c.QueryFromBuilder(ctx, &results, testBuilder)

		tt.AssertErrContains(t, err, "build query error")
	})

	t.Run("should propagate errors from the adapter QueryContext", func(t *testing.T) {
		c := DB{
			dialect: sqldialect.SupportedDialects["postgres"],
			db: mockDBAdapter{
				QueryContextFn: func(ctx context.Context, query string, args ...interface{}) (Rows, error) {
					return nil, fmt.Errorf("adapter query error")
				},
			},
		}

		testBuilder := MockQueryBuilder{
			BuildQueryFn: func(dialect sqldialect.Provider) (string, []interface{}, error) {
				return `SELECT "name", "age" FROM users`, []interface{}{}, nil
			},
		}

		var results []testUser
		err := c.QueryFromBuilder(ctx, &results, testBuilder)

		tt.AssertErrContains(t, err, "adapter query error")
	})
}

func TestExecFromBuilder(t *testing.T) {
	ctx := context.Background()

	t.Run("should build query and pass correct query and params to the adapter", func(t *testing.T) {
		var capturedQuery string
		var capturedParams []interface{}

		c := DB{
			dialect: sqldialect.SupportedDialects["postgres"],
			db: mockDBAdapter{
				ExecContextFn: func(ctx context.Context, query string, args ...interface{}) (Result, error) {
					capturedQuery = query
					capturedParams = args
					return mockResult{
						LastInsertIdFn: func() (int64, error) { return 1, nil },
						RowsAffectedFn: func() (int64, error) { return 1, nil },
					}, nil
				},
			},
		}

		testBuilder := MockQueryBuilder{
			BuildQueryFn: func(dialect sqldialect.Provider) (string, []interface{}, error) {
				return `INSERT INTO "users" ("name", "age") VALUES ($1, $2)`, []interface{}{"foo", 42}, nil
			},
		}

		result, err := c.ExecFromBuilder(ctx, testBuilder)

		tt.AssertNoErr(t, err)
		tt.AssertEqual(t, capturedQuery, `INSERT INTO "users" ("name", "age") VALUES ($1, $2)`)
		tt.AssertEqual(t, capturedParams, []interface{}{"foo", 42})

		lastID, err := result.LastInsertId()
		tt.AssertNoErr(t, err)
		tt.AssertEqual(t, lastID, int64(1))

		rowsAffected, err := result.RowsAffected()
		tt.AssertNoErr(t, err)
		tt.AssertEqual(t, rowsAffected, int64(1))
	})

	t.Run("should propagate errors from BuildQuery", func(t *testing.T) {
		c := DB{
			dialect: sqldialect.SupportedDialects["postgres"],
			db: mockDBAdapter{
				ExecContextFn: func(ctx context.Context, query string, args ...interface{}) (Result, error) {
					return mockResult{}, nil
				},
			},
		}

		testBuilder := MockQueryBuilder{
			BuildQueryFn: func(dialect sqldialect.Provider) (string, []interface{}, error) {
				return "", nil, fmt.Errorf("build query error")
			},
		}

		_, err := c.ExecFromBuilder(ctx, testBuilder)

		tt.AssertErrContains(t, err, "build query error")
	})

	t.Run("should propagate errors from the adapter ExecContext", func(t *testing.T) {
		c := DB{
			dialect: sqldialect.SupportedDialects["postgres"],
			db: mockDBAdapter{
				ExecContextFn: func(ctx context.Context, query string, args ...interface{}) (Result, error) {
					return nil, fmt.Errorf("adapter exec error")
				},
			},
		}

		testBuilder := MockQueryBuilder{
			BuildQueryFn: func(dialect sqldialect.Provider) (string, []interface{}, error) {
				return `INSERT INTO users VALUES ($1)`, []interface{}{42}, nil
			},
		}

		_, err := c.ExecFromBuilder(ctx, testBuilder)

		tt.AssertErrContains(t, err, "adapter exec error")
	})
}
