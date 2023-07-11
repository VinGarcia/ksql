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
		logLevel   string
		methodCall func(ctx context.Context, db Provider) error
		queryErr   error

		expectLoggedQueryToContain []string
		expectLoggedParams         []interface{}
		expectLoggedErrToContain   []string
	}{
		{
			desc:     "should work for the Query function",
			logLevel: "info",
			methodCall: func(ctx context.Context, db Provider) error {
				var row []struct {
					Count int `ksql:"count"`
				}
				return db.Query(ctx, &row, `SELECT count(*) AS count FROM users WHERE type = $1 AND age < $2`, "fakeType", 42)
			},

			expectLoggedQueryToContain: []string{"count(*)", "type = $1"},
			expectLoggedParams:         []interface{}{"fakeType", 42},
		},
		{
			desc:     "should work for the Query function when an error is returned",
			logLevel: "info",
			methodCall: func(ctx context.Context, db Provider) error {
				var row []struct {
					Count int `ksql:"count"`
				}
				return db.Query(ctx, &row, `SELECT count(*) AS count FROM users WHERE type = $1 AND age < $2`, "fakeType", 42)
			},
			queryErr: errors.New("fakeErrMsg"),

			expectLoggedQueryToContain: []string{"count(*)", "type = $1"},
			expectLoggedParams:         []interface{}{"fakeType", 42},
			expectLoggedErrToContain:   []string{"fakeErrMsg"},
		},
		{
			desc:     "should work for the Query function when an error is returned with error level",
			logLevel: "error",
			methodCall: func(ctx context.Context, db Provider) error {
				var row []struct {
					Count int `ksql:"count"`
				}
				return db.Query(ctx, &row, `SELECT count(*) AS count FROM users WHERE type = $1 AND age < $2`, "fakeType", 42)
			},
			queryErr: errors.New("fakeErrMsg"),

			expectLoggedQueryToContain: []string{"count(*)", "type = $1"},
			expectLoggedParams:         []interface{}{"fakeType", 42},
			expectLoggedErrToContain:   []string{"fakeErrMsg"},
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			var inputQuery string
			var inputParams []interface{}
			c := DB{
				db: mockDBAdapter{
					QueryContextFn: func(ctx context.Context, query string, params ...interface{}) (Rows, error) {
						inputQuery = query
						inputParams = params

						return mockRows{
							NextFn: func() bool { return false },
						}, test.queryErr
					},
					ExecContextFn: func(ctx context.Context, query string, params ...interface{}) (Result, error) {
						inputQuery = query
						inputParams = params

						return mockResult{}, test.queryErr
					},
				},
			}

			var loggedQuery string
			var loggedParams []interface{}
			var loggedErr error
			ctx := InjectLogger(ctx, "info", func(ctx context.Context, values LogValues) {
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
			tt.AssertEqual(t, loggedParams, test.expectLoggedParams)
		})
	}
}
