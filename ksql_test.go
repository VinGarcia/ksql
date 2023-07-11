package ksql

import (
	"context"
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

	t.Run("should work for the Query function", func(t *testing.T) {
		var inputQuery string
		var inputParams []interface{}
		c := DB{
			db: mockDBAdapter{
				QueryContextFn: func(ctx context.Context, query string, params ...interface{}) (Rows, error) {
					inputQuery = query
					inputParams = params

					return mockRows{
						NextFn: func() bool { return false },
					}, nil
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

		var row []struct {
			Count int `ksql:"count"`
		}
		err := c.Query(ctx, &row, `SELECT count(*) AS count FROM users WHERE type = $1 AND age < $2`, "fakeType", 42)
		tt.AssertNoErr(t, err)
		tt.AssertEqual(t, len(row), 0)
		tt.AssertEqual(t, loggedQuery, inputQuery)
		tt.AssertEqual(t, loggedParams, inputParams)
		tt.AssertEqual(t, loggedErr, nil)
	})
}
