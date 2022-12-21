package ksql

import (
	"fmt"
	"io"
	"testing"

	tt "github.com/vingarcia/ksql/internal/testtools"
)

func TestConfigSetDefaultValues(t *testing.T) {
	config := Config{}
	config.SetDefaultValues()

	tt.AssertEqual(t, config, Config{
		MaxOpenConns: 1,
	})
}

func TestNewAdapterWith(t *testing.T) {
	t.Run("should build new instances correctly", func(t *testing.T) {
		for dialectName := range supportedDialects {
			db, err := NewWithAdapter(
				DBAdapter(nil),
				dialectName,
			)

			tt.AssertNoErr(t, err)
			tt.AssertEqual(t, db.dialect, supportedDialects[dialectName])
		}
	})

	t.Run("should report invalid dialectNames correctly", func(t *testing.T) {
		_, err := NewWithAdapter(
			DBAdapter(nil),
			"fake-dialect-name",
		)

		tt.AssertNotEqual(t, err, nil)
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
