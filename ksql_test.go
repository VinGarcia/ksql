package ksql

import (
	"testing"

	"github.com/ditointernet/go-assert"

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

			assert.Equal(t, nil, err)
			assert.Equal(t, supportedDialects[dialectName], db.dialect)
			assert.Equal(t, dialectName, db.driver)
		}
	})

	t.Run("should report invalid dialectNames correctly", func(t *testing.T) {
		_, err := NewWithAdapter(
			DBAdapter(nil),
			"fake-dialect-name",
		)

		assert.NotEqual(t, nil, err)
	})
}
