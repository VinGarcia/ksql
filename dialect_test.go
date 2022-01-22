package ksql

import (
	"testing"

	tt "github.com/vingarcia/ksql/internal/testtools"
)

func TestGetDriverDialect(t *testing.T) {
	t.Run("should work for all registered drivers", func(t *testing.T) {
		for drivername, expectedDialect := range supportedDialects {
			t.Run(drivername, func(t *testing.T) {
				dialect, err := GetDriverDialect(drivername)
				tt.AssertNoErr(t, err)
				tt.AssertEqual(t, dialect, expectedDialect)
			})
		}
	})

	t.Run("should report error if no driver is found", func(t *testing.T) {
		_, err := GetDriverDialect("non-existing-driver")
		tt.AssertErrContains(t, err, "unsupported driver", "non-existing-driver")
	})
}
