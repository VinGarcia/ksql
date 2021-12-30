package tt

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// AssertEqual will compare the got argument with the expected argument
// and fail the test with an appropriate error message if they don't match.
func AssertEqual(t *testing.T, got interface{}, expected interface{}, msg ...interface{}) {
	require.Equal(t, expected, got, msg...)
}

// AssertNotEqual will compare the got argument with the expected argument
// and fail the test with an appropriate error message if they match.
func AssertNotEqual(t *testing.T, got interface{}, expected interface{}, msg ...interface{}) {
	require.NotEqual(t, expected, got, msg...)
}

// AssertNoErr will check if the input error is nil, and if not
// it will fail the test with an appropriate error message.
func AssertNoErr(t *testing.T, err error) {
	require.Equal(t, nil, err, "received unexpected error: %s", err)
}

// AssertErrContains will first check if the error that the error
// indeed is not nil, and then check if its error message contains
// all the substrs specified on the substrs argument.
//
// In case either assertion fails it will fail the test with
// an appropriate error message.
func AssertErrContains(t *testing.T, err error, substrs ...string) {
	require.NotEqual(t, nil, err, "expected an error but the error is nil")

	msg := err.Error()

	for _, substr := range substrs {
		require.True(t,
			strings.Contains(msg, substr),
			"missing substring '%s' in error message: '%s'",
			substr, msg,
		)
	}
}

// AssertApproxDuration checks if the durations v1 and v2 are close up to the tolerance specified.
// The format and args slice can be used for generating an appropriate error message if they are not.
func AssertApproxDuration(t *testing.T, tolerance time.Duration, v1, v2 time.Duration, format string, args ...interface{}) {
	diff := v1 - v2
	if diff < 0 {
		diff = -diff
	}

	require.True(t, diff <= tolerance, fmt.Sprintf(format, args...))
}

// AssertApproxTime checks if the times v1 and v2 are close up to the tolerance specified.
// The format and args slice can be used for generating an appropriate error message if they are not.
func AssertApproxTime(t *testing.T, tolerance time.Duration, v1, v2 time.Time, format string, args ...interface{}) {
	diff := v1.Sub(v2)
	if diff < 0 {
		diff = -diff
	}

	require.True(t, diff <= tolerance, fmt.Sprintf(format, args...))
}
