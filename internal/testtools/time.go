package tt

import (
	"testing"
	"time"
)

func ParseTime(t *testing.T, timestr string) time.Time {
	parsedTime, err := time.Parse(time.RFC3339, timestr)
	AssertNoErr(t, err)
	return parsedTime
}
