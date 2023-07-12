package ksql

import (
	"context"
	"testing"

	tt "github.com/vingarcia/ksql/internal/testtools"
)

func TestCtxLog(t *testing.T) {
	ctx := context.Background()

	t.Run("should not log anything nor panic if logger is nil", func(t *testing.T) {
		panicPayload := tt.PanicHandler(func() {
			ctxLog(ctx, "fakeQuery", []interface{}{}, nil)
		})
		tt.AssertEqual(t, panicPayload, nil)
	})
}
