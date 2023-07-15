package ksql

import (
	"context"
	"errors"
	"fmt"
	"testing"

	tt "github.com/vingarcia/ksql/internal/testtools"
)

func TestCtxLog(t *testing.T) {
	ctx := context.Background()

	defer func() {
		logPrinter = fmt.Println
	}()

	t.Run("should not log anything nor panic when the logger is not injected", func(t *testing.T) {
		var printedArgs []interface{}
		logPrinter = func(args ...interface{}) (n int, err error) {
			printedArgs = args
			return 0, nil
		}

		panicPayload := tt.PanicHandler(func() {
			ctxLog(ctx, "fakeQuery", []interface{}{}, nil)
		})
		tt.AssertEqual(t, panicPayload, nil)
		tt.AssertEqual(t, printedArgs, []interface{}(nil))
	})

}

func TestBuiltinLoggers(t *testing.T) {
	ctx := context.Background()

	defer func() {
		logPrinter = fmt.Println
	}()

	t.Run("Logger", func(t *testing.T) {
		t.Run("with no errors", func(t *testing.T) {
			var printedArgs []interface{}
			logPrinter = func(args ...interface{}) (n int, err error) {
				printedArgs = args
				return 0, nil
			}

			Logger(ctx, LogValues{
				Query:  "FakeQuery",
				Params: []interface{}{"FakeParam"},
			})

			tt.AssertContains(t, fmt.Sprint(printedArgs...), "FakeQuery", "FakeParam")
		})

		t.Run("with errors", func(t *testing.T) {
			var printedArgs []interface{}
			logPrinter = func(args ...interface{}) (n int, err error) {
				printedArgs = args
				return 0, nil
			}

			Logger(ctx, LogValues{
				Query:  "FakeQuery",
				Params: []interface{}{"FakeParam"},
				Err:    errors.New("fakeErrMsg"),
			})

			tt.AssertContains(t, fmt.Sprint(printedArgs...), "FakeQuery", "FakeParam", "fakeErrMsg")
		})
	})

	t.Run("ErrorsLogger", func(t *testing.T) {
		t.Run("with no errors", func(t *testing.T) {
			var printedArgs []interface{}
			logPrinter = func(args ...interface{}) (n int, err error) {
				printedArgs = args
				return 0, nil
			}

			ErrorLogger(ctx, LogValues{
				Query:  "FakeQuery",
				Params: []interface{}{"FakeParam"},
			})

			tt.AssertEqual(t, printedArgs, []interface{}(nil))
		})

		t.Run("with errors", func(t *testing.T) {
			var printedArgs []interface{}
			logPrinter = func(args ...interface{}) (n int, err error) {
				printedArgs = args
				return 0, nil
			}

			ErrorLogger(ctx, LogValues{
				Query:  "FakeQuery",
				Params: []interface{}{"FakeParam"},
				Err:    errors.New("fakeErrMsg"),
			})

			tt.AssertContains(t, fmt.Sprint(printedArgs...), "FakeQuery", "FakeParam", "fakeErrMsg")
		})
	})
}
