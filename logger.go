package ksql

import (
	"context"
	"encoding/json"
	"fmt"
)

// This variable is only used during tests:
var logPrinter = fmt.Println

var _ LoggerFn = ErrorsLogger

func ErrorsLogger(ctx context.Context, values LogValues) {
	if values.Err == nil {
		return
	}

	Logger(ctx, values)
}

var _ LoggerFn = Logger

func Logger(ctx context.Context, values LogValues) {
	m := map[string]interface{}{
		"query":  values.Query,
		"params": values.Params,
	}

	if values.Err != nil {
		m["error"] = values.Err.Error()
	}

	b, _ := json.Marshal(m)
	logPrinter(string(b))
}

type loggerKey struct{}

type LogValues struct {
	Query  string
	Params []interface{}
	Err    error
}

type LoggerFn func(ctx context.Context, values LogValues)
type loggerFn func(ctx context.Context, query string, params []interface{}, err error)

func InjectLogger(
	ctx context.Context,
	logFn LoggerFn,
) context.Context {
	return context.WithValue(ctx, loggerKey{}, loggerFn(func(ctx context.Context, query string, params []interface{}, err error) {
		logFn(ctx, LogValues{
			Query:  query,
			Params: params,
			Err:    err,
		})
	}))
}

func ctxLog(ctx context.Context, query string, params []interface{}, err *error) {
	l := ctx.Value(loggerKey{})
	if l == nil {
		return
	}

	l.(loggerFn)(ctx, query, params, *err)
}
