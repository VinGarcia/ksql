package ksql

import (
	"context"
)

type loggerKey struct{}

type LogValues struct {
	Query  string
	Params []interface{}
	Err    error
}

type loggerFn func(ctx context.Context, query string, params []interface{}, err error)

func InjectLogger(
	ctx context.Context,
	logFn func(ctx context.Context, values LogValues),
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
