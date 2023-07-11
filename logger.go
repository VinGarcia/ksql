package ksql

import "context"

type loggerKey struct{}

type LogValues struct {
	Query  string
	Params []interface{}
	Err    error
}

func InjectLogger(
	ctx context.Context,
	level string,
	logFn func(ctx context.Context, values LogValues),
) context.Context {
	if level != "info" {
		level = "error"
	}

	return context.WithValue(ctx, loggerKey{}, logger{
		level: level,
		logFn: func(ctx context.Context, query string, params []interface{}, err error) {
			logFn(ctx, LogValues{
				Query:  query,
				Params: params,
				Err:    err,
			})
		},
	})
}

func ctxLog(ctx context.Context, query string, params []interface{}, err *error) {
	l := ctx.Value(loggerKey{})
	if l == nil {
		return
	}

	if *err != nil {
		l.(logger)._error(ctx, query, params, *err)
		return
	}

	l.(logger)._info(ctx, query, params, nil)
}

type logger struct {
	level string
	logFn func(ctx context.Context, query string, params []interface{}, err error)
}

func (l logger) _info(ctx context.Context, query string, params []interface{}, err error) {
	if l.level == "error" {
		return
	}

	l.logFn(ctx, query, params, err)
}

func (l logger) _error(ctx context.Context, query string, params []interface{}, err error) {
	l.logFn(ctx, query, params, err)
}
