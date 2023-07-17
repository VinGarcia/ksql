package ksql

import (
	"context"
	"encoding/json"
	"fmt"
)

// This variable is only used during tests:
var logPrinter = fmt.Println

var _ LoggerFn = ErrorLogger

// ErrorLogger is a builtin logger that can be passed to
// ksql.InjectLogger() to only log when an error occurs.
//
// Note that only errors that happen after KSQL sends the
// query to the backend adapter will be logged.
// Any errors that happen before that will not be logged.
func ErrorLogger(ctx context.Context, values LogValues) {
	if values.Err == nil {
		return
	}

	Logger(ctx, values)
}

var _ LoggerFn = Logger

// Logger is a builtin logger that can be passed to
// ksql.InjectLogger() to log every query and query errors.
//
// Note that only errors that happen after KSQL sends the
// query to the backend adapter will be logged.
// Any errors that happen before that will not be logged.
func Logger(ctx context.Context, values LogValues) {
	b, _ := json.Marshal(values)
	logPrinter(string(b))
}

type loggerKey struct{}

// LogValues is the argument type of ksql.LoggerFn which contains
// the data available for logging whenever a query is executed.
type LogValues struct {
	Query  string
	Params []interface{}
	Err    error
}

func (l LogValues) MarshalJSON() ([]byte, error) {
	var out struct {
		Query  string        `json:"query"`
		Params []interface{} `json:"params"`
		Err    string        `json:"error,omitempty"`
	}

	out.Query = l.Query

	out.Params = l.Params

	// Force it to print Params: [], instead of Params: null
	if out.Params == nil {
		out.Params = []interface{}{}
	}

	if l.Err != nil {
		out.Err = l.Err.Error()
	}
	return json.Marshal(out)
}

// LoggerFn is a the type of function received as
// argument of the ksql.InjectLogger function.
type LoggerFn func(ctx context.Context, values LogValues)

type loggerFn func(ctx context.Context, query string, params []interface{}, err error)

// InjectLogger is a debugging tool that allows the user to force
// KSQL to log the query, query params and error response whenever
// a query is executed.
//
// Example Usage:
//
//     // After injecting a logger into `ctx` all subsequent queries
//     // that use this context will be logged.
//     ctx = ksql.InjectLogger(ctx, ksql.Logger)
//
//     // All the calls below will cause KSQL to log the queries:
//     var user User
//     db.Insert(ctx, usersTable, &user)
//
//     user.Name = "NewName"
//     db.Patch(ctx, usersTable, &user)
//
//     var users []User
//     db.Query(ctx, &users, someQuery, someParams...)
//     db.QueryOne(ctx, &user, someQuery, someParams...)
//
//     db.Delete(ctx, usersTable, user.ID)
//
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
