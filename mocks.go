package ksql

import (
	"context"
	"fmt"
)

var _ Provider = Mock{}

// Mock ...
type Mock struct {
	InsertFn func(ctx context.Context, table Table, record interface{}) error
	UpdateFn func(ctx context.Context, table Table, record interface{}) error
	DeleteFn func(ctx context.Context, table Table, idOrRecord interface{}) error

	QueryFn       func(ctx context.Context, records interface{}, query string, params ...interface{}) error
	QueryOneFn    func(ctx context.Context, record interface{}, query string, params ...interface{}) error
	QueryChunksFn func(ctx context.Context, parser ChunkParser) error

	ExecFn        func(ctx context.Context, query string, params ...interface{}) (rowsAffected int64, _ error)
	TransactionFn func(ctx context.Context, fn func(db Provider) error) error
}

// SetFallbackDatabase will set all the Fn attributes to use
// the function from the input database.
//
// SetFallbackDatabase is useful when you only want to
// overwrite some of the operations, e.g. for testing errors
// or if you want to use the same setup for making unit tests
// and integration tests, this way instead of creating a new server
// with a real database and another with a mocked one you can start
// the server once and run both types of tests.
//
// Example Usage:
//
//	db, err := ksql.New(...)
//	if err != nil {
//		t.Fatal(err.Error())
//	}
//
//	mockdb := ksql.Mock{
//		UpdateFn: func(_ context.Context, _ ksql.Table, record interface{}) error {
//			return ksql.ErrRecordNotFound
//		},
//	}.SetFallbackDatabase(db)
//
//	// Passing the address to the service so
//	// you can change it for each test
//	myService := myservice.New(..., &mockdb, ...)
func (m Mock) SetFallbackDatabase(db Provider) Mock {
	if m.InsertFn == nil {
		m.InsertFn = db.Insert
	}
	if m.UpdateFn == nil {
		m.UpdateFn = db.Update
	}
	if m.DeleteFn == nil {
		m.DeleteFn = db.Delete
	}

	if m.QueryFn == nil {
		m.QueryFn = db.Query
	}
	if m.QueryOneFn == nil {
		m.QueryOneFn = db.QueryOne
	}
	if m.QueryChunksFn == nil {
		m.QueryChunksFn = db.QueryChunks
	}

	if m.ExecFn == nil {
		m.ExecFn = db.Exec
	}
	if m.TransactionFn == nil {
		m.TransactionFn = db.Transaction
	}

	return m
}

// Insert ...
func (m Mock) Insert(ctx context.Context, table Table, record interface{}) error {
	if m.InsertFn == nil {
		panic(fmt.Errorf("Mock.Insert(ctx, %v, %v) called but the ksql.Mock.InsertFn() is not set", table, record))
	}
	return m.InsertFn(ctx, table, record)
}

// Update ...
func (m Mock) Update(ctx context.Context, table Table, record interface{}) error {
	if m.UpdateFn == nil {
		panic(fmt.Errorf("Mock.Update(ctx, %v, %v) called but the ksql.Mock.UpdateFn() is not set", table, record))
	}
	return m.UpdateFn(ctx, table, record)
}

// Delete ...
func (m Mock) Delete(ctx context.Context, table Table, idOrRecord interface{}) error {
	if m.DeleteFn == nil {
		panic(fmt.Errorf("Mock.Delete(ctx, %v, %v) called but the ksql.Mock.DeleteFn() is not set", table, idOrRecord))
	}
	return m.DeleteFn(ctx, table, idOrRecord)
}

// Query ...
func (m Mock) Query(ctx context.Context, records interface{}, query string, params ...interface{}) error {
	if m.QueryFn == nil {
		panic(fmt.Errorf("Mock.Query(ctx, %v, %s, %v) called but the ksql.Mock.QueryFn() is not set", records, query, params))
	}
	return m.QueryFn(ctx, records, query, params...)
}

// QueryOne ...
func (m Mock) QueryOne(ctx context.Context, record interface{}, query string, params ...interface{}) error {
	if m.QueryOneFn == nil {
		panic(fmt.Errorf("Mock.QueryOne(ctx, %v, %s, %v) called but the ksql.Mock.QueryOneFn() is not set", record, query, params))
	}
	return m.QueryOneFn(ctx, record, query, params...)
}

// QueryChunks ...
func (m Mock) QueryChunks(ctx context.Context, parser ChunkParser) error {
	if m.QueryChunksFn == nil {
		panic(fmt.Errorf("Mock.QueryChunks(ctx, %v) called but the ksql.Mock.QueryChunksFn() is not set", parser))
	}
	return m.QueryChunksFn(ctx, parser)
}

// Exec ...
func (m Mock) Exec(ctx context.Context, query string, params ...interface{}) (rowsAffected int64, _ error) {
	if m.ExecFn == nil {
		panic(fmt.Errorf("Mock.Exec(ctx, %s, %v) called but the ksql.Mock.ExecFn() is not set", query, params))
	}
	return m.ExecFn(ctx, query, params...)
}

// Transaction ...
func (m Mock) Transaction(ctx context.Context, fn func(db Provider) error) error {
	if m.TransactionFn == nil {
		return fn(m)
	}
	return m.TransactionFn(ctx, fn)
}
