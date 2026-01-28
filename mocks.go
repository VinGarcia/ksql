package ksql

import (
	"context"
	"fmt"

	"github.com/vingarcia/ksql/sqldialect"
)

var _ Provider = Mock{}

// Mock implements the Provider interface in order to allow users
// to easily mock the behavior of a ksql.Provider.
//
// To mock a particular method, e.g. Insert, you just need to overwrite
// the corresponding function attribute whose name is InsertFn().
//
// NOTE: This mock should be instantiated inside each unit test not globally.
//
// For capturing input values use a closure as in the example:
//
//	var insertRecord interface{}
//	dbMock := Mock{
//		InsertFn: func(ctx context.Context, table Table, record interface{}) error {
//			insertRecord = record
//		},
//	}
//
// NOTE: It is recommended not to make assertions inside the mocked methods,
// you should only check the captured values afterwards as all tests should
// have 3 stages: (1) setup, (2) run and finally (3) assert.
//
// For cases where the function will be called several times you might want to capture
// the number of calls as well as the values passed each time for that
// use closures and a slice of values, e.g.:
//
//	var insertRecords []interface{}
//	dbMock := Mock{
//		InsertFn: func(ctx context.Context, table Table, record interface{}) error {
//			insertRecords = append(insertRecords, record)
//		},
//	}
//
//	expectedNumberOfCalls := 2
//	assert.Equal(t, expectedNumberOfCalls, len(insertRecords))
//
//	expectedInsertedRecords := []interface{}{
//		user1,
//		user2,
//	}
//	assert.Equal(t, expectedInsertedRecords, insertRecords)
type Mock struct {
	InsertFn func(ctx context.Context, table Table, record interface{}) error
	PatchFn  func(ctx context.Context, table Table, record interface{}) error
	DeleteFn func(ctx context.Context, table Table, idOrRecord interface{}) error

	QueryFn       func(ctx context.Context, records interface{}, query string, params ...interface{}) error
	QueryOneFn    func(ctx context.Context, record interface{}, query string, params ...interface{}) error
	QueryChunksFn func(ctx context.Context, parser ChunkParser) error

	ExecFn        func(ctx context.Context, query string, params ...interface{}) (Result, error)
	TransactionFn func(ctx context.Context, fn func(db Provider) error) error

	QueryFromBuilderFn func(ctx context.Context, records interface{}, builder QueryBuilder) (err error)
	ExecFromBuilderFn  func(ctx context.Context, builder QueryBuilder) (_ Result, err error)
}

// MockResult implements the Result interface returned by the Exec function
//
// Use the constructor `NewMockResult(42, 42)` for a simpler instantiation of this mock.
//
// But if you want one of the functions to return an error you'll need
// to specify the desired behavior by overwriting one of the attributes
// of the struct.
type MockResult struct {
	LastInsertIdFn func() (int64, error)
	RowsAffectedFn func() (int64, error)
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
//		PatchFn: func(_ context.Context, _ ksql.Table, record interface{}) error {
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
	if m.PatchFn == nil {
		m.PatchFn = db.Patch
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

	if m.QueryFromBuilderFn == nil {
		m.QueryFromBuilderFn = db.QueryFromBuilder
	}
	if m.ExecFromBuilderFn == nil {
		m.ExecFromBuilderFn = db.ExecFromBuilder
	}

	return m
}

// Insert mocks the behavior of the Insert method.
// If InsertFn is set it will just call it returning the same return values.
// If InsertFn is unset it will panic with an appropriate error message.
func (m Mock) Insert(ctx context.Context, table Table, record interface{}) error {
	if m.InsertFn == nil {
		panic(fmt.Errorf("ksql.Mock.Insert(ctx, %v, %v) called but the ksql.Mock.InsertFn() is not set", table, record))
	}
	return m.InsertFn(ctx, table, record)
}

// Patch mocks the behavior of the Patch method.
// If PatchFn is set it will just call it returning the same return values.
// If PatchFn is unset it will panic with an appropriate error message.
func (m Mock) Patch(ctx context.Context, table Table, record interface{}) error {
	if m.PatchFn == nil {
		panic(fmt.Errorf("ksql.Mock.Patch(ctx, %v, %v) called but the ksql.Mock.PatchFn() is not set", table, record))
	}
	return m.PatchFn(ctx, table, record)
}

// Delete mocks the behavior of the Delete method.
// If DeleteFn is set it will just call it returning the same return values.
// If DeleteFn is unset it will panic with an appropriate error message.
func (m Mock) Delete(ctx context.Context, table Table, idOrRecord interface{}) error {
	if m.DeleteFn == nil {
		panic(fmt.Errorf("ksql.Mock.Delete(ctx, %v, %v) called but the ksql.Mock.DeleteFn() is not set", table, idOrRecord))
	}
	return m.DeleteFn(ctx, table, idOrRecord)
}

// Query mocks the behavior of the Query method.
// If QueryFn is set it will just call it returning the same return values.
// If QueryFn is unset it will panic with an appropriate error message.
func (m Mock) Query(ctx context.Context, records interface{}, query string, params ...interface{}) error {
	if m.QueryFn == nil {
		panic(fmt.Errorf("ksql.Mock.Query(ctx, %v, %s, %v) called but the ksql.Mock.QueryFn() is not set", records, query, params))
	}
	return m.QueryFn(ctx, records, query, params...)
}

// QueryOne mocks the behavior of the QueryOne method.
// If QueryOneFn is set it will just call it returning the same return values.
// If QueryOneFn is unset it will panic with an appropriate error message.
func (m Mock) QueryOne(ctx context.Context, record interface{}, query string, params ...interface{}) error {
	if m.QueryOneFn == nil {
		panic(fmt.Errorf("ksql.Mock.QueryOne(ctx, %v, %s, %v) called but the ksql.Mock.QueryOneFn() is not set", record, query, params))
	}
	return m.QueryOneFn(ctx, record, query, params...)
}

// QueryChunks mocks the behavior of the QueryChunks method.
// If QueryChunksFn is set it will just call it returning the same return values.
// If QueryChunksFn is unset it will panic with an appropriate error message.
func (m Mock) QueryChunks(ctx context.Context, parser ChunkParser) error {
	if m.QueryChunksFn == nil {
		panic(fmt.Errorf("ksql.Mock.QueryChunks(ctx, %v) called but the ksql.Mock.QueryChunksFn() is not set", parser))
	}
	return m.QueryChunksFn(ctx, parser)
}

// Exec mocks the behavior of the Exec method.
// If ExecFn is set it will just call it returning the same return values.
// If ExecFn is unset it will panic with an appropriate error message.
func (m Mock) Exec(ctx context.Context, query string, params ...interface{}) (Result, error) {
	if m.ExecFn == nil {
		panic(fmt.Errorf("ksql.Mock.Exec(ctx, %s, %v) called but the ksql.Mock.ExecFn() is not set", query, params))
	}
	return m.ExecFn(ctx, query, params...)
}

// Transaction mocks the behavior of the Transaction method.
// If TransactionFn is set it will just call it returning the same return values.
// If TransactionFn is unset it will just call the input function
// passing the Mock itself as the database.
func (m Mock) Transaction(ctx context.Context, fn func(db Provider) error) error {
	if m.TransactionFn == nil {
		return fn(m)
	}
	return m.TransactionFn(ctx, fn)
}

// QueryFromBuilder mocks the behavior of the QueryFromBuilder method.
// If QueryFromBuilderFn is set it will just call it returning the same return values.
// If QueryFromBuilderFn is unset it will panic with an appropriate error message.
func (m Mock) QueryFromBuilder(ctx context.Context, records interface{}, builder QueryBuilder) (err error) {
	if m.QueryFromBuilderFn == nil {
		panic(fmt.Errorf("ksql.Mock.QueryFromBuilder(ctx, %+v) called but the ksql.Mock.QueryFromBuilderFn() is not set", builder))
	}
	return m.QueryFromBuilderFn(ctx, records, builder)
}

// ExecFromBuilder mocks the behavior of the ExecFromBuilder method.
// If ExecFromBuilderFn is set it will just call it returning the same return values.
// If ExecFromBuilderFn is unset it will panic with an appropriate error message.
func (m Mock) ExecFromBuilder(ctx context.Context, builder QueryBuilder) (_ Result, err error) {
	if m.ExecFromBuilderFn == nil {
		panic(fmt.Errorf("ksql.Mock.ExecFromBuilder(ctx, %+v) called but the ksql.Mock.ExecFromBuilderFn() is not set", builder))
	}
	return m.ExecFromBuilderFn(ctx, builder)
}

// NewMockResult returns a simple implementation of the Result interface.
func NewMockResult(lastInsertID int64, rowsAffected int64) Result {
	return MockResult{
		LastInsertIdFn: func() (int64, error) { return lastInsertID, nil },
		RowsAffectedFn: func() (int64, error) { return rowsAffected, nil },
	}
}

// LastInsertId implements the Result interface
func (m MockResult) LastInsertId() (int64, error) {
	if m.LastInsertIdFn == nil {
		panic(fmt.Errorf("ksql.MockResult.LastInsertId() called but ksql.MockResult.LastInsertIdFn is not set"))
	}
	return m.LastInsertIdFn()
}

// RowsAffected implements the Result interface
func (m MockResult) RowsAffected() (int64, error) {
	if m.RowsAffectedFn == nil {
		panic(fmt.Errorf("ksql.MockResult.RowsAffected() called but ksql.MockResult.RowsAffectedFn is not set"))
	}
	return m.RowsAffectedFn()
}

// MockQueryBuilder implements the QueryBuilder interface in order to allow users
// to easily mock the behavior of a ksql.QueryBuilder.
//
// To mock the BuildQuery method, you just need to set the BuildQueryFn attribute.
//
// NOTE: This mock should be instantiated inside each unit test not globally.
//
// Example Usage:
//
//	var capturedDialect sqldialect.Provider
//	mockBuilder := MockQueryBuilder{
//		BuildQueryFn: func(dialect sqldialect.Provider) (string, []interface{}, error) {
//			capturedDialect = dialect
//			return "SELECT * FROM users WHERE id = ?", []interface{}{42}, nil
//		},
//	}
type MockQueryBuilder struct {
	BuildQueryFn func(dialect sqldialect.Provider) (sqlQuery string, params []interface{}, _ error)
}

// BuildQuery mocks the behavior of the BuildQuery method.
// If BuildQueryFn is set it will just call it returning the same return values.
// If BuildQueryFn is unset it will panic with an appropriate error message.
func (m MockQueryBuilder) BuildQuery(dialect sqldialect.Provider) (sqlQuery string, params []interface{}, _ error) {
	if m.BuildQueryFn == nil {
		panic(fmt.Errorf("ksql.MockQueryBuilder.BuildQuery(dialect) called but ksql.MockQueryBuilder.BuildQueryFn is not set"))
	}
	return m.BuildQueryFn(dialect)
}
