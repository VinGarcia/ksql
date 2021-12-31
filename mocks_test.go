// This test was written mostly for test coverage since the mock is trivial
package ksql_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/vingarcia/ksql"
	tt "github.com/vingarcia/ksql/internal/testtools"
)

func TestMock(t *testing.T) {
	UsersTable := ksql.NewTable("users", "id")
	type User struct {
		ID   int    `ksql:"id"`
		Name string `ksql:"name"`
		Age  int    `ksql:"age"`
	}

	t.Run("testing unset behaviors for all methods", func(t *testing.T) {
		t.Run("Insert should panic", func(t *testing.T) {
			ctx := context.Background()
			mock := ksql.Mock{}
			panicPayload := tt.PanicHandler(func() {
				mock.Insert(ctx, UsersTable, &User{
					Name: "fake-name",
					Age:  42,
				})
			})

			err, ok := panicPayload.(error)
			tt.AssertEqual(t, ok, true)
			tt.AssertErrContains(t, err, "ksql.Mock.Insert(", "ksql.Mock.InsertFn", "not set")
		})

		t.Run("Update should panic", func(t *testing.T) {
			ctx := context.Background()
			mock := ksql.Mock{}
			panicPayload := tt.PanicHandler(func() {
				mock.Update(ctx, UsersTable, &User{
					ID:   4242,
					Name: "fake-name",
					Age:  42,
				})
			})

			err, ok := panicPayload.(error)
			tt.AssertEqual(t, ok, true)
			tt.AssertErrContains(t, err, "ksql.Mock.Update(", "ksql.Mock.UpdateFn", "not set")
		})

		t.Run("Delete should panic", func(t *testing.T) {
			ctx := context.Background()
			mock := ksql.Mock{}
			panicPayload := tt.PanicHandler(func() {
				mock.Delete(ctx, UsersTable, &User{
					ID:   4242,
					Name: "fake-name",
					Age:  42,
				})
			})

			err, ok := panicPayload.(error)
			tt.AssertEqual(t, ok, true)
			tt.AssertErrContains(t, err, "ksql.Mock.Delete(", "ksql.Mock.DeleteFn", "not set")
		})

		t.Run("Query should panic", func(t *testing.T) {
			ctx := context.Background()
			mock := ksql.Mock{}
			panicPayload := tt.PanicHandler(func() {
				var users []User
				mock.Query(ctx, &users, "SELECT * FROM user WHERE age = ?", 42)
			})

			err, ok := panicPayload.(error)
			tt.AssertEqual(t, ok, true)
			tt.AssertErrContains(t, err, "ksql.Mock.Query(", "ksql.Mock.QueryFn", "not set")
		})

		t.Run("QueryOne should panic", func(t *testing.T) {
			ctx := context.Background()
			mock := ksql.Mock{}
			panicPayload := tt.PanicHandler(func() {
				var user User
				mock.QueryOne(ctx, &user, "SELECT * FROM user WHERE id = ?", 4242)
			})

			err, ok := panicPayload.(error)
			tt.AssertEqual(t, ok, true)
			tt.AssertErrContains(t, err, "ksql.Mock.QueryOne(", "ksql.Mock.QueryOneFn", "not set")
		})

		t.Run("QueryChunks should panic", func(t *testing.T) {
			ctx := context.Background()
			mock := ksql.Mock{}
			panicPayload := tt.PanicHandler(func() {
				var users []User
				mock.QueryChunks(ctx, ksql.ChunkParser{
					Query: "SELECT * FROM users WHERE age = ?",
					Params: []interface{}{
						4242,
					},
					ChunkSize: 10,
					ForEachChunk: func(chunk []User) error {
						users = append(users, chunk...)
						return nil
					},
				})
			})

			err, ok := panicPayload.(error)
			tt.AssertEqual(t, ok, true)
			tt.AssertErrContains(t, err, "ksql.Mock.QueryChunks(", "ksql.Mock.QueryChunksFn", "not set")
		})

		t.Run("Exec should panic", func(t *testing.T) {
			ctx := context.Background()
			mock := ksql.Mock{}
			panicPayload := tt.PanicHandler(func() {
				mock.Exec(ctx, "INSERT INTO users_permissions(user_id, permission_id) VALUES (?, ?)", 4242, 4)
			})

			err, ok := panicPayload.(error)
			tt.AssertEqual(t, ok, true)
			tt.AssertErrContains(t, err, "ksql.Mock.Exec(", "ksql.Mock.ExecFn", "not set")
		})

		t.Run("Transaction should not panic", func(t *testing.T) {
			ctx := context.Background()
			mock := ksql.Mock{}

			executed := false
			panicPayload := tt.PanicHandler(func() {
				mock.Transaction(ctx, func(db ksql.Provider) error {
					executed = true
					return nil
				})
			})

			tt.AssertEqual(t, panicPayload, nil)
			tt.AssertEqual(t, executed, true)
		})
	})

	t.Run("should call the user provided behavior correctly", func(t *testing.T) {
		t.Run("Insert", func(t *testing.T) {
			ctx := context.WithValue(context.Background(), "key", "value")
			var capturedArgs struct {
				ctx    context.Context
				table  ksql.Table
				record interface{}
			}
			mock := ksql.Mock{
				InsertFn: func(ctx context.Context, table ksql.Table, record interface{}) error {
					capturedArgs.ctx = ctx
					capturedArgs.table = table
					capturedArgs.record = record
					return fmt.Errorf("fake-error")
				},
			}
			err := mock.Insert(ctx, UsersTable, &User{
				Name: "fake-name",
				Age:  42,
			})

			tt.AssertErrContains(t, err, "fake-error")
			tt.AssertEqual(t, capturedArgs.ctx, ctx)
			tt.AssertEqual(t, capturedArgs.table, UsersTable)
			tt.AssertEqual(t, capturedArgs.record, &User{
				Name: "fake-name",
				Age:  42,
			})
		})

		t.Run("Update", func(t *testing.T) {
			ctx := context.Background()
			var capturedArgs struct {
				ctx    context.Context
				table  ksql.Table
				record interface{}
			}
			mock := ksql.Mock{
				UpdateFn: func(ctx context.Context, table ksql.Table, record interface{}) error {
					capturedArgs.ctx = ctx
					capturedArgs.table = table
					capturedArgs.record = record
					return fmt.Errorf("fake-error")
				},
			}
			err := mock.Update(ctx, UsersTable, &User{
				ID:   4242,
				Name: "fake-name",
				Age:  42,
			})

			tt.AssertErrContains(t, err, "fake-error")
			tt.AssertEqual(t, capturedArgs.ctx, ctx)
			tt.AssertEqual(t, capturedArgs.table, UsersTable)
			tt.AssertEqual(t, capturedArgs.record, &User{
				ID:   4242,
				Name: "fake-name",
				Age:  42,
			})
		})

		t.Run("Delete", func(t *testing.T) {
			ctx := context.Background()
			var capturedArgs struct {
				ctx    context.Context
				table  ksql.Table
				record interface{}
			}
			mock := ksql.Mock{
				DeleteFn: func(ctx context.Context, table ksql.Table, record interface{}) error {
					capturedArgs.ctx = ctx
					capturedArgs.table = table
					capturedArgs.record = record
					return fmt.Errorf("fake-error")
				},
			}
			err := mock.Delete(ctx, UsersTable, &User{
				ID:   4242,
				Name: "fake-name",
				Age:  42,
			})

			tt.AssertErrContains(t, err, "fake-error")
			tt.AssertEqual(t, capturedArgs.ctx, ctx)
			tt.AssertEqual(t, capturedArgs.table, UsersTable)
			tt.AssertEqual(t, capturedArgs.record, &User{
				ID:   4242,
				Name: "fake-name",
				Age:  42,
			})
		})

		t.Run("Query", func(t *testing.T) {
			ctx := context.Background()
			var capturedArgs struct {
				ctx     context.Context
				records interface{}
				query   string
				params  []interface{}
			}
			mock := ksql.Mock{
				QueryFn: func(ctx context.Context, records interface{}, query string, params ...interface{}) error {
					capturedArgs.ctx = ctx
					capturedArgs.records = records
					capturedArgs.query = query
					capturedArgs.params = params
					return fmt.Errorf("fake-error")
				},
			}
			var users []User
			err := mock.Query(ctx, &users, "SELECT * FROM user WHERE age = ?", 42)

			tt.AssertErrContains(t, err, "fake-error")
			tt.AssertEqual(t, capturedArgs.ctx, ctx)
			tt.AssertEqual(t, capturedArgs.records, &users)
			tt.AssertEqual(t, capturedArgs.query, "SELECT * FROM user WHERE age = ?")
			tt.AssertEqual(t, capturedArgs.params, []interface{}{42})
		})

		t.Run("QueryOne", func(t *testing.T) {
			ctx := context.Background()
			var capturedArgs struct {
				ctx    context.Context
				record interface{}
				query  string
				params []interface{}
			}
			mock := ksql.Mock{
				QueryOneFn: func(ctx context.Context, record interface{}, query string, params ...interface{}) error {
					capturedArgs.ctx = ctx
					capturedArgs.record = record
					capturedArgs.query = query
					capturedArgs.params = params
					return fmt.Errorf("fake-error")
				},
			}

			var user User
			err := mock.QueryOne(ctx, &user, "SELECT * FROM user WHERE id = ?", 4242)

			tt.AssertErrContains(t, err, "fake-error")
			tt.AssertEqual(t, capturedArgs.ctx, ctx)
			tt.AssertEqual(t, capturedArgs.record, &user)
			tt.AssertEqual(t, capturedArgs.query, "SELECT * FROM user WHERE id = ?")
			tt.AssertEqual(t, capturedArgs.params, []interface{}{4242})
		})

		t.Run("QueryChunks", func(t *testing.T) {
			ctx := context.Background()
			var capturedArgs struct {
				ctx    context.Context
				parser ksql.ChunkParser
			}
			mock := ksql.Mock{
				QueryChunksFn: func(ctx context.Context, parser ksql.ChunkParser) error {
					capturedArgs.ctx = ctx
					capturedArgs.parser = parser
					return fmt.Errorf("fake-error")
				},
			}

			var users []User
			chunkParserFunc := func(chunk []User) error {
				users = append(users, chunk...)
				return nil
			}
			err := mock.QueryChunks(ctx, ksql.ChunkParser{
				Query: "SELECT * FROM users WHERE age = ?",
				Params: []interface{}{
					4242,
				},
				ChunkSize:    10,
				ForEachChunk: chunkParserFunc,
			})

			tt.AssertErrContains(t, err, "fake-error")
			tt.AssertEqual(t, capturedArgs.ctx, ctx)
			tt.AssertEqual(t, capturedArgs.parser.Query, "SELECT * FROM users WHERE age = ?")
			tt.AssertEqual(t, capturedArgs.parser.Params, []interface{}{
				4242,
			})
			tt.AssertEqual(t, capturedArgs.parser.ChunkSize, 10)
		})

		t.Run("Exec", func(t *testing.T) {
			ctx := context.Background()
			var capturedArgs struct {
				ctx    context.Context
				query  string
				params []interface{}
			}
			mock := ksql.Mock{
				ExecFn: func(ctx context.Context, query string, params ...interface{}) (rowsAffected int64, _ error) {
					capturedArgs.ctx = ctx
					capturedArgs.query = query
					capturedArgs.params = params
					return 42, fmt.Errorf("fake-error")
				},
			}
			rowsAffected, err := mock.Exec(ctx, "INSERT INTO users_permissions(user_id, permission_id) VALUES (?, ?)", 4242, 4)

			tt.AssertErrContains(t, err, "fake-error")
			tt.AssertEqual(t, rowsAffected, int64(42))
			tt.AssertEqual(t, capturedArgs.ctx, ctx)
			tt.AssertEqual(t, capturedArgs.query, "INSERT INTO users_permissions(user_id, permission_id) VALUES (?, ?)")
			tt.AssertEqual(t, capturedArgs.params, []interface{}{4242, 4})
		})

		t.Run("Transaction", func(t *testing.T) {
			ctx := context.Background()
			executingMockedTransaction := false
			mock := ksql.Mock{
				TransactionFn: func(ctx context.Context, fn func(db ksql.Provider) error) error {
					executingMockedTransaction = true
					return nil
				},
			}

			executed := false
			mock.Transaction(ctx, func(db ksql.Provider) error {
				executed = true
				return nil
			})

			tt.AssertEqual(t, executingMockedTransaction, true)
			tt.AssertEqual(t, executed, false)
		})
	})
}
