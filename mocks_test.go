// This test was written mostly for test coverage since the mock is trivial
package ksql_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/vingarcia/ksql"
	tt "github.com/vingarcia/ksql/internal/testtools"
	"github.com/vingarcia/ksql/sqldialect"
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

		t.Run("Patch should panic", func(t *testing.T) {
			ctx := context.Background()
			mock := ksql.Mock{}
			panicPayload := tt.PanicHandler(func() {
				mock.Patch(ctx, UsersTable, &User{
					ID:   4242,
					Name: "fake-name",
					Age:  42,
				})
			})

			err, ok := panicPayload.(error)
			tt.AssertEqual(t, ok, true)
			tt.AssertErrContains(t, err, "ksql.Mock.Patch(", "ksql.Mock.PatchFn", "not set")
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

		t.Run("QueryFromBuilder should panic", func(t *testing.T) {
			ctx := context.Background()
			mock := ksql.Mock{}
			panicPayload := tt.PanicHandler(func() {
				var users []User
				builder := ksql.MockQueryBuilder{}
				mock.QueryFromBuilder(ctx, &users, builder)
			})

			err, ok := panicPayload.(error)
			tt.AssertEqual(t, ok, true)
			tt.AssertErrContains(t, err, "ksql.Mock.QueryFromBuilder(", "ksql.Mock.QueryFromBuilderFn", "not set")
		})

		t.Run("ExecFromBuilder should panic", func(t *testing.T) {
			ctx := context.Background()
			mock := ksql.Mock{}
			panicPayload := tt.PanicHandler(func() {
				builder := ksql.MockQueryBuilder{}
				mock.ExecFromBuilder(ctx, builder)
			})

			err, ok := panicPayload.(error)
			tt.AssertEqual(t, ok, true)
			tt.AssertErrContains(t, err, "ksql.Mock.ExecFromBuilder(", "ksql.Mock.ExecFromBuilderFn", "not set")
		})
	})

	t.Run("should call the user provided behavior correctly", func(t *testing.T) {
		t.Run("Insert", func(t *testing.T) {
			ctx := context.Background()
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

		t.Run("Patch", func(t *testing.T) {
			ctx := context.Background()
			var capturedArgs struct {
				ctx    context.Context
				table  ksql.Table
				record interface{}
			}
			mock := ksql.Mock{
				PatchFn: func(ctx context.Context, table ksql.Table, record interface{}) error {
					capturedArgs.ctx = ctx
					capturedArgs.table = table
					capturedArgs.record = record
					return fmt.Errorf("fake-error")
				},
			}
			err := mock.Patch(ctx, UsersTable, &User{
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
				ExecFn: func(ctx context.Context, query string, params ...interface{}) (ksql.Result, error) {
					capturedArgs.ctx = ctx
					capturedArgs.query = query
					capturedArgs.params = params
					return ksql.NewMockResult(42, 42), fmt.Errorf("fake-error")
				},
			}
			r, err := mock.Exec(ctx, "INSERT INTO users_permissions(user_id, permission_id) VALUES (?, ?)", 4242, 4)

			tt.AssertErrContains(t, err, "fake-error")
			rowsAffected, err := r.RowsAffected()
			tt.AssertNoErr(t, err)
			tt.AssertEqual(t, rowsAffected, int64(42))
			lastInsertID, err := r.LastInsertId()
			tt.AssertNoErr(t, err)
			tt.AssertEqual(t, lastInsertID, int64(42))
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

		t.Run("QueryFromBuilder", func(t *testing.T) {
			ctx := context.Background()
			var capturedArgs struct {
				ctx     context.Context
				records interface{}
			}
			var builderCalled bool
			mock := ksql.Mock{
				QueryFromBuilderFn: func(ctx context.Context, records interface{}, builder ksql.QueryBuilder) error {
					capturedArgs.ctx = ctx
					capturedArgs.records = records
					// Call the builder to verify it was passed correctly
					_, _, _ = builder.BuildQuery(nil)
					return fmt.Errorf("fake-error")
				},
			}

			var users []User
			testBuilder := ksql.MockQueryBuilder{
				BuildQueryFn: func(dialect sqldialect.Provider) (string, []interface{}, error) {
					builderCalled = true
					return "SELECT * FROM users WHERE age = $1", []interface{}{42}, nil
				},
			}
			err := mock.QueryFromBuilder(ctx, &users, testBuilder)

			tt.AssertErrContains(t, err, "fake-error")
			tt.AssertEqual(t, capturedArgs.ctx, ctx)
			tt.AssertEqual(t, capturedArgs.records, &users)
			tt.AssertEqual(t, builderCalled, true)
		})

		t.Run("ExecFromBuilder", func(t *testing.T) {
			ctx := context.Background()
			var capturedArgs struct {
				ctx context.Context
			}
			var builderCalled bool
			mock := ksql.Mock{
				ExecFromBuilderFn: func(ctx context.Context, builder ksql.QueryBuilder) (ksql.Result, error) {
					capturedArgs.ctx = ctx
					// Call the builder to verify it was passed correctly
					_, _, _ = builder.BuildQuery(nil)
					return ksql.NewMockResult(42, 42), fmt.Errorf("fake-error")
				},
			}

			testBuilder := ksql.MockQueryBuilder{
				BuildQueryFn: func(dialect sqldialect.Provider) (string, []interface{}, error) {
					builderCalled = true
					return `INSERT INTO "users" ("name", "age") VALUES ($1, $2)`, []interface{}{"foo", 42}, nil
				},
			}
			r, err := mock.ExecFromBuilder(ctx, testBuilder)

			tt.AssertErrContains(t, err, "fake-error")
			rowsAffected, err := r.RowsAffected()
			tt.AssertNoErr(t, err)
			tt.AssertEqual(t, rowsAffected, int64(42))
			lastInsertID, err := r.LastInsertId()
			tt.AssertNoErr(t, err)
			tt.AssertEqual(t, lastInsertID, int64(42))
			tt.AssertEqual(t, capturedArgs.ctx, ctx)
			tt.AssertEqual(t, builderCalled, true)
		})
	})

	t.Run("SetFallbackDatabase", func(t *testing.T) {
		testMock := ksql.Mock{}
		dbMock := ksql.Mock{
			InsertFn: func(ctx context.Context, table ksql.Table, record interface{}) error {
				return fmt.Errorf("called from InsertFn")
			},
			PatchFn: func(ctx context.Context, table ksql.Table, record interface{}) error {
				return fmt.Errorf("called from PatchFn")
			},
			DeleteFn: func(ctx context.Context, table ksql.Table, record interface{}) error {
				return fmt.Errorf("called from DeleteFn")
			},
			QueryFn: func(ctx context.Context, records interface{}, query string, params ...interface{}) error {
				return fmt.Errorf("called from QueryFn")
			},
			QueryOneFn: func(ctx context.Context, record interface{}, query string, params ...interface{}) error {
				return fmt.Errorf("called from QueryOneFn")
			},
			QueryChunksFn: func(ctx context.Context, parser ksql.ChunkParser) error {
				return fmt.Errorf("called from QueryChunksFn")
			},
			ExecFn: func(ctx context.Context, query string, params ...interface{}) (ksql.Result, error) {
				return nil, fmt.Errorf("called from ExecFn")
			},
			TransactionFn: func(ctx context.Context, fn func(db ksql.Provider) error) error {
				return fmt.Errorf("called from TransactionFn")
			},
			QueryFromBuilderFn: func(ctx context.Context, records interface{}, builder ksql.QueryBuilder) error {
				return fmt.Errorf("called from QueryFromBuilderFn")
			},
			ExecFromBuilderFn: func(ctx context.Context, builder ksql.QueryBuilder) (ksql.Result, error) {
				return nil, fmt.Errorf("called from ExecFromBuilderFn")
			},
		}

		ctx := context.Background()
		testMock = testMock.SetFallbackDatabase(dbMock)

		var user User
		err := testMock.Insert(ctx, UsersTable, &user)
		tt.AssertErrContains(t, err, "called from InsertFn")
		err = testMock.Patch(ctx, UsersTable, &user)
		tt.AssertErrContains(t, err, "called from PatchFn")
		err = testMock.Delete(ctx, UsersTable, &user)
		tt.AssertErrContains(t, err, "called from DeleteFn")

		var users []User
		err = testMock.Query(ctx, &users, "fake-query")
		tt.AssertErrContains(t, err, "called from QueryFn")
		err = testMock.QueryOne(ctx, &user, "fake-query")
		tt.AssertErrContains(t, err, "called from QueryOneFn")
		err = testMock.QueryChunks(ctx, ksql.ChunkParser{})
		tt.AssertErrContains(t, err, "called from QueryChunksFn")
		_, err = testMock.Exec(ctx, "fake-query")
		tt.AssertErrContains(t, err, "called from ExecFn")
		err = testMock.Transaction(ctx, func(db ksql.Provider) error {
			return nil
		})
		tt.AssertErrContains(t, err, "called from TransactionFn")
		err = testMock.QueryFromBuilder(ctx, &users, ksql.MockQueryBuilder{})
		tt.AssertErrContains(t, err, "called from QueryFromBuilderFn")
		_, err = testMock.ExecFromBuilder(ctx, ksql.MockQueryBuilder{})
		tt.AssertErrContains(t, err, "called from ExecFromBuilderFn")
	})
}

func TestMockResult(t *testing.T) {
	t.Run("LastInsertId", func(t *testing.T) {
		t.Run("the constructor should work correctly", func(t *testing.T) {
			result := ksql.NewMockResult(24, 42)
			lastInsertID, err := result.LastInsertId()
			tt.AssertNoErr(t, err)
			tt.AssertEqual(t, lastInsertID, int64(24))
		})

		t.Run("should panic if no values are provided", func(t *testing.T) {
			result := ksql.MockResult{}

			panicPayload := tt.PanicHandler(func() {
				result.LastInsertId()
			})

			err, ok := panicPayload.(error)
			tt.AssertEqual(t, ok, true)
			tt.AssertErrContains(t, err, "ksql.MockResult.LastInsertId(", "ksql.MockResult.LastInsertIdFn", "not set")
		})
	})

	t.Run("RowsAffected", func(t *testing.T) {
		t.Run("the constructor should work correctly", func(t *testing.T) {
			result := ksql.NewMockResult(24, 42)
			rowsAffected, err := result.RowsAffected()
			tt.AssertNoErr(t, err)
			tt.AssertEqual(t, rowsAffected, int64(42))
		})

		t.Run("should panic if no values are provided", func(t *testing.T) {
			result := ksql.MockResult{}

			panicPayload := tt.PanicHandler(func() {
				result.RowsAffected()
			})

			err, ok := panicPayload.(error)
			tt.AssertEqual(t, ok, true)
			tt.AssertErrContains(t, err, "ksql.MockResult.RowsAffected(", "ksql.MockResult.RowsAffectedFn", "not set")
		})
	})
}

func TestMockQueryBuilder(t *testing.T) {
	t.Run("BuildQuery", func(t *testing.T) {
		t.Run("should call the provided function correctly", func(t *testing.T) {
			builder := ksql.MockQueryBuilder{
				BuildQueryFn: func(dialect sqldialect.Provider) (string, []interface{}, error) {
					return "SELECT * FROM users WHERE age = $1", []interface{}{42}, nil
				},
			}

			query, params, err := builder.BuildQuery(nil)
			tt.AssertNoErr(t, err)
			tt.AssertEqual(t, query, "SELECT * FROM users WHERE age = $1")
			tt.AssertEqual(t, params, []interface{}{42})
		})

		t.Run("should propagate errors from the BuildQueryFn", func(t *testing.T) {
			builder := ksql.MockQueryBuilder{
				BuildQueryFn: func(dialect sqldialect.Provider) (string, []interface{}, error) {
					return "", nil, fmt.Errorf("build query error")
				},
			}

			_, _, err := builder.BuildQuery(nil)
			tt.AssertErrContains(t, err, "build query error")
		})

		t.Run("should panic if BuildQueryFn is not set", func(t *testing.T) {
			builder := ksql.MockQueryBuilder{}

			panicPayload := tt.PanicHandler(func() {
				builder.BuildQuery(nil)
			})

			err, ok := panicPayload.(error)
			tt.AssertEqual(t, ok, true)
			tt.AssertErrContains(t, err, "ksql.MockQueryBuilder.BuildQuery(", "ksql.MockQueryBuilder.BuildQueryFn", "not set")
		})
	})
}
