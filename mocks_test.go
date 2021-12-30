// This test was written mostly for test coverage since the mock is trivial
package ksql_test

import (
	"context"
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
}
