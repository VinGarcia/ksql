package exampleservice

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vingarcia/ksql"
	"github.com/vingarcia/ksql/ksqltest"
	"github.com/vingarcia/ksql/nullable"
)

func TestCreateUserWithKsqlMock(t *testing.T) {
	t.Run("should call ksql.Insert correctly", func(t *testing.T) {
		var users []interface{}
		mockDB := ksql.Mock{
			InsertFn: func(ctx context.Context, table ksql.Table, record interface{}) error {
				users = append(users, record)
				return nil
			},
		}

		s := Service{
			db:              mockDB,
			streamChunkSize: 100,
		}

		user := UserEntity{Name: nullable.String("TestUser")}

		err := s.CreateUser(context.TODO(), user)
		assert.Equal(t, nil, err)
		assert.Equal(t, 1, len(users))
		assert.Equal(t, &user, users[0])
	})

	t.Run("another way of testing input structs", func(t *testing.T) {
		var users []map[string]interface{}
		mockDB := ksql.Mock{
			InsertFn: func(ctx context.Context, table ksql.Table, record interface{}) error {
				// The StructToMap function will convert a struct with `ksql` tags
				// into a map using the ksql attr names as keys.
				//
				// If you are inserting an anonymous struct (not usual) this function
				// can make your tests shorter:
				uMap, err := ksqltest.StructToMap(record)
				if err != nil {
					return err
				}
				users = append(users, uMap)

				return nil
			},
		}

		s := Service{
			db:              mockDB,
			streamChunkSize: 100,
		}

		user := UserEntity{Name: nullable.String("TestUser")}

		err := s.CreateUser(context.TODO(), user)
		assert.Equal(t, nil, err)
		assert.Equal(t, 1, len(users))

		assert.Equal(t, "TestUser", users[0]["name"])
	})
}

func TestUpdateUserScoreWithKsqlMock(t *testing.T) {
	t.Run("should call ksql.QueryOne() & Patch() correctly", func(t *testing.T) {

		var users []interface{}
		mockDB := ksql.Mock{
			QueryOneFn: func(ctx context.Context, result interface{}, query string, params ...interface{}) error {
				// This function will use reflection to fill the
				// struct fields with the values from the map
				return ksqltest.FillStructWith(result, map[string]interface{}{
					// Use int this map the keys you set on the ksql tags, e.g. `ksql:"score"`
					// Each of these fields represent the database rows returned
					// by the query.
					"score": 42,
				})
			},

			PatchFn: func(ctx context.Context, table ksql.Table, record interface{}) error {
				users = append(users, record)
				return nil
			},
		}

		s := Service{
			db:              mockDB,
			streamChunkSize: 100,
		}

		err := s.UpdateUserScore(context.TODO(), 1, -2)
		assert.Equal(t, nil, err)
		assert.Equal(t, 1, len(users))

		resultUser := UserEntity{
			ID:    1,
			Score: nullable.Int(40),
		}
		assert.Equal(t, &resultUser, users[0])
	})
}

func TestListUsersWithKsqlMock(t *testing.T) {
	t.Run("should call ksql.QueryOne() & Query() correctly", func(t *testing.T) {
		mockDB := ksql.Mock{
			QueryOneFn: func(ctx context.Context, result interface{}, query string, params ...interface{}) error {
				// This function will use reflection to fill the
				// struct fields with the values from the map
				return ksqltest.FillStructWith(result, map[string]interface{}{
					// Use int this map the keys you set on the ksql tags, e.g. `ksql:"score"`
					// Each of these fields represent the database rows returned
					// by the query.
					"count": 420,
				})
			},
			QueryFn: func(ctx context.Context, results interface{}, query string, params ...interface{}) error {
				return ksqltest.FillSliceWith(results, []map[string]interface{}{
					{
						"id":   1,
						"name": "fake name",
						"age":  42,
					},
					{
						"id":   2,
						"name": "another fake name",
						"age":  43,
					},
				})
			},
		}

		s := Service{
			db:              mockDB,
			streamChunkSize: 100,
		}

		total, users, err := s.ListUsers(context.TODO(), 40, 2)
		assert.Equal(t, nil, err)
		assert.Equal(t, 420, total)
		assert.Equal(t, 2, len(users))

		expectedUsers := []UserEntity{
			{
				ID:   1,
				Name: nullable.String("fake name"),
				Age:  nullable.Int(42),
			},
			{
				ID:   2,
				Name: nullable.String("another fake name"),
				Age:  nullable.Int(43),
			},
		}
		assert.Equal(t, expectedUsers, users)
	})
}

func TestStreamAllUsersWithKsqlMock(t *testing.T) {
	t.Run("should call ksql.QueryChunks correctly", func(t *testing.T) {
		mockDB := ksql.Mock{
			QueryChunksFn: func(ctx context.Context, parser ksql.ChunkParser) error {
				// Chunk 1:
				err := ksqltest.CallFunctionWithRows(parser.ForEachChunk, []map[string]interface{}{
					{
						"id":   1,
						"name": "fake name",
						"age":  42,
					},
					{
						"id":   2,
						"name": "another fake name",
						"age":  43,
					},
				})
				if err != nil {
					return err
				}

				// Chunk 2:
				err = ksqltest.CallFunctionWithRows(parser.ForEachChunk, []map[string]interface{}{
					{
						"id":   3,
						"name": "yet another fake name",
						"age":  44,
					},
				})
				return err
			},
		}

		s := Service{
			db:              mockDB,
			streamChunkSize: 2,
		}

		var users []UserEntity
		err := s.StreamAllUsers(context.TODO(), func(u UserEntity) error {
			users = append(users, u)
			return nil
		})

		assert.Equal(t, nil, err)
		assert.Equal(t, 3, len(users))

		expectedUsers := []UserEntity{
			{
				ID:   1,
				Name: nullable.String("fake name"),
				Age:  nullable.Int(42),
			},
			{
				ID:   2,
				Name: nullable.String("another fake name"),
				Age:  nullable.Int(43),
			},
			{
				ID:   3,
				Name: nullable.String("yet another fake name"),
				Age:  nullable.Int(44),
			},
		}
		assert.Equal(t, expectedUsers, users)
	})
}

func TestDeleteUserWithKsqlMock(t *testing.T) {
	t.Run("should call ksql.Delete correctly", func(t *testing.T) {

		var ids []interface{}
		mockDB := ksql.Mock{
			DeleteFn: func(ctx context.Context, table ksql.Table, idOrObj interface{}) error {
				ids = append(ids, idOrObj)
				return nil
			},
		}

		s := Service{
			db:              mockDB,
			streamChunkSize: 100,
		}

		err := s.DeleteUser(context.TODO(), 42)
		assert.Equal(t, nil, err)
		assert.Equal(t, 1, len(ids))
		assert.Equal(t, 42, ids[0])
	})
}
