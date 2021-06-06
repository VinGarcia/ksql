package exampleservice

import (
	"context"
	"testing"

	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/tj/assert"
	"github.com/vingarcia/ksql"
	"github.com/vingarcia/ksql/nullable"
	"github.com/vingarcia/ksql/structs"
)

func TestCreateUser(t *testing.T) {
	t.Run("should call ksql.Insert correctly", func(t *testing.T) {
		controller := gomock.NewController(t)
		defer controller.Finish()

		mockDB := NewMockSQLProvider(controller)

		s := Service{
			db:              mockDB,
			streamChunkSize: 100,
		}

		var users []interface{}
		mockDB.EXPECT().Insert(gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, table ksql.Table, records ...interface{}) error {
				users = append(users, records...)
				return nil
			})

		user := UserEntity{Name: nullable.String("TestUser")}

		err := s.CreateUser(context.TODO(), user)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(users))
		assert.Equal(t, &user, users[0])
	})

	t.Run("another way of testing input structs", func(t *testing.T) {
		controller := gomock.NewController(t)
		defer controller.Finish()

		mockDB := NewMockSQLProvider(controller)

		s := Service{
			db:              mockDB,
			streamChunkSize: 100,
		}

		var users []map[string]interface{}
		mockDB.EXPECT().Insert(gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, table ksql.Table, records ...interface{}) error {
				for _, record := range records {
					// The StructToMap function will convert a struct with `ksql` tags
					// into a map using the ksql attr names as keys.
					//
					// If you are inserting an anonymous struct (not usual) this function
					// can make your tests shorter:
					uMap, err := structs.StructToMap(record)
					if err != nil {
						return err
					}
					users = append(users, uMap)
				}
				return nil
			})

		user := UserEntity{Name: nullable.String("TestUser")}

		err := s.CreateUser(context.TODO(), user)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(users))

		assert.Equal(t, "TestUser", users[0]["name"])
	})
}

func TestUpdateUserScore(t *testing.T) {
	t.Run("should call ksql.QueryOne() & Update() correctly", func(t *testing.T) {
		controller := gomock.NewController(t)
		defer controller.Finish()

		mockDB := NewMockSQLProvider(controller)

		s := Service{
			db:              mockDB,
			streamChunkSize: 100,
		}

		var users []interface{}
		gomock.InOrder(
			mockDB.EXPECT().QueryOne(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(ctx context.Context, result interface{}, query string, params ...interface{}) error {
					// This function will use reflection to fill the
					// struct fields with the values from the map
					return structs.FillStructWith(result, map[string]interface{}{
						// Use int this map the keys you set on the ksql tags, e.g. `ksql:"score"`
						// Each of these fields represent the database rows returned
						// by the query.
						"score": 42,
					})
				}),
			mockDB.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(ctx context.Context, table ksql.Table, records ...interface{}) error {
					users = append(users, records...)
					return nil
				}),
		)

		err := s.UpdateUserScore(context.TODO(), 1, -2)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(users))

		resultUser := UserEntity{
			ID:    1,
			Score: nullable.Int(40),
		}
		assert.Equal(t, &resultUser, users[0])
	})
}

func TestListUsers(t *testing.T) {
	t.Run("should call ksql.QueryOne() & Query() correctly", func(t *testing.T) {
		controller := gomock.NewController(t)
		defer controller.Finish()

		mockDB := NewMockSQLProvider(controller)

		s := Service{
			db:              mockDB,
			streamChunkSize: 100,
		}

		gomock.InOrder(
			mockDB.EXPECT().QueryOne(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(ctx context.Context, result interface{}, query string, params ...interface{}) error {
					// This function will use reflection to fill the
					// struct fields with the values from the map
					return structs.FillStructWith(result, map[string]interface{}{
						// Use int this map the keys you set on the ksql tags, e.g. `ksql:"score"`
						// Each of these fields represent the database rows returned
						// by the query.
						"count": 420,
					})
				}),
			mockDB.EXPECT().Query(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(ctx context.Context, results interface{}, query string, params ...interface{}) error {
					return structs.FillSliceWith(results, []map[string]interface{}{
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
				}),
		)

		total, users, err := s.ListUsers(context.TODO(), 40, 2)
		assert.Nil(t, err)
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

func TestStreamAllUsers(t *testing.T) {
	t.Run("should call ksql.QueryChunks correctly", func(t *testing.T) {
		controller := gomock.NewController(t)
		defer controller.Finish()

		mockDB := NewMockSQLProvider(controller)

		s := Service{
			db:              mockDB,
			streamChunkSize: 2,
		}

		mockDB.EXPECT().QueryChunks(gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, parser ksql.ChunkParser) error {
				fn, ok := parser.ForEachChunk.(func(users []UserEntity) error)
				require.True(t, ok)
				// Chunk 1:
				err := fn([]UserEntity{
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
				})
				if err != nil {
					return err
				}

				// Chunk 2:
				err = fn([]UserEntity{
					{
						ID:   3,
						Name: nullable.String("yet another fake name"),
						Age:  nullable.Int(44),
					},
				})
				return err
			})

		var users []UserEntity
		err := s.StreamAllUsers(context.TODO(), func(u UserEntity) error {
			users = append(users, u)
			return nil
		})

		assert.Nil(t, err)
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

func TestDeleteUser(t *testing.T) {
	t.Run("should call ksql.Delete correctly", func(t *testing.T) {
		controller := gomock.NewController(t)
		defer controller.Finish()

		mockDB := NewMockSQLProvider(controller)

		s := Service{
			db:              mockDB,
			streamChunkSize: 100,
		}

		var ids []interface{}
		mockDB.EXPECT().Delete(gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, table ksql.Table, idArgs ...interface{}) error {
				ids = append(ids, idArgs...)
				return nil
			})

		err := s.DeleteUser(context.TODO(), 42)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(ids))
		assert.Equal(t, 42, ids[0])
	})
}
