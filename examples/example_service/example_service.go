package exampleservice

import (
	"context"
	"time"

	"github.com/vingarcia/kissorm"
	"github.com/vingarcia/kissorm/nullable"
)

// Service ...
type Service struct {
	usersTable      kissorm.ORMProvider
	streamChunkSize int
}

// UserEntity represents a domain user,
// the pointer fields represent optional fields that
// might not be present in some requests.
//
// Its recommended that this struct contains
// one field for each database column,
// so you can write generic queries like `SELECT * FROM users`.
//
// If this is not the case, it might be a good idea
// to create a DTO struct to receive select queries.
type UserEntity struct {
	ID          int       `kissorm:"id"`
	Name        *string   `kissorm:"name"`
	Age         *int      `kissorm:"age"`
	Score       *int      `kissorm:"score"`
	LastPayment time.Time `kissorm:"last_payment"`
}

// NewUserService ...
func NewUserService(usersTable kissorm.ORMProvider) Service {
	return Service{
		usersTable:      usersTable,
		streamChunkSize: 100,
	}
}

// CreateUser ...
func (s Service) CreateUser(ctx context.Context, u UserEntity) error {
	return s.usersTable.Insert(ctx, &u)
}

// UpdateUserScore update the user score adding scoreChange with the current
// user score. Defaults to 0 if not set.
func (s Service) UpdateUserScore(ctx context.Context, uID int, scoreChange int) error {
	var scoreRow struct {
		Score int `kissorm:"score"`
	}
	err := s.usersTable.QueryOne(ctx, &scoreRow, "SELECT score FROM users WHERE id = ?", uID)
	if err != nil {
		return err
	}

	return s.usersTable.Update(ctx, &UserEntity{
		ID:    uID,
		Score: nullable.Int(scoreRow.Score + scoreChange),
	})
}

// ListUsers returns a page of users
func (s Service) ListUsers(ctx context.Context, offset, limit int) (total int, users []UserEntity, err error) {
	var countRow struct {
		Count int `kissorm:"count"`
	}
	err = s.usersTable.QueryOne(ctx, &countRow, "SELECT count(*) as count FROM users")
	if err != nil {
		return 0, nil, err
	}

	return countRow.Count, users, s.usersTable.Query(ctx, &users, "SELECT * FROM users OFFSET ? LIMIT ?", offset, limit)
}

// StreamAllUsers sends all users from the database to an external client
//
// Note: This method is unusual, but so are the use-cases for the QueryChunks function.
// In most cases you should just use the Query or QueryOne functions and use the QueryChunks
// function only when the ammount of data loaded might exceed the available memory and/or
// when you can't put an upper limit on the number of values returned.
func (s Service) StreamAllUsers(ctx context.Context, sendUser func(u UserEntity) error) error {
	return s.usersTable.QueryChunks(ctx, kissorm.ChunkParser{
		Query:     "SELECT * FROM users",
		Params:    []interface{}{},
		ChunkSize: s.streamChunkSize,
		ForEachChunk: func(users []UserEntity) error {
			for _, user := range users {
				err := sendUser(user)
				if err != nil {
					// This will abort the QueryChunks loop and return this error
					return err
				}
			}
			return nil
		},
	})
}

// DeleteUser deletes a user by its ID
func (s Service) DeleteUser(ctx context.Context, uID int) error {
	return s.usersTable.Delete(ctx, uID)
}
