package repo

import (
	"context"
	"fmt"
	"time"

	"github.com/vingarcia/ksql"
)

var usersTable = ksql.NewTable("users", "id")

// This function doesn't care if db is a transaction or not:
func GetUser(ctx context.Context, db ksql.Provider, userId int) (User, error) {
	var user User
	err := db.QueryOne(ctx, &user, "FROM users WHERE id = ?", userId)
	return user, err
}

// This function doesn't care if db is a transaction or not:
func GetUserByEmail(ctx context.Context, db ksql.Provider, email string) (User, error) {
	var user User
	err := db.QueryOne(ctx, &user, "FROM users WHERE email = ?", email)
	return user, err
}

// This function doesn't care if db is a transaction or not:
func CreateUser(ctx context.Context, db ksql.Provider, user User) error {
	user.CreatedAt = time.Now()
	return db.Insert(ctx, usersTable, &user)
}

// This function doesn't care if db is a transaction or not:
func UpdateUser(ctx context.Context, db ksql.Provider, user User) error {
	return db.Patch(ctx, usersTable, &user)
}

// This function creates a transaction from the input db, if db was already a transaction
// this operation will just keep working in the same transaction instead of creating a new one.
func ChangeUserEmail(ctx context.Context, db ksql.Provider, userID int, newEmail string) error {
	return db.Transaction(ctx, func(db ksql.Provider) error {
		user, err := GetUser(ctx, db, userID)
		if err != nil {
			return err
		}

		// If there is nothing to do, just return:
		if user.Email == newEmail {
			return nil
		}

		_, err = GetUserByEmail(ctx, db, newEmail)
		if err != ksql.ErrRecordNotFound {
			return fmt.Errorf("can't change user email to '%s': this email is already used by other user", newEmail)
		}
		if err != nil {
			return err
		}

		user.Email = newEmail
		return UpdateUser(ctx, db, user)
	})
}
