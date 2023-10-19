package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/vingarcia/ksql"
	"github.com/vingarcia/ksql/adapters/ksqlite3"
)

var UsersTable = ksql.NewTable("users")

type User struct {
	ID   int    `ksql:"id"`
	Name string `ksql:"name"`
	Age  int    `ksql:"age"`

	UpdatedAt time.Time `ksql:"updated_at,timeNowUTC"`
	CreatedAt time.Time `ksql:"created_at,timeNowUTC/skipUpdates"`
}

func main() {
	ctx := context.Background()

	db, err := ksqlite3.New(ctx, "/tmp/hello.sqlite", ksql.Config{
		MaxOpenConns: 1,
	})
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	// In the definition below, please note that BLOB is
	// the only type we can use in sqlite for storing JSON.
	_, err = db.Exec(ctx, `CREATE TABLE IF NOT EXISTS users (
	  id INTEGER PRIMARY KEY,
		age INTEGER,
		name TEXT,
		address BLOB,
		created_at DATETIME,
		updated_at DATETIME
	)`)
	if err != nil {
		panic(err.Error())
	}

	db.Exec(ctx, `DELETE FROM users`)

	err = insertManyUsers(ctx, db, []User{
		{Name: "User1", Age: 22},
		{Name: "User2", Age: 32},
		{Name: "User3", Age: 42},
	})
	if err != nil {
		panic(err.Error())
	}

	var users []User
	err = db.Query(ctx, &users, `FROM users WHERE name like 'User%'`)
	if err != nil {
		panic(err.Error())
	}

	b, err := json.MarshalIndent(users, "", "  ")
	if err != nil {
		panic(err.Error())
	}

	fmt.Println("users:", string(b))
}

func insertManyUsers(ctx context.Context, db ksql.Provider, users []User) error {
	if len(users) == 0 {
		return nil
	}

	query := []string{}
	values := []interface{}{}

	for i, user := range users {
		query = append(query, fmt.Sprintf(`($%d, $%d, $%d, $%d)`, i*5+1, i*5+2, i*5+3, i*5+4))
		values = append(values, user.Age, user.Name, time.Now().UTC(), time.Now().UTC())
	}

	_, err := db.Exec(ctx, `INSERT INTO users (age, name, created_at, updated_at) VALUES `+strings.Join(query, ", "), values...)
	return err
}
