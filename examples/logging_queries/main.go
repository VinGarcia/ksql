package main

import (
	"context"

	"github.com/vingarcia/ksql"
	"github.com/vingarcia/ksql/adapters/ksqlite3"
)

type User struct {
	ID   int    `ksql:"id"`
	Name string `ksql:"name"`
	Age  int    `ksql:"age"`
}

// UsersTable informs KSQL the name of the table and that it can
// use the default value for the primary key column name: "id"
var UsersTable = ksql.NewTable("users")

func main() {
	ctx := context.Background()

	db, err := ksqlite3.New(ctx, "/tmp/hello.sqlite", ksql.Config{
		MaxOpenConns: 1,
	})
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	// After we inject a logger, all subsequent queries
	// will use this logger.
	//
	// You can also inject the ksql.ErrorLogger if you only
	// care about these logs when a query error happens.
	ctx = ksql.InjectLogger(ctx, ksql.Logger)

	// This logs: {"query":"CREATE TABLE IF NOT EXISTS users (\n\t  id INTEGER PRIMARY KEY,\n\t\tage INTEGER,\n\t\tname TEXT\n\t)","params":null}
	_, err = db.Exec(ctx, `CREATE TABLE IF NOT EXISTS users (
	  id INTEGER PRIMARY KEY,
		age INTEGER,
		name TEXT
	)`)
	if err != nil {
		panic(err.Error())
	}

	// This logs: {"query":"INSERT INTO `users` (`name`, `age`) VALUES (?, ?)","params":["Alison",22]}
	var alison = User{
		Name: "Alison",
		Age:  22,
	}
	err = db.Insert(ctx, UsersTable, &alison)
	if err != nil {
		panic(err.Error())
	}

	// This logs: {"query":"SELECT `id`, `name`, `age` FROM users LIMIT 10","params":null}
	var users []User
	err = db.Query(ctx, &users, "FROM users LIMIT 10")
	if err != nil {
		panic(err.Error())
	}

	// This logs: {"query":"SELECT `id`, `name`, `age` FROM users WHERE age < ?","params":[42]}
	err = db.QueryChunks(ctx, ksql.ChunkParser{
		Query:     "FROM users WHERE age < ?",
		Params:    []interface{}{42},
		ChunkSize: 100,
		ForEachChunk: func(chunk []User) error {
			// Do nothing, since this is just an example
			return nil
		},
	})
	if err != nil {
		panic(err.Error())
	}

	// This logs: {"query":"SELECT `id`, `name`, `age` FROM users WHERE name = ?","params":["Alison"]}
	err = db.QueryOne(ctx, &alison, "FROM users WHERE name = ?", "Alison")
	if err != nil {
		panic(err.Error())
	}

	// This logs: {"query":"UPDATE `users` SET `name` = ?, `age` = ? WHERE `id` = ?","params":["Alison",23,1]}
	alison.Age++
	err = db.Patch(ctx, UsersTable, alison)
	if err != nil {
		panic(err.Error())
	}

	// This logs: {"query":"DELETE FROM `users` WHERE `id` = ?","params":[1]}
	err = db.Delete(ctx, UsersTable, alison.ID)
	if err != nil {
		panic(err.Error())
	}

	// Here we are provoking an error, so we can see an error on the log:
	_ = db.QueryOne(ctx, &alison, "not a valid query", "someFakeParams")
	// This logs: {"query":"not a valid query","params":["someFakeParams"],"error":"error running query: near \"not\": syntax error"}
}
