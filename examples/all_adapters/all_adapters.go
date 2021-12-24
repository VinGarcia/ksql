package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/vingarcia/ksql"
	"github.com/vingarcia/ksql/adapters/kmysql"
	"github.com/vingarcia/ksql/adapters/kpgx"
	"github.com/vingarcia/ksql/adapters/ksqlite3"
	"github.com/vingarcia/ksql/adapters/ksqlserver"
)

// User ...
type User struct {
	ID   int    `ksql:"id"`
	Name string `ksql:"name"`
	Age  int    `ksql:"age"`

	// This field will be saved as JSON in the database
	Address Address `ksql:"address,json"`
}

// Address ...
type Address struct {
	State string `json:"state"`
	City  string `json:"city"`
}

// UsersTable informs ksql the name of the table and that it can
// use the default value for the primary key column name: "id"
var UsersTable = ksql.NewTable("users")

func main() {
	ctx := context.Background()

	var db ksql.Provider
	var err error

	// This switch lists all the supported adapters, and how to use them:
	switch os.Getenv("KSQL_ADAPTER") {
	case "postgres", "pgx":
		db, err = kpgx.New(ctx, "host=localhost port=5432 user=postgres password=postgres dbname=ksql sslmode=disable", ksql.Config{
			MaxOpenConns: 1,
		})
		if err != nil {
			log.Fatalf("unable to open database, reason: %s", err)
		}
		_, err = db.Exec(ctx, `CREATE TABLE users (
		  id serial PRIMARY KEY,
			age INT,
			name VARCHAR(50),
			address jsonb
		)`)
		if err != nil {
			log.Fatalf("unable to create users table: %s", err)
		}
	case "mysql":
		db, err = kmysql.New(ctx, "root:mysql@(127.0.0.1:3306)/ksql?timeout=30s", ksql.Config{
			MaxOpenConns: 1,
		})
		if err != nil {
			log.Fatalf("unable to open database: %s", err)
		}
		_, err = db.Exec(ctx, `CREATE TABLE users (
			id INT AUTO_INCREMENT PRIMARY KEY,
			age INT,
			name VARCHAR(50),
			address JSON
		)`)
		if err != nil {
			log.Fatalf("unable to create users table: %s", err)
		}
	case "sqlserver":
		db, err = ksqlserver.New(ctx, "sqlserver://sa:Sqls3rv3r@127.0.0.1:1433?databaseName=ksql", ksql.Config{
			MaxOpenConns: 1,
		})
		if err != nil {
			log.Fatalf("unable to open database: %s", err)
		}

		// In the example below NVARCHAR is the format
		// we are using for storing JSON:
		_, err = db.Exec(ctx, `CREATE TABLE users (
			id INT IDENTITY(1,1) PRIMARY KEY,
			age INT,
			name VARCHAR(50),
			address NVARCHAR(4000)
		)`)
		if err != nil {
			log.Fatalf("unable to create users table: %s", err)
		}
	case "", "sqlite3":
		db, err = ksqlite3.New(ctx, "/tmp/ksql.sqlite", ksql.Config{
			MaxOpenConns: 1,
		})
		if err != nil {
			log.Fatalf("unable to open database: %s", err)
		}

		// In the definition below, please note that BLOB is
		// the only type we can use in sqlite for storing JSON.
		_, err = db.Exec(ctx, `CREATE TABLE IF NOT EXISTS users (
			id INTEGER PRIMARY KEY,
			age INTEGER,
			name TEXT,
			address BLOB
		)`)
		if err != nil {
			log.Fatalf("unable to create users table: %s", err)
		}
	}

	insertUser := &User{
		Name: "Some Username",
		Age:  22,
		Address: Address{
			State: "MG",
		},
	}
	err = db.Insert(ctx, UsersTable, &insertUser)
	if err != nil {
		log.Fatalf("error inserting user: %s", err)
	}

	var user User
	err = db.QueryOne(ctx, &user, `SELECT name FROM users WHERE name = 'Some Username'`)
	if err != nil {
		log.Fatalf("error querying for user: %s", err)
	}

	// Loaded from the database using LastInsertID or equivalent:
	fmt.Printf("insertedUserID: %d\n", insertUser.ID)
	fmt.Printf("ID loaded with QueryOne: %d\n", user.ID)
}
