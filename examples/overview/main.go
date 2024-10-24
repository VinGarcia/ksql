package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/vingarcia/ksql"
	"github.com/vingarcia/ksql/adapters/kpgx"
)

var UsersTable = ksql.NewTable("users", "user_id")

type User struct {
	ID    int    `ksql:"user_id"`
	Name  string `ksql:"name"`
	Type  string `ksql:"type"`
	Posts []Post
}

// Post have a many to one relationship with User
var PostsTable = ksql.NewTable("posts", "post_id")

type Post struct {
	ID     int    `ksql:"post_id"`
	UserID int    `ksql:"user_id"`
	Title  string `ksql:"title"`
	Text   string `ksql:"text"`
}

// Address have a one to one relationship with User
//
// Note that we are omitting the name of the ID column
// below because "id" is already the default:
var AddressesTable = ksql.NewTable("addresses")

type Address struct {
	ID       int    `ksql:"id"`
	UserID   int    `ksql:"user_id"`
	FullAddr string `ksql:"full_addr"`
}

func main() {
	ctx := context.Background()
	dbURL, closeDB := startPostgresDB(ctx)
	defer closeDB()

	db, err := kpgx.New(ctx, dbURL, ksql.Config{})
	if err != nil {
		log.Fatalf("unable connect to database: %s", err)
	}
	defer db.Close()

	err = createTablesAndRecords(ctx, db)
	if err != nil {
		log.Fatalf("error creating tables: %s", err)
	}

	// For querying only some attributes you can
	// create a custom struct like this:
	var count []struct {
		Count int    `ksql:"count"`
		Type  string `ksql:"type"`
	}
	err = db.Query(ctx, &count, "SELECT type, count(*) as count FROM users GROUP BY type")
	if err != nil {
		log.Fatalf("unable to query users: %s", err)
	}

	fmt.Println("number of users by type:", count)

	// For loading entities from the database KSQL can build
	// the SELECT part of the query for you if you omit it like this:
	var adminUsers []User
	err = db.Query(ctx, &adminUsers, "FROM users WHERE type = $1", "admin")
	if err != nil {
		log.Fatalf("unable to query admin users: %s", err)
	}

	fmt.Println("admin users:", adminUsers)

	// A nice way of loading the posts of a user might be like this:
	var user User
	err = errors.Join(
		db.QueryOne(ctx, &user, "FROM users WHERE user_id = $1", 42),
		db.Query(ctx, &user.Posts, "FROM posts WHERE user_id = $1", user.ID),
	)
	if err != nil {
		log.Fatalf("unable to query users: %s", err)
	}

	fmt.Println("user with posts:", user)

	// You can retrieve data from joined tables like this
	// (notice you can either use the name of the table or the alias you choose for it in the query):
	var rows []struct {
		OneUser    User    `tablename:"users"`
		OneAddress Address `tablename:"addr"`
	}
	err = db.Query(ctx, &rows,
		`FROM users
		JOIN addresses addr
		  ON users.user_id = addr.user_id`,
	)
	if err != nil {
		log.Fatalf("unable to query users: %s", err)
	}

	fmt.Println("rows of joined tables:", rows)
}
