package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/vingarcia/ksql"
	"github.com/vingarcia/ksql/adapters/kpgx"
)

type User struct {
	ID   int    `ksql:"id"`
	Name string `ksql:"name"`
}

type TeamMember struct {
	TeamID int `ksql:"team_id"`
	UserID int `ksql:"user_id"`
}

var UsersTable = ksql.NewTable("users", "id")

var TeamMembersTable = ksql.NewTable("team_members", "user_id", "team_id")

func main() {
	ctx := context.Background()

	db, err := kpgx.New(ctx, os.Getenv("PG_URL"), ksql.Config{})
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	db.Exec(ctx, `DROP TABLE team_members`)
	_, err = db.Exec(ctx, `CREATE TABLE IF NOT EXISTS team_members (
	  user_id INTEGER,
	  team_id INTEGER,
		PRIMARY KEY (user_id, team_id)
	)`)
	if err != nil {
		panic(err.Error())
	}

	db.Exec(ctx, `DROP TABLE users`)
	_, err = db.Exec(ctx, `CREATE TABLE IF NOT EXISTS users (
	  id serial PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		panic(err.Error())
	}

	u := User{
		Name: "ExampleUser1",
	}
	err = db.Insert(ctx, UsersTable, &u)
	if err != nil {
		panic(err.Error())
	}

	err = db.Insert(ctx, TeamMembersTable, &TeamMember{
		UserID: u.ID,
		TeamID: 42,
	})
	if err != nil {
		panic(err.Error())
	}

	err = db.Insert(ctx, TeamMembersTable, &TeamMember{
		UserID: u.ID,
		TeamID: 43,
	})
	if err != nil {
		panic(err.Error())
	}

	err = db.Insert(ctx, TeamMembersTable, &TeamMember{
		UserID: u.ID,
		TeamID: 44,
	})
	if err != nil {
		panic(err.Error())
	}

	userID := u.ID

	// The example below illustrates how it is possible to use pgx
	// special support to slices Inside a query:

	// Find user iff user belongs to either team on the input list:
	var user User
	err = db.QueryOne(ctx, &user,
		`SELECT u.*
		FROM users AS u
		JOIN team_members AS tm
			ON u.id = tm.user_id
		WHERE u.id = $1
			AND tm.team_id = ANY($2)`,
		userID,
		[]int{1, 2, 42},
	)
	if err == ksql.ErrRecordNotFound {
		fmt.Println("Input user does not exist or does not belong to any of the provided teams")
		return
	} else if err != nil {
		log.Fatalf("unexpected error: %s", err)
	}

	fmt.Printf("Found user: %+v\n", user)
}
