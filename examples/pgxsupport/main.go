package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgtype"
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

	checkIfUserBelongsToTeams(ctx, db, u.ID, []int{1, 2, 42})
}

func checkIfUserBelongsToTeams(ctx context.Context, db ksql.Provider, userID int, teamIDs []int) {
	// Check if user belongs to either of the input teams:
	var row struct {
		Count pgtype.Int8 `ksql:"c"`
	}
	err := db.QueryOne(ctx, &row,
		`SELECT count(*) as c
		FROM users AS u
		JOIN team_members AS tm
			ON u.id = tm.user_id
		WHERE u.id = $1
			AND tm.team_id = ANY($2)`,
		userID,
		[]int{1, 2, 42}, // Int slices are supported by PGX
	)
	if err != nil {
		log.Fatalf("unexpected error: %s", err)
	}

	fmt.Printf("Count: %+v\n", row.Count.Int)
}
