package main

import (
	"context"
	"fmt"
	"time"

	"github.com/vingarcia/ksql"
	"github.com/vingarcia/ksql/adapters/ksqlite3"
	"github.com/vingarcia/ksql/nullable"
)

type User struct {
	ID   int    `ksql:"id"`
	Name string `ksql:"name"`
	Age  int    `ksql:"age"`

	// The following attributes are making use of the KSQL Modifiers,
	// you can find more about them on our Wiki:
	//
	// - https://github.com/VinGarcia/ksql/wiki/Modifiers
	//

	// The `json` modifier will save the address as JSON in the database
	Address Address `ksql:"address,json"`

	// The timeNowUTC modifier will set this field to `time.Now().UTC()` before saving it:
	UpdatedAt time.Time `ksql:"updated_at,timeNowUTC"`

	// The timeNowUTC/skipUpdates modifier will set this field to `time.Now().UTC()` only
	// when first creating it and ignore it during updates.
	CreatedAt time.Time `ksql:"created_at,timeNowUTC/skipUpdates"`
}

type PartialUpdateUser struct {
	ID      int      `ksql:"id"`
	Name    *string  `ksql:"name"`
	Age     *int     `ksql:"age"`
	Address *Address `ksql:"address,json"`
}

type Address struct {
	State string `json:"state"`
	City  string `json:"city"`
}

// UsersTable informs KSQL the name of the table and that it can
// use the default value for the primary key column name: "id"
var UsersTable = ksql.NewTable("users")

func main() {
	ctx := context.Background()

	// The available adapters are:
	// - kpgx.New(ctx, connURL, ksql.Config{})
	// - kmysql.New(ctx, connURL, ksql.Config{})
	// - ksqlserver.New(ctx, connURL, ksql.Config{})
	// - ksqlite3.New(ctx, connURL, ksql.Config{})
	//
	// For more detailed examples see:
	// - `./examples/all_adapters/all_adapters.go`
	//
	// In this example we'll use sqlite3:
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

	var alison = User{
		Name: "Alison",
		Age:  22,
		Address: Address{
			State: "MG",
		},
	}
	err = db.Insert(ctx, UsersTable, &alison)
	if err != nil {
		panic(err.Error())
	}
	fmt.Println("Alison ID:", alison.ID)

	// Inserting inline:
	err = db.Insert(ctx, UsersTable, &User{
		Name: "Cristina",
		Age:  27,
		Address: Address{
			State: "SP",
		},
	})
	if err != nil {
		panic(err.Error())
	}

	// Deleting Alison:
	err = db.Delete(ctx, UsersTable, alison.ID)
	if err != nil {
		panic(err.Error())
	}

	// Retrieving Cristina, note that if you omit the SELECT part of the query
	// KSQL will build it for you (efficiently) based on the fields from the struct:
	var cris User
	err = db.QueryOne(ctx, &cris, "FROM users WHERE name = ? ORDER BY id", "Cristina")
	if err != nil {
		panic(err.Error())
	}
	fmt.Printf("Cristina: %#v\n", cris)

	// Updating all fields from Cristina:
	cris.Name = "Cris"
	err = db.Patch(ctx, UsersTable, cris)

	// Changing the age of Cristina but not touching any other fields:

	// Partial update technique 1:
	err = db.Patch(ctx, UsersTable, struct {
		ID  int `ksql:"id"`
		Age int `ksql:"age"`
	}{ID: cris.ID, Age: 28})
	if err != nil {
		panic(err.Error())
	}

	// Partial update technique 2:
	err = db.Patch(ctx, UsersTable, PartialUpdateUser{
		ID:  cris.ID,
		Age: nullable.Int(28), // (just a pointer to an int, if null it won't be updated)
	})
	if err != nil {
		panic(err.Error())
	}

	// Listing first 10 users from the database
	// (each time you run this example a new Cristina is created)
	//
	// Note: Using this function it is recommended to set a LIMIT, since
	// not doing so can load too many users on your computer's memory or
	// cause an Out Of Memory Kill.
	//
	// If you need to query very big numbers of users we recommend using
	// the `QueryChunks` function.
	var users []User
	err = db.Query(ctx, &users, "FROM users LIMIT 10")
	if err != nil {
		panic(err.Error())
	}

	fmt.Printf("Users: %#v\n", users)

	// Making transactions:
	err = db.Transaction(ctx, func(db ksql.Provider) error {
		var cris2 User
		err = db.QueryOne(ctx, &cris2, "FROM users WHERE id = ?", cris.ID)
		if err != nil {
			// This will cause an automatic rollback:
			return err
		}

		err = db.Patch(ctx, UsersTable, PartialUpdateUser{
			ID:  cris2.ID,
			Age: nullable.Int(29),
		})
		if err != nil {
			// This will also cause an automatic rollback and then panic again
			// so that we don't hide the panic inside the KSQL library
			panic(err.Error())
		}

		// Commits the transaction
		return nil
	})
	if err != nil {
		panic(err.Error())
	}
}
