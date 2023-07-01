[![CI](https://github.com/VinGarcia/ksql/actions/workflows/ci.yml/badge.svg)](https://github.com/VinGarcia/ksql/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/VinGarcia/ksql/branch/master/graph/badge.svg?token=5CNJ867C66)](https://codecov.io/gh/VinGarcia/ksql)
[![Go Reference](https://pkg.go.dev/badge/github.com/vingarcia/ksql.svg)](https://pkg.go.dev/github.com/vingarcia/ksql)
![Go Report Card](https://goreportcard.com/badge/github.com/vingarcia/ksql)

# KSQL the Keep it Simple SQL library

KSQL was created to offer an actually simple and satisfactory
tool for interacting with SQL Databases in Golang.

The core goal of KSQL is not to offer new features that
are unavailable on other libraries (although we do have some),
but to offer a well-thought and well-planned API so that users
have an easier time, learning, debugging, and avoiding common pitfalls.

KSQL is also decoupled from its backend so that
the actual communication with the database is performed by
well-known and trusted technologies, namely: `pgx` and `database/sql`.
You can even create your own backend adapter for KSQL which is
useful in some situations.

In this README you will find examples for "Getting Started" with the library,
for more advanced use-cases [please read our Wiki](https://github.com/VinGarcia/ksql/wiki).

## Outstanding Features

- Every operation returns errors a single time, so its easier to handle them
- Helper functions for everyday operations, namely: Insert, Patch and Delete
- Generic and powerful functions for Querying and Scanning data into structs
- Works on top of existing battle-tested libraries such as `database/sql` and `pgx`
- Supports `sql.Scanner` and `sql.Valuer` and also all `pgx` special types (when using `kpgx`)
- And many other features designed to make your life easier

## Let's start with some Code:

This short example below is a TLDR version to illustrate how easy it is to use KSQL.

You will find more complete examples in the sections below.

```golang
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/vingarcia/ksql"
	"github.com/vingarcia/ksql/adapters/kpgx"
)

var UsersTable = ksql.NewTable("users", "user_id")

type User struct {
	ID   int    `ksql:"user_id"`
	Name string `ksql:"name"`
	Type string `ksql:"type"`
}

func main() {
	ctx := context.Background()
	db, err := kpgx.New(ctx, os.Getenv("POSTGRES_URL"), ksql.Config{})
	if err != nil {
		log.Fatalf("unable connect to database: %s", err)
	}
	defer db.Close()

	// For querying only some attributes you can
	// create a custom struct like this:
	var count []struct {
		Count string `ksql:"count"`
		Type string `ksql:"type"`
	}
	err = db.Query(ctx, &count, "SELECT type, count(*) as count FROM users WHERE type = $1 GROUP BY type", "admin")
	if err != nil {
		log.Fatalf("unable to query users: %s", err)
	}

	fmt.Println("number of users by type:", count)

	// For loading entities from the database KSQL can build
	// the SELECT part of the query for you if you omit it like this:
	var users []User
	err = db.Query(ctx, &users, "FROM users WHERE type = $1", "admin")
	if err != nil {
		log.Fatalf("unable to query users: %s", err)
	}

	fmt.Println("users:", users)
}
```

We currently have 4 constructors available,
one of them is illustrated above (`kpgx.New()`),
the other ones have the exact same signature
but work on different databases, they are:

- `kpgx.New(ctx, os.Getenv("DATABASE_URL"), ksql.Config{})` for Postgres, it works on top of `pgxpool`
- `kmysql.New(ctx, os.Getenv("DATABASE_URL"), ksql.Config{})` for MySQL, it works on top of `database/sql`
- `ksqlserver.New(ctx, os.Getenv("DATABASE_URL"), ksql.Config{})` for SQLServer, it works on top of `database/sql`
- `ksqlite3.New(ctx, os.Getenv("DATABASE_URL"), ksql.Config{})` for SQLite3, it works on top of `database/sql`

## The KSQL Interface

The current interface contains the methods the users are expected to use,
and it is also used for making it easy to mock the whole library if needed.

This interface is declared in the project as `ksql.Provider` and is displayed below.

We plan on keeping it very simple with a small number
of well-thought functions that cover all use cases,
so don't expect many additions:

```go
// Provider describes the KSQL public behavior
//
// The Insert, Patch, Delete and QueryOne functions return `ksql.ErrRecordNotFound`
// if no record was found or no rows were changed during the operation.
type Provider interface {
	Insert(ctx context.Context, table Table, record interface{}) error
	Patch(ctx context.Context, table Table, record interface{}) error
	Delete(ctx context.Context, table Table, idOrRecord interface{}) error

	Query(ctx context.Context, records interface{}, query string, params ...interface{}) error
	QueryOne(ctx context.Context, record interface{}, query string, params ...interface{}) error
	QueryChunks(ctx context.Context, parser ChunkParser) error

	Exec(ctx context.Context, query string, params ...interface{}) (Result, error)
	Transaction(ctx context.Context, fn func(Provider) error) error
}
```

## Using KSQL

In the example below we'll cover all the most common use cases such as:

1. Inserting records
2. Updating records
3. Deleting records
4. Querying one or many records
5. Making transactions

More advanced use cases are illustrated on their own pages on [our Wiki](https://github.com/VinGarcia/ksql/wiki):

- [Querying in Chunks for Big Queries](https://github.com/VinGarcia/ksql/wiki/Querying-in-Chunks-for-Big-Queries)
- [Avoiding Code Duplication with the Select Builder](https://github.com/VinGarcia/ksql/wiki/Avoiding-Code-Duplication-with-the-Select-Builder)
- [Reusing Existing Structs on Queries with JOINs](https://github.com/VinGarcia/ksql/wiki/Reusing-Existing-Structs-on-Queries-with-JOINs)
- [Testing Tools and `ksql.Mock`](https://github.com/VinGarcia/ksql/wiki/Testing-Tools-and-ksql.Mock)

For the more common use cases please read the example below,
which is also available [here](./examples/crud/crud.go)
if you want to compile it yourself.

```Go
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
```

## Benchmark Comparison

The results of the benchmark are good:
they show that KSQL is in practical terms,
as fast as `sqlx` which was our goal from the start.

To understand the benchmark below you must know
that all tests are performed using Postgres 12.1 and
that we are comparing the following tools:

- KSQL using the adapter that wraps `database/sql`
- KSQL using the adapter that wraps `pgx`
- `database/sql`
- `sqlx`
- `pgx` (with `pgxpool`)
- `gorm`
- `sqlc`
- `sqlboiler`

For each of these tools, we are running 3 different queries:

The `insert-one` query looks like this:

`INSERT INTO users (name, age) VALUES ($1, $2) RETURNING id`

The `single-row` query looks like this:

`SELECT id, name, age FROM users OFFSET $1 LIMIT 1`

The `multiple-rows` query looks like this:

`SELECT id, name, age FROM users OFFSET $1 LIMIT 10`

Keep in mind that some of the tools tested (like GORM) actually build
the queries internally so the actual code used for the benchmark
might differ a little bit from the example ones above.

Without further ado, here are the results:

```bash
$ make bench TIME=5s
sqlc generate
go test -bench=. -benchtime=5s
goos: linux
goarch: amd64
pkg: github.com/vingarcia/ksql/benchmarks
cpu: Intel(R) Core(TM) i7-10750H CPU @ 2.60GHz
BenchmarkInsert/ksql/sql-adapter/insert-one-12         	    9373	    658434 ns/op
BenchmarkInsert/ksql/pgx-adapter/insert-one-12         	   10000	    571623 ns/op
BenchmarkInsert/sql/insert-one-12                      	    9423	    627719 ns/op
BenchmarkInsert/sql/prep-stmt/insert-one-12            	   10000	    559441 ns/op
BenchmarkInsert/sqlx/insert-one-12                     	    9651	    637823 ns/op
BenchmarkInsert/sqlx/prep-stmt/insert-one-12           	   10000	    574260 ns/op
BenchmarkInsert/pgxpool/insert-one-12                  	   10000	    568112 ns/op
BenchmarkInsert/gorm/insert-one-12                     	    8504	    696791 ns/op
BenchmarkInsert/sqlc/insert-one-12                     	    9504	    662484 ns/op
BenchmarkInsert/sqlc/prep-stmt/insert-one-12           	   10000	    568876 ns/op
BenchmarkInsert/sqlboiler/insert-one-12                	    9298	    667913 ns/op
BenchmarkQuery/ksql/sql-adapter/single-row-12          	   39939	    150412 ns/op
BenchmarkQuery/ksql/sql-adapter/multiple-rows-12       	   35901	    156905 ns/op
BenchmarkQuery/ksql/pgx-adapter/single-row-12          	   83677	     72461 ns/op
BenchmarkQuery/ksql/pgx-adapter/multiple-rows-12       	   71182	     89788 ns/op
BenchmarkQuery/sql/single-row-12                       	   40140	    147991 ns/op
BenchmarkQuery/sql/multiple-rows-12                    	   39210	    154899 ns/op
BenchmarkQuery/sql/prep-stmt/single-row-12             	   82580	     76769 ns/op
BenchmarkQuery/sql/prep-stmt/multiple-rows-12          	   76880	     77115 ns/op
BenchmarkQuery/sqlx/single-row-12                      	   42120	    144501 ns/op
BenchmarkQuery/sqlx/multiple-rows-12                   	   39396	    155193 ns/op
BenchmarkQuery/sqlx/prep-stmt/single-row-12            	   84583	     72094 ns/op
BenchmarkQuery/sqlx/prep-stmt/multiple-rows-12         	   75465	     78078 ns/op
BenchmarkQuery/pgxpool/single-row-12                   	   87724	     72368 ns/op
BenchmarkQuery/pgxpool/multiple-rows-12                	   77012	     77658 ns/op
BenchmarkQuery/gorm/single-row-12                      	   74268	     80303 ns/op
BenchmarkQuery/gorm/multiple-rows-12                   	   63933	    100220 ns/op
BenchmarkQuery/sqlc/single-row-12                      	   39211	    149178 ns/op
BenchmarkQuery/sqlc/multiple-rows-12                   	   38748	    153076 ns/op
BenchmarkQuery/sqlc/prep-stmt/single-row-12            	   83739	     76111 ns/op
BenchmarkQuery/sqlc/prep-stmt/multiple-rows-12         	   75025	     80939 ns/op
BenchmarkQuery/sqlboiler/single-row-12                 	   63660	     95534 ns/op
BenchmarkQuery/sqlboiler/multiple-rows-12              	   64256	     98597 ns/op
PASS
ok  	github.com/vingarcia/ksql/benchmarks	224.967s
Benchmark executed at: 2022-12-04
Benchmark executed on commit: e7896dc16ef8ede091e5d1568bd53096af65a1ef
```

## Running the KSQL tests (for contributors)

The tests use `docker-test` for setting up all the supported databases,
which means that:

- You need to have `docker` installed
- You must be able to run docker without `sudo`, i.e.
  if you are not root you should add yourself to the docker group, e.g.:

  ```bash
  $ sudo usermod <your_username> -aG docker
  ```
  And then restart your login session (or just reboot)

After that, you can just run the tests by using:

```bash
make test
```

But it is recommended to first download the required images using:

```bash
docker pull postgres:14.0
docker pull mysql:8.0.27
docker pull mcr.microsoft.com/mssql/server:2017-latest
```

Otherwise, the first attempt to run the tests will
spend a long time downloading these images
and then fail because the `TestMain()` function
is configured to kill the containers after 20 seconds.

## TODO List

- Update `ksqltest.FillStructWith` to work with `ksql:"..,json"` tagged attributes
- Improve error messages (ongoing)

## Optimization Opportunities

- Test if using a pointer on the field info is faster or not
- Consider passing the cached structInfo as an argument for all the functions that use it,
  so that we don't need to get it more than once in the same call.
- Use a cache to store often-used queries (like pgx)
- Preload the insert method for all dialects inside `ksql.NewTable()`
- Use prepared statements for the helper functions, `Update`, `Insert` and `Delete`.

## Features for a possible V2

- Change the `.Transaction(db ksql.Provider)` to a `.Transaction(ctx context.Context)`
- Make the `.Query()` method to return a `type Query interface { One(); All(); Chunks(); }`
- Have an `Update()` method that updates without ignoring NULLs as `Patch()` does
  - Have a new Modifier `skipNullUpdates` so that the Update function will do the job of the `Patch`
  - Remove the `Patch` function.
- Rename `NewTable()` to just `Table()` so it feels right to declare it inline when convenient
