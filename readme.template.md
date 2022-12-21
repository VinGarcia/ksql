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

## Let's start with some Code:

This short example below is a TLDR version for illustrating how easy it is to use KSQL.

You will find more complete examples on the sections below.

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
of well thought functions that cover all use-cases,
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

In the example below we'll cover all the most common use-cases such as:

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

For the more common use-cases please read the example below,
which is also available [here](./examples/crud/crud.go)
if you want to compile it yourself.

```Go
{{ .crudExample -}}
```

## Benchmark Comparison

The results of the benchmark are good:
they show that KSQL is in practical terms,
as fast as sqlx which was our goal from the start.

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

For each of these tools we are running 3 different queries:

The `insert-one` query looks like:

`INSERT INTO users (name, age) VALUES ($1, $2) RETURNING id`

The `single-row` query looks like:

`SELECT id, name, age FROM users OFFSET $1 LIMIT 1`

The `multiple-rows` query looks like:

`SELECT id, name, age FROM users OFFSET $1 LIMIT 10`

Keep in mind that some of the tools tested (like GORM) actually build
the queries internally so the actual code used for the benchmark
might differ a little bit from the example ones above.

Without further ado, here are the results:

```bash
$ make bench TIME=5s
{{ .benchmark -}}
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

After that you can just run the tests by using:

```bash
make test
```

But it is recommended to first download the required images using:

```bash
docker pull postgres:14.0
docker pull mysql:8.0.27
docker pull mcr.microsoft.com/mssql/server:2017-latest
```

Otherwise the first attempt to run the tests will
spend a long time downloading these images
and then fail because the `TestMain()` function
is configured to kill the containers after 20 seconds.

## TODO List

- Update `ksqltest.FillStructWith` to work with `ksql:"..,json"` tagged attributes
- Create a way for users to submit user defined dialects
- Improve error messages (ongoing)
- Add support for the Patch function to work with maps for partial updates
- Add support for the Insert function to work with maps
- Add support for a `ksql.Array(params ...interface{})` for allowing queries like this:
  `db.Query(ctx, &user, "SELECT * FROM user WHERE id in (?)", ksql.Array(1,2,3))`

## Optimization Oportunities

- Test if using a pointer on the field info is faster or not
- Consider passing the cached structInfo as argument for all the functions that use it,
  so that we don't need to get it more than once in the same call.
- Use a cache to store often used queries (like pgx)
- Preload the insert method for all dialects inside `ksql.NewTable()`
- Use prepared statements for the helper functions, `Update`, `Insert` and `Delete`.

## Features for a possible V2

- Change the `.Transaction(db ksql.Provider)` to a `.Transaction(ctx context.Context)`
- Make the `.Query()` method to return a `type Query interface { One(); All(); Chunks(); }`
- Have an `Update()` method that updates without ignoring NULLs as `Patch()` does
  - Have a new Modifier `skipNullUpdates` so that the Update function will do the job of the `Patch`
  - Remove the `Patch` function.
- Rename `NewTable()` to just `Table()` so it feels right to declare it inline when convenient
