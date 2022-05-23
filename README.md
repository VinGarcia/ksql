
[![CI](https://github.com/VinGarcia/ksql/actions/workflows/ci.yml/badge.svg)](https://github.com/VinGarcia/ksql/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/VinGarcia/ksql/branch/master/graph/badge.svg?token=5CNJ867C66)](https://codecov.io/gh/VinGarcia/ksql)

# Kiss SQL

KissSQL or the "Keep it Simple" SQL package was created to offer an
actually simple and satisfactory tool for interacting with SQL Databases.

The core idea on `ksql` is to offer an easy to use interface,
the actual comunication with the database is decoupled so we can use
`ksql` on top of `pgx`, `database/sql` and possibly other tools.
You can even create you own backend adapter for `ksql` which is
useful in some situations.

## Using `ksql`

This is a TLDR version of the more complete examples below.

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

	// For loading entities from the database `ksql` can build
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

- `kpgx.New(ctx, os.Getenv("POSTGRES_URL"), ksql.Config{})` for Postgres, it works on top of `pgxpool`
- `kmysql.New(ctx, os.Getenv("POSTGRES_URL"), ksql.Config{})` for MySQL, it works on top of `database/sql`
- `ksqlserver.New(ctx, os.Getenv("POSTGRES_URL"), ksql.Config{})` for SQLServer, it works on top of `database/sql`
- `ksqlite3.New(ctx, os.Getenv("POSTGRES_URL"), ksql.Config{})` for SQLite3, it works on top of `database/sql`

## Why `ksql`?

> Note: If you want numbers see our [Benchmark section](https://github.com/vingarcia/ksql#benchmark-comparison) below

ksql is meant to improve on the existing ecosystem by providing
a well-designed database package that has:

1. A small number of easy-to-use helper functions for common use cases
2. Support for more complicated use-cases by allowing
   the user to write SQL directly

This strategy allows the API to be

- Very simple with a small number of functions
- Harness all the power of SQL, by just allowing the user to type SQL
- Less opportunities for making mistakes, which makes code reviews easier
- A succinct and idiomatic Go idiom reducing the cognitive complexity of your code
- Easy ways of mocking your database when you need to
- Support for all common relational database: `mysql`, `sqlite`, `sqlserver` and `postgres`

Some special use-cases also have some special support:

- The `QueryChunks()` method helps you in the few situations when you might
  need to load in a single query more data than would fit in memory.
- For saving you time when you are selecting all fields from a struct you
  can omit the `SELECT ...` part of the query which causes ksql to write
  this part for you saving a lot of work when working with big structs/tables.
- The Nested Structs feature will help you reuse existing structs/models when working with JOINs.

**Supported Drivers:**

ksql is well decoupled from its backend implementation which makes
it easy to change the actual technology used, currently we already
support the following options:

- Using the `database/sql` as the backend we support the following drivers:
  - `"postgres"`
  - `"sqlite3"`
  - `"mysql"`
  - `"sqlserver"`
- We also support `pgx` (actually `pgxpool`) as the backend which
  is a lot faster for Postgres databases.

If you need a new `database/sql` driver or backend adapter included
please open an issue or make your own implementation
and submit it as a Pull Request.

## Comparing `ksql` with other tools

`ksql` was created because of a few insatisfactions
with the existing packages for interacting with
relational databases in Go. To mention a few:

**Low Level Tools:**

Tools like `database/sql`, `sqlx` and even `pgx` will usually
require you to check errors several times for the same query and
also when iterating over several rows you end up with a `for rows.Next() {}`
loop which is often more cognitive complex than desirable.

**High Level Tools such as ORMs:**

More high level tools such as `gorm` and `bun` will often force you
and your team to interact with a complicated DSL which requires
time to learn it and then ending up still being a little bit harder
to read than a regular SQL query would be.

**Code Generation tools:**

Tools like `sqlc` and `sqlboiler` that rely on code generation
are good options if performance is your main goal, but they also
have some issues that might bother you:

- There is some learning curve that goes beyond just reading a GoDoc as with most packages.
- You will often need to copy to and from custom generated structs instead of using your own.
- Sometimes the generated function will not be as flexible as you'd prefer forcing you to make
  some tricks with SQL (e.g. that happens with `sqlc` for partial updates for example).
- And it does add an extra step on your building process.

And finally you might just prefer to avoid codegen when possible,
in which case ksql is also for you.

## Kiss Interface

The current interface is as follows and we plan on keeping
it with as little functions as possible, so don't expect many additions:

```go
// Provider describes the ksql public behavior
//
// The Insert, Patch, Delete and QueryOne functions return ksql.ErrRecordNotFound
// if no record was found or no rows were changed during the operation.
type Provider interface {
	Insert(ctx context.Context, table Table, record interface{}) error
	Patch(ctx context.Context, table Table, record interface{}) error
	Delete(ctx context.Context, table Table, idOrRecord interface{}) error

	Query(ctx context.Context, records interface{}, query string, params ...interface{}) error
	QueryOne(ctx context.Context, record interface{}, query string, params ...interface{}) error
	QueryChunks(ctx context.Context, parser ChunkParser) error

	Exec(ctx context.Context, query string, params ...interface{}) (rowsAffected int64, _ error)
	Transaction(ctx context.Context, fn func(Provider) error) error
}
```

## Usage examples

This example is also available [here](./examples/crud/crud.go)
if you want to compile it yourself.

```Go
package main

import (
	"context"
	"fmt"

	"github.com/vingarcia/ksql"
	"github.com/vingarcia/ksql/adapters/ksqlite3"
	"github.com/vingarcia/ksql/nullable"
)

// User ...
type User struct {
	ID   int    `ksql:"id"`
	Name string `ksql:"name"`
	Age  int    `ksql:"age"`

	// This field will be saved as JSON in the database
	Address Address `ksql:"address,json"`
}

// PartialUpdateUser ...
type PartialUpdateUser struct {
	ID      int      `ksql:"id"`
	Name    *string  `ksql:"name"`
	Age     *int     `ksql:"age"`
	Address *Address `ksql:"address,json"`
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
		address BLOB
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
	// ksql will build it for you (efficiently) based on the fields from the struct:
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
		Age: nullable.Int(28),
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
			// so that we don't hide the panic inside the KissSQL library
			panic(err.Error())
		}

		// Commits the transaction
		return nil
	})
	if err != nil {
		panic(err.Error())
	}

	fmt.Printf("Users: %#v\n", users)
}
```

## Query Chunks Feature

It's very unsual for us to need to load a number of records from the
database that might be too big for fitting in memory, e.g. load all the
users and send them somewhere. But it might happen.

For these cases it's best to load chunks of data at a time so
that we can work on a substantial amount of data at a time and never
overload our memory capacity. For this use case we have a specific
function called `QueryChunks`:

```golang
err = db.QueryChunks(ctx, ksql.ChunkParser{
	Query:     "SELECT * FROM users WHERE type = ?",
	Params:    []interface{}{usersType},
	ChunkSize: 100,
	ForEachChunk: func(users []User) error {
		err := sendUsersSomewhere(users)
		if err != nil {
			// This will abort the QueryChunks loop and return this error
			return err
		}
		return nil
	},
})
if err != nil {
	panic(err.Error())
}
```

It's signature is more complicated than the other two Query\* methods,
thus, it is adivisible to always prefer using the other two when possible
reserving this one for the rare use-case where you are actually
loading big sections of the database into memory.

## Select Generator Feature

There are good reasons not to use `SELECT *` queries the most important
of them is that you might end up loading more information than you are actually
going to use putting more pressure in your database for no good reason.

To prevent that `ksql` has a feature specifically for building the `SELECT`
part of the query using the tags from the input struct.
Using it is very simple and it works with all the 3 Query\* functions:

Querying a single user:

```golang
var user User
err = db.QueryOne(ctx, &user, "FROM users WHERE id = ?", userID)
if err != nil {
	panic(err.Error())
}
```

Querying a page of users:

```golang
var users []User
err = db.Query(ctx, &users, "FROM users WHERE type = ? ORDER BY id LIMIT ? OFFSET ?", "Cristina", limit, offset)
if err != nil {
	panic(err.Error())
}
```

Querying all the users, or any potentially big number of users, from the database (not usual, but supported):

```golang
err = db.QueryChunks(ctx, ksql.ChunkParser{
	Query:     "FROM users WHERE type = ?",
	Params:    []interface{}{usersType},
	ChunkSize: 100,
	ForEachChunk: func(users []User) error {
		err := sendUsersSomewhere(users)
		if err != nil {
			// This will abort the QueryChunks loop and return this error
			return err
		}
		return nil
	},
})
if err != nil {
	panic(err.Error())
}
```

The implementation of this feature is actually simple internally.
First we check if the query is starting with the word `FROM`,
if it is then we just get the `ksql` tags from the struct and
then use it for building the `SELECT` statement.

The `SELECT` statement is then cached so we don't have to build it again
the next time in order to keep the library efficient even when
using this feature.

## Select Generation with Joins

So there is one use-case that was not covered by `ksql` so far:

What if you want to JOIN multiple tables for which you already have
structs defined? Would you need to create a new struct to represent
the joined columns of the two tables? no, we actually have this covered as well.

`ksql` has a special feature for allowing the reuse of existing
structs by using composition in an anonymous struct, and then
generating the `SELECT` part of the query accordingly:

Querying a single joined row:

```golang
var row struct{
	User User `tablename:"u"`     // (here the tablename must match the aliased tablename in the query)
	Post Post `tablename:"posts"` // (if no alias is used you should use the actual name of the table)
}
err = db.QueryOne(ctx, &row, "FROM users as u JOIN posts ON u.id = posts.user_id WHERE u.id = ?", userID)
if err != nil {
	panic(err.Error())
}
```

Querying a page of joined rows:

```golang
var rows []struct{
	User User `tablename:"u"`
	Post Post `tablename:"p"`
}
err = db.Query(ctx, &rows,
	"FROM users as u JOIN posts as p ON u.id = p.user_id WHERE name = ? LIMIT ? OFFSET ?",
	"Cristina", limit, offset,
)
if err != nil {
	panic(err.Error())
}
```

Querying all the users, or any potentially big number of users, from the database (not usual, but supported):

```golang
err = db.QueryChunks(ctx, ksql.ChunkParser{
	Query:     "FROM users as u JOIN posts as p ON u.id = p.user_id WHERE type = ?",
	Params:    []interface{}{usersType},
	ChunkSize: 100,
	ForEachChunk: func(rows []struct{
		User User `tablename:"u"`
		Post Post `tablename:"p"`
	}) error {
		err := sendRowsSomewhere(rows)
		if err != nil {
			// This will abort the QueryChunks loop and return this error
			return err
		}
		return nil
	},
})
if err != nil {
	panic(err.Error())
}
```

As advanced as this feature might seem we don't do any parsing of the query,
and all the work is done only once and then cached.

What actually happens is that we use the "tablename" tag to build the `SELECT`
part of the query like this:

- `SELECT u.id, u.name, u.age, p.id, p.title `

This is then cached, and when we need it again we concatenate it with the rest
of the query.

This feature has two important limitations:

1. It is not possible to use `tablename` tags together with normal `ksql` tags.
   Doing so will cause the `tablename` tags to be ignored in favor of the `ksql` ones.
2. It is not possible to use it without omitting the `SELECT` part of the query.
   While in normal queries we match the selected field with the attribute by name,
   in queries joining multiple tables we can't use this strategy because
   different tables might have columns with the same name, and we don't
   really have access to the full name of these columns making, for example,
   it impossible to differentiate between `u.id` and `p.id` except by the
   order in which these fields were passed. Thus, it is necessary that
   the library itself writes the `SELECT` part of the query when using
   this technique so that we can control the order or the selected fields.

Ok, but what if I don't want to use this feature?

You are not forced to, and there are a few use-cases where you would prefer not to, e.g.:

```golang
var rows []struct{
	UserName string `ksql:"name"`
	PostTitle string `ksql:"title"`
}
err := db.Query(ctx, &rows, "SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id LIMIT 10")
if err != nil {
	panic(err.Error())
}
```

In the example above, since we are only interested in a couple of columns it
is far simpler and more efficient for the database to only select the columns
that we actually care about, so it's better not to use composite structs.

## Testing Examples & ksql.Mock

`ksql.Mock` is a simple mock that is available out of the box for the `ksql.Provider` interface.

For each of the methods available on the interface this Mock has a function attribute with the same
function signature and with the same name but with an `Fn` in the end of the name.

For instantiating this mock and at the same time mocking one of the functions all you need to do is
this:

```golang
var capturedRecord interface{}
var capturedQuery string
var capturedParams []interface{}
mockDB := ksql.Mock{
  QueryOneFn: func(ctx context.Context, record interface{}, query string, params ...interface{}) error {
    capturedRecord = record
    capturedQuery = query
    capturedParams = params

    // For simulating an error you would do this:
    return fmt.Errorf("some fake error")
  },
}

var user User
err := GetUser(db, &user, otherArgs)
assert.NotNil(t, err)
assert.Equal(t, user, capturedRecord)
assert.Equal(t, `SELECT * FROM user WHERE other_args=$1`, capturedQuery)
assert.Equal(t, []interface{}{otherArgs}, capturedParams)
```

For different types of functions you might need do some tricks with structs to make it easier
to write the tests such as:

Converting a struct to something that is more easy to assert like a `map[string]interface{}`
or filling a struct with fake data from the database, for these situations the library provides
the following functions:

- `ksqltest.FillStructWith(struct interface{}, dbRow map[string]interface{}) error`
- `ksqltest.FillSliceWith(structSlice interface{}, dbRows []map[string]interface{}) error`
- `ksqltest.StructToMap(struct interface{}) (map[string]interface{}, error)`

For example:

```golang
createdAt := time.Now().Add(10*time.Hour)
mockDB := ksql.Mock{
  QueryOneFn: func(ctx context.Context, record interface{}, query string, params ...interface{}) error {
    // For simulating a succesful scenario you can just fillup the struct:
    return ksqltest.FillStructWith(map[string]interface{}{
      "id": 42,
      "name": "fake-name",
      "age": 32,
      "created_at": createdAt,
    })
  },
}

var user User
err := GetUser(db, &user, otherArgs)
assert.Nil(t, err)
assert.Equal(t, user, User{
  ID: 42,
  Name: "fake-name",
  Age: 32,
  CreatedAt: createdAt,
})
```

And for running tests on the `QueryChunks` function which is a particularly complex function
we also have this test helper:

- `ksqltest.CallFunctionWithRows(fn interface{}, rows []map[string]interface{}) (map[string]interface{}, error)`

If you want to see examples (we have examples for all the public functions) just
read the example tests available on our [example service](./examples/example_service/).

Please note that that in the example service above we have two sets
of tests in two different files, exemplifying how to use `gomock` and
then how to use `ksql.Mock{}`.

## Benchmark Comparison

The results of the benchmark are good:
they show that ksql is in practical terms,
as fast as sqlx which was our goal from the start.

To understand the benchmark below you must know
that all tests are performed using Postgres 12.1 and
that we are comparing the following tools:

- ksql using the adapter that wraps database/sql
- ksql using the adapter that wraps pgx
- sql
- sqlx
- pgx (with pgxpool)
- gorm

For each of these tools we are running 3 different queries:

The `insert-one` query looks like:

`INSERT INTO users (name, age) VALUES ($1, $2) RETURNING id`

The `single-row` query looks like:

`SELECT id, name, age FROM users OFFSET $1 LIMIT 1`

The `multiple-rows` query looks like:

`SELECT id, name, age FROM users OFFSET $1 LIMIT 10`

Keep in mind that some of the tools tested actually build
the query internally so the actual query might differ a little
bit from the example ones above.

Without further ado, here are the results:

```bash
$ make bench TIME=5s
cd benchmarks && go test -bench=. -benchtime=5s
goos: linux
goarch: amd64
pkg: github.com/vingarcia/ksql/benchmarks
cpu: Intel(R) Core(TM) i7-10750H CPU @ 2.60GHz
BenchmarkInsert/ksql/sql-adapter/insert-one-12         	    7966	    630421 ns/op
BenchmarkInsert/ksql/pgx-adapter/insert-one-12         	   10000	    557143 ns/op
BenchmarkInsert/sql/insert-one-12                      	    9296	    627834 ns/op
BenchmarkInsert/sql/prep-statements/insert-one-12      	   10836	    561999 ns/op
BenchmarkInsert/sqlx/insert-one-12                     	    9686	    638942 ns/op
BenchmarkInsert/pgxpool/insert-one-12                  	   10000	    567271 ns/op
BenchmarkInsert/gorm/insert-one-12                     	    9163	    669396 ns/op
BenchmarkQuery/ksql/sql-adapter/single-row-12          	   42124	    149014 ns/op
BenchmarkQuery/ksql/sql-adapter/multiple-rows-12       	   38244	    157941 ns/op
BenchmarkQuery/ksql/pgx-adapter/single-row-12          	   88578	     70401 ns/op
BenchmarkQuery/ksql/pgx-adapter/multiple-rows-12       	   74156	     81391 ns/op
BenchmarkQuery/sql/single-row-12                       	   42818	    142127 ns/op
BenchmarkQuery/sql/multiple-rows-12                    	   38788	    148733 ns/op
BenchmarkQuery/sql/prep-statements/single-row-12       	   85287	     69135 ns/op
BenchmarkQuery/sql/prep-statements/multiple-rows-12    	   80311	     73877 ns/op
BenchmarkQuery/sqlx/single-row-12                      	   41606	    146031 ns/op
BenchmarkQuery/sqlx/multiple-rows-12                   	   39592	    154469 ns/op
BenchmarkQuery/pgxpool/single-row-12                   	   88638	     66996 ns/op
BenchmarkQuery/pgxpool/multiple-rows-12                	   83715	     71674 ns/op
BenchmarkQuery/gorm/single-row-12                      	   80734	     73582 ns/op
BenchmarkQuery/gorm/multiple-rows-12                   	   63243	     95192 ns/op
PASS
ok  	github.com/vingarcia/ksql/benchmarks	146.098s
Benchmark executed at: 2022-01-20
Benchmark executed on commit: 8cd7a37da9c74f6f365665cd0c20d24843284421
```

## Running the ksql tests (for contributors)

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

- Add tests for tables using composite keys
- Add support for serializing structs as other formats such as YAML
- Update `ksqltest.FillStructWith` to work with `ksql:"..,json"` tagged attributes
- Create a way for users to submit user defined dialects
- Improve error messages
- Add support for the Patch function to work with maps for partial updates
- Add support for the Insert function to work with maps
- Add support for a `ksql.Array(params ...interface{})` for allowing queries like this:
  `db.Query(ctx, &user, "SELECT * FROM user WHERE id in (?)", ksql.Array(1,2,3))`
- Improve docs about `ksql.Mock`

## Optimization Oportunities

- Test if using a pointer on the field info is faster or not
- Consider passing the cached structInfo as argument for all the functions that use it,
  so that we don't need to get it more than once in the same call.
- Use a cache to store often used queries (like pgx)
- Preload the insert method for all dialects inside `ksql.NewTable()`
foo
foo
foo
foo
foo
foo
foo
foo
foo
foo
foo
foo
