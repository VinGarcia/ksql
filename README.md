
# KissSQL

Welcome to the KissSQL project, the Keep It Stupid Simple sql package.

This package was created to be used by any developer efficiently and safely.
The goals were:

- To be easy to use
- To be hard to make mistakes
- To have a small API so it's easy to learn
- To be easy to mock and test (very easy)
- To be above all readable.

**Supported Drivers:**

Currently we only support 2 Drivers:

- `"postgres"`
- `"sqlite3"`

### Why KissSQL?

> Note: If you want numbers see our Benchmark section below

KissSQL was created to fill a hole between the complexity
we find in the tools I've seen so far, namely:

- ORMs such as `GORM` that do a lot and have literally hundreds
  of functions that require learning, increasing the risk of
  interpretation errors, learning time, complicating mocking, etc.
- Tools such as `sqlx` that do little but still have most of
  the quirks from the standard `sql` lib, requiring several
  error checks for each query which is very low level.

Besides both these examples were not created having
easy tests as one of the requisites, which might cause
your team to loose far more time than necessary writing
the tests or worst: Opting to not writing tests since
it would take too much time.

So the goal was to be high level enough that it would
avoid the complications from the `sql` package and
at the same time to be simple enough to avoid
the big learning curve and complexity of the hundreds
of functions offered by ORMs.

That said, KissSQL attempts to apply the Kiss principle,
in order to save development time for your team, i.e.:

- Less time spent learning (few methods to learn)
- Less time spent testing (helper tools made to help you)
- less time spent debugging (simple apis are easier to debug)
- and less time reading & understanding the code

### Kiss Interface

The current interface is as follows and we plan to keep
it with as little functions as possible, so don't expect many additions:

```go
// SQLProvider describes the public behavior of this ORM
type SQLProvider interface {
	Insert(ctx context.Context, records ...interface{}) error
	Delete(ctx context.Context, ids ...interface{}) error
	Update(ctx context.Context, records ...interface{}) error

	Query(ctx context.Context, records interface{}, query string, params ...interface{}) error
	QueryOne(ctx context.Context, record interface{}, query string, params ...interface{}) error
	QueryChunks(ctx context.Context, parser ChunkParser) error

	Exec(ctx context.Context, query string, params ...interface{}) error
	Transaction(ctx context.Context, fn func(SQLProvider) error) error
}
```

### Usage examples

This example is also available [here](./examples/crud/crud.go)
if you want to compile it yourself.

```Go
package main

import (
	"context"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
	"github.com/vingarcia/ksql"
	"github.com/vingarcia/ksql/nullable"
)

type User struct {
	ID      int     `ksql:"id"`
	Name    string  `ksql:"name"`
	Age     int     `ksql:"age"`

	// This field will be saved as JSON in the database
	Address Address `ksql:"address,json"`
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

func main() {
	ctx := context.Background()
	db, err := ksql.New("sqlite3", "/tmp/hello.sqlite", 1, "users")
	if err != nil {
		panic(err.Error())
	}

	// In the definition below, please note that BLOB is
	// the only type we can use in sqlite for storing JSON.
	err = db.Exec(ctx, `CREATE TABLE IF NOT EXISTS users (
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
	err = db.Insert(ctx, &alison)
	if err != nil {
		panic(err.Error())
	}
	fmt.Println("Alison ID:", alison.ID)

	// Inserting inline:
	err = db.Insert(ctx, &User{
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
	err = db.Delete(ctx, alison.ID)
	if err != nil {
		panic(err.Error())
	}

	// Retrieving Cristina:
	var cris User
	err = db.QueryOne(ctx, &cris, "SELECT * FROM users WHERE name = ? ORDER BY id", "Cristina")
	if err != nil {
		panic(err.Error())
	}
	fmt.Printf("Cristina: %#v\n", cris)

	// Updating all fields from Cristina:
	cris.Name = "Cris"
	err = db.Update(ctx, cris)

	// Changing the age of Cristina but not touching any other fields:

	// Partial update technique 1:
	err = db.Update(ctx, struct {
		ID  int `ksql:"id"`
		Age int `ksql:"age"`
	}{ID: cris.ID, Age: 28})
	if err != nil {
		panic(err.Error())
	}

	// Partial update technique 2:
	err = db.Update(ctx, PartialUpdateUser{
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
	err = db.Query(ctx, &users, "SELECT * FROM users LIMIT 10")
	if err != nil {
		panic(err.Error())
	}

	// Making transactions:
	err = db.Transaction(ctx, func(db ksql.SQLProvider) error {
		var cris2 User
		err = db.QueryOne(ctx, &cris2, "SELECT * FROM users WHERE id = ?", cris.ID)
		if err != nil {
			// This will cause an automatic rollback:
			return err
		}

		err = db.Update(ctx, PartialUpdateUser{
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

### Testing Examples

This library has a few helper functions for helping your tests:

- `ksql.FillStructWith(struct interface{}, dbRow map[string]interface{}) error`
- `ksql.FillSliceWith(structSlice interface{}, dbRows []map[string]interface{}) error`
- `ksql.StructToMap(struct interface{}) (map[string]interface{}, error)`

If you want to see examples (we have examples for all the public functions) just
read the example tests available on our [example service](./examples/example_service)

### Benchmark Comparison

The benchmark is not bad, as far the code is in average as fast as sqlx:

```bash
$ make bench TIME=3s
go test -bench=. -benchtime=3s
goos: linux
goarch: amd64
pkg: github.com/vingarcia/ksql
cpu: Intel(R) Core(TM) i5-3210M CPU @ 2.50GHz
BenchmarkInsert/ksql-setup/insert-one-4         	    4302	    776648 ns/op
BenchmarkInsert/sqlx-setup/insert-one-4            	    4716	    762358 ns/op
BenchmarkQuery/ksql-setup/single-row-4          	   12204	    293858 ns/op
BenchmarkQuery/ksql-setup/multiple-rows-4       	   11145	    323571 ns/op
BenchmarkQuery/sqlx-setup/single-row-4             	   12440	    290937 ns/op
BenchmarkQuery/sqlx-setup/multiple-rows-4          	   10000	    310314 ns/op
PASS
ok  	github.com/vingarcia/ksql	34.251s
```

### TODO List

- Implement support for nested objects with prefixed table names
- Improve error messages
- Add tests for tables using composite keys
- Add support for serializing structs as other formats such as YAML
- Update structs.FillStructWith to work with `json` tagged attributes

### Optimization Oportunities

- Test if using a pointer on the field info is faster or not
- Consider passing the cached structInfo as argument for all the functions that use it,
  so that we don't need to get it more than once in the same call.
