
# KissORM

Welcome to the KissORM project, the Keep It Stupid Simple - ORM.

This ORM was created to be used by any developer efficiently and safely.
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

### Why KissORM?

> Note: If you want numbers see our Benchmark section below

KissORM was created to fill a hole between the complexity
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
of functions offered by more complete ORMs.

That said, KissORM attempts to apply the Kiss principle,
in order to save development time for your team, i.e.:

- Less time spent learning (few methods to learn)
- Less time spent testing (helper tools made to help you)
- less time spent debugging (simple apis are easier to debug)
- and less time reading & understanding the code

### Kiss Interface

The current interface is as follows and we plan to keep
it with as little functions as possible, so don't expect many additions:

```go
// ORMProvider describes the public behavior of this ORM
type ORMProvider interface {
	Insert(ctx context.Context, records ...interface{}) error
	Delete(ctx context.Context, ids ...interface{}) error
	Update(ctx context.Context, records ...interface{}) error

	Query(ctx context.Context, records interface{}, query string, params ...interface{}) error
	QueryOne(ctx context.Context, record interface{}, query string, params ...interface{}) error
	QueryChunks(ctx context.Context, parser ChunkParser) error

	Exec(ctx context.Context, query string, params ...interface{}) error
	Transaction(ctx context.Context, fn func(ORMProvider) error) error
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
	"github.com/vingarcia/kissorm"
	"github.com/vingarcia/kissorm/nullable"
)

type User struct {
	ID   int    `kissorm:"id"`
	Name string `kissorm:"name"`
	Age  int    `kissorm:"age"`
}

type PartialUpdateUser struct {
	ID   int     `kissorm:"id"`
	Name *string `kissorm:"name"`
	Age  *int    `kissorm:"age"`
}

func main() {
	ctx := context.Background()
	db, err := kissorm.New("sqlite3", "/tmp/hello.sqlite", 1, "users")
	if err != nil {
		panic(err.Error())
	}

	err = db.Exec(ctx, `CREATE TABLE IF NOT EXISTS users (
	  id INTEGER PRIMARY KEY,
		age INTEGER,
		name TEXT
	)`)
	if err != nil {
		panic(err.Error())
	}

	var alison = User{
		Name: "Alison",
		Age:  22,
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
		ID  int `kissorm:"id"`
		Age int `kissorm:"age"`
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
	var users []User
	err = db.Query(ctx, &users, "SELECT * FROM users LIMIT 10")
	if err != nil {
		panic(err.Error())
	}
	fmt.Printf("Users: %#v\n", users)
}
```

### Testing Examples

This library has a few helper functions for helping your tests:

- `kissorm.FillStructWith(struct interface{}, dbRow map[string]interface{}) error`
- `kissorm.FillSliceWith(structSlice interface{}, dbRows []map[string]interface{}) error`
- `kissorm.StructToMap(struct interface{}) (map[string]interface{}, error)`

If you want to see examples (we have examples for all the public functions) just
read the example tests available on our [example service](./examples/example_service)

### Benchmark Comparison

The benchmark is not bad, as far the code is in average as fast as sqlx:

```bash
$ make bench TIME=3s
go test -bench=. -benchtime=3s
goos: linux
goarch: amd64
pkg: github.com/vingarcia/kissorm
BenchmarkInsert/kissorm-setup/insert-one-4         	    4306	    880132 ns/op
BenchmarkInsert/sqlx-setup/insert-one-4            	    4573	    792488 ns/op
BenchmarkQuery/kissorm-setup/single-row-4          	   10000	    315328 ns/op
BenchmarkQuery/kissorm-setup/multiple-rows-4       	    9288	    388538 ns/op
BenchmarkQuery/sqlx-setup/single-row-4             	   10000	    323424 ns/op
BenchmarkQuery/sqlx-setup/multiple-rows-4          	   10000	    338570 ns/op
PASS
ok  	github.com/vingarcia/kissorm	21.740s
```

### TODO List

- Write documentation for the new feature of serializing structs as JSON
- Implement support for nested objects with prefixed table names
- Improve error messages
- Add tests for tables using composite keys
- Add support for serializing structs as other formats such as YAML

### Optimizations Oportunities

- Test if using a pointer on the field info is faster or not
- Consider passing the cached structInfo as argument for all the functions that use it,
  so that we don't need to get it twice in the same call.
