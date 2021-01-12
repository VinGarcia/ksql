
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
you team to loose far more time than necessary writing
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

The current interface is as follows:

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
}
```

You might notice we are lacking an abstraction for transactions, but it is on our TODO list.

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
read the example tests available on the our [example service](./examples/example_service)

### TODO List

- Add support for transactions
- Improve error messages
- Allow the ID field to have a different name
- Fix a bug that is causing "database locked" errors when some of the tests fail
- Implement a JSON fields on the database (encoding/decoding them automatically into structs)
- Implement support for nested objects with prefixed table names
- Double check if all reflection is safe on the Insert() function
- Make sure `SELECT *` works even if not all fields are present
