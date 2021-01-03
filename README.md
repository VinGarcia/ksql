
# KissORM

Welcome to the KissORM project, the Keep It Stupid Simple - ORM.

This ORM was created to be used by any developer efficiently and safely.
The goals were:

- It should be easy to use
- It should be hard to make mistakes
- It should have a small API so it's easy to learn
- It should be easy to mock and test (very easy)
- It should be above all readable.

**Supported Drivers:**

Currently we only support 2 Drivers:

- `"postgres"`
- `"sqlite3"`

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
read the example tests available on the our [example service](./examples/testing)

### TODO List

- Add support for transactions
- Improve error messages
- Allow the ID field to have a different name
- Allow database replicas for reading
- Fix a bug that is causing "database locked" errors when some the tests fail
- Implement a JSON fields on the database (encoding/decoding them automatically into structs)
- Double check if all reflection is safe on the Insert() function
- Make sure `SELECT *` works even if not all fields are present
