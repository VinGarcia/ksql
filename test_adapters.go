package ksql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"testing"

	"github.com/pkg/errors"
	tt "github.com/vingarcia/ksql/internal/testtools"
	"github.com/vingarcia/ksql/nullable"
)

var usersTable = NewTable("users")

type user struct {
	ID   uint   `ksql:"id"`
	Name string `ksql:"name"`
	Age  int    `ksql:"age"`

	Address address `ksql:"address,json"`

	// This attr has no ksql tag, thus, it should be ignored:
	AttrThatShouldBeIgnored string
}

type address struct {
	Street string `json:"street"`
	Number string `json:"number"`

	City    string `json:"city"`
	State   string `json:"state"`
	Country string `json:"country"`
}

type post struct {
	ID     int    `ksql:"id"`
	UserID uint   `ksql:"user_id"`
	Title  string `ksql:"title"`
}

var userPermissionsTable = NewTable("user_permissions", "user_id", "perm_id")

type userPermission struct {
	ID     int    `ksql:"id"`
	UserID int    `ksql:"user_id"`
	PermID int    `ksql:"perm_id"`
	Type   string `ksql:"type"`
}

// RunTestsForAdapter will run all necessary tests for making sure
// a given adapter is working as expected.
//
// Optionally it is also possible to run each of these tests
// separatedly, which might be useful during the development
// of a new adapter.
func RunTestsForAdapter(
	t *testing.T,
	adapterName string,
	driver string,
	connStr string,
	newDBAdapter func(t *testing.T) (DBAdapter, io.Closer),
) {
	t.Run(adapterName+"."+driver, func(t *testing.T) {
		QueryTest(t, driver, connStr, newDBAdapter)
		QueryOneTest(t, driver, connStr, newDBAdapter)
		InsertTest(t, driver, connStr, newDBAdapter)
		DeleteTest(t, driver, connStr, newDBAdapter)
		PatchTest(t, driver, connStr, newDBAdapter)
		QueryChunksTest(t, driver, connStr, newDBAdapter)
		TransactionTest(t, driver, connStr, newDBAdapter)
		ScanRowsTest(t, driver, connStr, newDBAdapter)
	})
}

// QueryTest runs all tests for making sure the Query function is
// working for a given adapter and driver.
func QueryTest(
	t *testing.T,
	driver string,
	connStr string,
	newDBAdapter func(t *testing.T) (DBAdapter, io.Closer),
) {
	t.Run("QueryTest", func(t *testing.T) {
		variations := []struct {
			desc        string
			queryPrefix string
		}{
			{
				desc:        "with select *",
				queryPrefix: "SELECT * ",
			},
			{
				desc:        "building the SELECT part of the query internally",
				queryPrefix: "",
			},
		}
		for _, variation := range variations {
			t.Run(variation.desc, func(t *testing.T) {
				t.Run("using slice of structs", func(t *testing.T) {
					err := createTables(driver, connStr)
					if err != nil {
						t.Fatal("could not create test table!, reason:", err.Error())
					}

					t.Run("should return 0 results correctly", func(t *testing.T) {
						db, closer := newDBAdapter(t)
						defer closer.Close()

						ctx := context.Background()
						c := newTestDB(db, driver)
						var users []user
						err := c.Query(ctx, &users, variation.queryPrefix+`FROM users WHERE id=1;`)
						tt.AssertNoErr(t, err)
						tt.AssertEqual(t, len(users), 0)

						users = []user{}
						err = c.Query(ctx, &users, variation.queryPrefix+`FROM users WHERE id=1;`)
						tt.AssertNoErr(t, err)
						tt.AssertEqual(t, len(users), 0)
					})

					t.Run("should return a user correctly", func(t *testing.T) {
						db, closer := newDBAdapter(t)
						defer closer.Close()

						_, err := db.ExecContext(context.TODO(), `INSERT INTO users (name, age, address) VALUES ('Bia', 0, '{"country":"BR"}')`)
						tt.AssertNoErr(t, err)

						ctx := context.Background()
						c := newTestDB(db, driver)
						var users []user
						err = c.Query(ctx, &users, variation.queryPrefix+`FROM users WHERE name=`+c.dialect.Placeholder(0), "Bia")

						tt.AssertNoErr(t, err)
						tt.AssertEqual(t, len(users), 1)
						tt.AssertNotEqual(t, users[0].ID, uint(0))
						tt.AssertEqual(t, users[0].Name, "Bia")
						tt.AssertEqual(t, users[0].Address.Country, "BR")
					})

					t.Run("should return multiple users correctly", func(t *testing.T) {
						db, closer := newDBAdapter(t)
						defer closer.Close()

						_, err := db.ExecContext(context.TODO(), `INSERT INTO users (name, age, address) VALUES ('João Garcia', 0, '{"country":"US"}')`)
						tt.AssertNoErr(t, err)

						_, err = db.ExecContext(context.TODO(), `INSERT INTO users (name, age, address) VALUES ('Bia Garcia', 0, '{"country":"BR"}')`)
						tt.AssertNoErr(t, err)

						ctx := context.Background()
						c := newTestDB(db, driver)
						var users []user
						err = c.Query(ctx, &users, variation.queryPrefix+`FROM users WHERE name like `+c.dialect.Placeholder(0), "% Garcia")

						tt.AssertNoErr(t, err)
						tt.AssertEqual(t, len(users), 2)

						tt.AssertNotEqual(t, users[0].ID, uint(0))
						tt.AssertEqual(t, users[0].Name, "João Garcia")
						tt.AssertEqual(t, users[0].Address.Country, "US")

						tt.AssertNotEqual(t, users[1].ID, uint(0))
						tt.AssertEqual(t, users[1].Name, "Bia Garcia")
						tt.AssertEqual(t, users[1].Address.Country, "BR")
					})

					t.Run("should query joined tables correctly", func(t *testing.T) {
						db, closer := newDBAdapter(t)
						defer closer.Close()

						// This test only makes sense with no query prefix
						if variation.queryPrefix != "" {
							return
						}

						_, err := db.ExecContext(context.TODO(), `INSERT INTO users (name, age, address) VALUES ('João Ribeiro', 0, '{"country":"US"}')`)
						tt.AssertNoErr(t, err)
						var joao user
						getUserByName(db, driver, &joao, "João Ribeiro")
						tt.AssertNoErr(t, err)

						_, err = db.ExecContext(context.TODO(), `INSERT INTO users (name, age, address) VALUES ('Bia Ribeiro', 0, '{"country":"BR"}')`)
						tt.AssertNoErr(t, err)
						var bia user
						getUserByName(db, driver, &bia, "Bia Ribeiro")

						_, err = db.ExecContext(context.TODO(), fmt.Sprint(`INSERT INTO posts (user_id, title) VALUES (`, bia.ID, `, 'Bia Post1')`))
						tt.AssertNoErr(t, err)
						_, err = db.ExecContext(context.TODO(), fmt.Sprint(`INSERT INTO posts (user_id, title) VALUES (`, bia.ID, `, 'Bia Post2')`))
						tt.AssertNoErr(t, err)
						_, err = db.ExecContext(context.TODO(), fmt.Sprint(`INSERT INTO posts (user_id, title) VALUES (`, joao.ID, `, 'João Post1')`))
						tt.AssertNoErr(t, err)

						ctx := context.Background()
						c := newTestDB(db, driver)
						var rows []struct {
							User user `tablename:"u"`
							Post post `tablename:"p"`

							// This one has no ksql or tablename tag,
							// so it should just be ignored to avoid strange
							// unexpected errors:
							ExtraStructThatShouldBeIgnored user
						}
						err = c.Query(ctx, &rows, fmt.Sprint(
							`FROM users u JOIN posts p ON p.user_id = u.id`,
							` WHERE u.name like `, c.dialect.Placeholder(0),
							` ORDER BY u.id, p.id`,
						), "% Ribeiro")

						tt.AssertNoErr(t, err)
						tt.AssertEqual(t, len(rows), 3)

						tt.AssertEqual(t, rows[0].User.ID, joao.ID)
						tt.AssertEqual(t, rows[0].User.Name, "João Ribeiro")
						tt.AssertEqual(t, rows[0].Post.Title, "João Post1")

						tt.AssertEqual(t, rows[1].User.ID, bia.ID)
						tt.AssertEqual(t, rows[1].User.Name, "Bia Ribeiro")
						tt.AssertEqual(t, rows[1].Post.Title, "Bia Post1")

						tt.AssertEqual(t, rows[2].User.ID, bia.ID)
						tt.AssertEqual(t, rows[2].User.Name, "Bia Ribeiro")
						tt.AssertEqual(t, rows[2].Post.Title, "Bia Post2")
					})
				})

				t.Run("using slice of pointers to structs", func(t *testing.T) {
					err := createTables(driver, connStr)
					if err != nil {
						t.Fatal("could not create test table!, reason:", err.Error())
					}

					t.Run("should return 0 results correctly", func(t *testing.T) {
						db, closer := newDBAdapter(t)
						defer closer.Close()

						ctx := context.Background()
						c := newTestDB(db, driver)
						var users []*user
						err := c.Query(ctx, &users, variation.queryPrefix+`FROM users WHERE id=1;`)
						tt.AssertNoErr(t, err)
						tt.AssertEqual(t, len(users), 0)

						users = []*user{}
						err = c.Query(ctx, &users, variation.queryPrefix+`FROM users WHERE id=1;`)
						tt.AssertNoErr(t, err)
						tt.AssertEqual(t, len(users), 0)
					})

					t.Run("should return a user correctly", func(t *testing.T) {
						db, closer := newDBAdapter(t)
						defer closer.Close()

						ctx := context.Background()

						_, err := db.ExecContext(ctx, `INSERT INTO users (name, age, address) VALUES ('Bia', 0, '{"country":"BR"}')`)
						tt.AssertNoErr(t, err)

						c := newTestDB(db, driver)
						var users []*user
						err = c.Query(ctx, &users, variation.queryPrefix+`FROM users WHERE name=`+c.dialect.Placeholder(0), "Bia")

						tt.AssertNoErr(t, err)
						tt.AssertEqual(t, len(users), 1)
						tt.AssertNotEqual(t, users[0].ID, uint(0))
						tt.AssertEqual(t, users[0].Name, "Bia")
						tt.AssertEqual(t, users[0].Address.Country, "BR")
					})

					t.Run("should return multiple users correctly", func(t *testing.T) {
						db, closer := newDBAdapter(t)
						defer closer.Close()

						ctx := context.Background()

						_, err := db.ExecContext(ctx, `INSERT INTO users (name, age, address) VALUES ('João Garcia', 0, '{"country":"US"}')`)
						tt.AssertNoErr(t, err)

						_, err = db.ExecContext(ctx, `INSERT INTO users (name, age, address) VALUES ('Bia Garcia', 0, '{"country":"BR"}')`)
						tt.AssertNoErr(t, err)

						c := newTestDB(db, driver)
						var users []*user
						err = c.Query(ctx, &users, variation.queryPrefix+`FROM users WHERE name like `+c.dialect.Placeholder(0), "% Garcia")

						tt.AssertNoErr(t, err)
						tt.AssertEqual(t, len(users), 2)

						tt.AssertNotEqual(t, users[0].ID, uint(0))
						tt.AssertEqual(t, users[0].Name, "João Garcia")
						tt.AssertEqual(t, users[0].Address.Country, "US")

						tt.AssertNotEqual(t, users[1].ID, uint(0))
						tt.AssertEqual(t, users[1].Name, "Bia Garcia")
						tt.AssertEqual(t, users[1].Address.Country, "BR")
					})

					t.Run("should query joined tables correctly", func(t *testing.T) {
						// This test only makes sense with no query prefix
						if variation.queryPrefix != "" {
							return
						}

						db, closer := newDBAdapter(t)
						defer closer.Close()

						ctx := context.Background()

						_, err := db.ExecContext(ctx, `INSERT INTO users (name, age, address) VALUES ('João Ribeiro', 0, '{"country":"US"}')`)
						tt.AssertNoErr(t, err)
						var joao user
						getUserByName(db, driver, &joao, "João Ribeiro")

						_, err = db.ExecContext(ctx, `INSERT INTO users (name, age, address) VALUES ('Bia Ribeiro', 0, '{"country":"BR"}')`)
						tt.AssertNoErr(t, err)
						var bia user
						getUserByName(db, driver, &bia, "Bia Ribeiro")

						_, err = db.ExecContext(ctx, fmt.Sprint(`INSERT INTO posts (user_id, title) VALUES (`, bia.ID, `, 'Bia Post1')`))
						tt.AssertNoErr(t, err)
						_, err = db.ExecContext(ctx, fmt.Sprint(`INSERT INTO posts (user_id, title) VALUES (`, bia.ID, `, 'Bia Post2')`))
						tt.AssertNoErr(t, err)
						_, err = db.ExecContext(ctx, fmt.Sprint(`INSERT INTO posts (user_id, title) VALUES (`, joao.ID, `, 'João Post1')`))
						tt.AssertNoErr(t, err)

						c := newTestDB(db, driver)
						var rows []*struct {
							User user `tablename:"u"`
							Post post `tablename:"p"`
						}
						err = c.Query(ctx, &rows, fmt.Sprint(
							`FROM users u JOIN posts p ON p.user_id = u.id`,
							` WHERE u.name like `, c.dialect.Placeholder(0),
							` ORDER BY u.id, p.id`,
						), "% Ribeiro")

						tt.AssertNoErr(t, err)
						tt.AssertEqual(t, len(rows), 3)

						tt.AssertEqual(t, rows[0].User.ID, joao.ID)
						tt.AssertEqual(t, rows[0].User.Name, "João Ribeiro")
						tt.AssertEqual(t, rows[0].Post.Title, "João Post1")

						tt.AssertEqual(t, rows[1].User.ID, bia.ID)
						tt.AssertEqual(t, rows[1].User.Name, "Bia Ribeiro")
						tt.AssertEqual(t, rows[1].Post.Title, "Bia Post1")

						tt.AssertEqual(t, rows[2].User.ID, bia.ID)
						tt.AssertEqual(t, rows[2].User.Name, "Bia Ribeiro")
						tt.AssertEqual(t, rows[2].Post.Title, "Bia Post2")
					})
				})
			})
		}

		t.Run("testing error cases", func(t *testing.T) {
			err := createTables(driver, connStr)
			if err != nil {
				t.Fatal("could not create test table!, reason:", err.Error())
			}

			t.Run("should report error if input is not a pointer to a slice of structs", func(t *testing.T) {
				db, closer := newDBAdapter(t)
				defer closer.Close()

				ctx := context.Background()

				_, err := db.ExecContext(ctx, `INSERT INTO users (name, age) VALUES ('Andréa Sá', 0)`)
				tt.AssertNoErr(t, err)

				_, err = db.ExecContext(ctx, `INSERT INTO users (name, age) VALUES ('Caio Sá', 0)`)
				tt.AssertNoErr(t, err)

				c := newTestDB(db, driver)
				err = c.Query(ctx, &user{}, `SELECT * FROM users WHERE name like `+c.dialect.Placeholder(0), "% Sá")
				tt.AssertErrContains(t, err, "expected", "to be a slice", "user")

				err = c.Query(ctx, []*user{}, `SELECT * FROM users WHERE name like `+c.dialect.Placeholder(0), "% Sá")
				tt.AssertErrContains(t, err, "expected", "slice of structs", "user")

				var i int
				err = c.Query(ctx, &i, `SELECT * FROM users WHERE name like `+c.dialect.Placeholder(0), "% Sá")
				tt.AssertErrContains(t, err, "expected", "to be a slice", "int")

				err = c.Query(ctx, &[]int{}, `SELECT * FROM users WHERE name like `+c.dialect.Placeholder(0), "% Sá")
				tt.AssertErrContains(t, err, "expected", "slice of structs", "[]int")
			})

			t.Run("should report error if the query is not valid", func(t *testing.T) {
				db, closer := newDBAdapter(t)
				defer closer.Close()

				ctx := context.Background()
				c := newTestDB(db, driver)
				var users []user
				err := c.Query(ctx, &users, `SELECT * FROM not a valid query`)
				tt.AssertErrContains(t, err, "error running query")
			})

			t.Run("should report error if using nested struct and the query starts with SELECT", func(t *testing.T) {
				db, closer := newDBAdapter(t)
				defer closer.Close()

				ctx := context.Background()
				c := newTestDB(db, driver)
				var rows []struct {
					User user `tablename:"users"`
					Post post `tablename:"posts"`
				}
				err := c.Query(ctx, &rows, `SELECT * FROM users u JOIN posts p ON u.id = p.user_id`)
				tt.AssertErrContains(t, err, "nested struct", "feature")
			})

			t.Run("should report error for nested structs with invalid types", func(t *testing.T) {
				t.Run("int", func(t *testing.T) {
					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, driver)
					var rows []struct {
						Foo int `tablename:"foo"`
					}
					err := c.Query(ctx, &rows, fmt.Sprint(
						`FROM users u JOIN posts p ON p.user_id = u.id`,
						` WHERE u.name like `, c.dialect.Placeholder(0),
						` ORDER BY u.id, p.id`,
					), "% Ribeiro")

					tt.AssertErrContains(t, err, "foo", "int")
				})

				t.Run("*struct", func(t *testing.T) {
					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, driver)
					var rows []struct {
						Foo *user `tablename:"foo"`
					}
					err := c.Query(ctx, &rows, fmt.Sprint(
						`FROM users u JOIN posts p ON p.user_id = u.id`,
						` WHERE u.name like `, c.dialect.Placeholder(0),
						` ORDER BY u.id, p.id`,
					), "% Ribeiro")

					tt.AssertErrContains(t, err, "foo", "*ksql.user")
				})
			})

			t.Run("should report error if nested struct is invalid", func(t *testing.T) {
				db, closer := newDBAdapter(t)
				defer closer.Close()

				ctx := context.Background()
				c := newTestDB(db, driver)
				var rows []struct {
					User user `tablename:"users"`
					Post struct {
						Attr1 int `ksql:"invalid_repeated_name"`
						Attr2 int `ksql:"invalid_repeated_name"`
					} `tablename:"posts"`
				}
				err := c.Query(ctx, &rows, `FROM users u JOIN posts p ON u.id = p.user_id`)
				tt.AssertErrContains(t, err, "same ksql tag name", "invalid_repeated_name")
			})
		})
	})
}

// QueryOneTest runs all tests for making sure the QueryOne function is
// working for a given adapter and driver.
func QueryOneTest(
	t *testing.T,
	driver string,
	connStr string,
	newDBAdapter func(t *testing.T) (DBAdapter, io.Closer),
) {
	t.Run("QueryOne", func(t *testing.T) {
		variations := []struct {
			desc        string
			queryPrefix string
		}{
			{
				desc:        "with select *",
				queryPrefix: "SELECT * ",
			},
			{
				desc:        "building the SELECT part of the query internally",
				queryPrefix: "",
			},
		}
		for _, variation := range variations {
			err := createTables(driver, connStr)
			if err != nil {
				t.Fatal("could not create test table!, reason:", err.Error())
			}

			t.Run(variation.desc, func(t *testing.T) {
				t.Run("should return RecordNotFoundErr when there are no results", func(t *testing.T) {
					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, driver)
					u := user{}
					err := c.QueryOne(ctx, &u, variation.queryPrefix+`FROM users WHERE id=1;`)
					tt.AssertEqual(t, err, ErrRecordNotFound)
				})

				t.Run("should return a user correctly", func(t *testing.T) {
					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()

					_, err := db.ExecContext(ctx, `INSERT INTO users (name, age, address) VALUES ('Bia', 0, '{"country":"BR"}')`)
					tt.AssertNoErr(t, err)

					c := newTestDB(db, driver)
					u := user{}
					err = c.QueryOne(ctx, &u, variation.queryPrefix+`FROM users WHERE name=`+c.dialect.Placeholder(0), "Bia")

					tt.AssertNoErr(t, err)
					tt.AssertNotEqual(t, u.ID, uint(0))
					tt.AssertEqual(t, u.Name, "Bia")
					tt.AssertEqual(t, u.Address, address{
						Country: "BR",
					})
				})

				t.Run("should return only the first result on multiples matches", func(t *testing.T) {
					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()

					_, err := db.ExecContext(ctx, `INSERT INTO users (name, age, address) VALUES ('Andréa Sá', 0, '{"country":"US"}')`)
					tt.AssertNoErr(t, err)

					_, err = db.ExecContext(ctx, `INSERT INTO users (name, age, address) VALUES ('Caio Sá', 0, '{"country":"BR"}')`)
					tt.AssertNoErr(t, err)

					c := newTestDB(db, driver)

					var u user
					err = c.QueryOne(ctx, &u, variation.queryPrefix+`FROM users WHERE name like `+c.dialect.Placeholder(0)+` ORDER BY id ASC`, "% Sá")
					tt.AssertNoErr(t, err)
					tt.AssertEqual(t, u.Name, "Andréa Sá")
					tt.AssertEqual(t, u.Age, 0)
					tt.AssertEqual(t, u.Address, address{
						Country: "US",
					})
				})

				t.Run("should query joined tables correctly", func(t *testing.T) {
					// This test only makes sense with no query prefix
					if variation.queryPrefix != "" {
						return
					}

					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()

					_, err := db.ExecContext(ctx, `INSERT INTO users (name, age, address) VALUES ('João Ribeiro', 0, '{"country":"US"}')`)
					tt.AssertNoErr(t, err)
					var joao user
					getUserByName(db, driver, &joao, "João Ribeiro")

					_, err = db.ExecContext(ctx, fmt.Sprint(`INSERT INTO posts (user_id, title) VALUES (`, joao.ID, `, 'João Post1')`))
					tt.AssertNoErr(t, err)

					c := newTestDB(db, driver)
					var row struct {
						User user `tablename:"u"`
						Post post `tablename:"p"`
					}
					err = c.QueryOne(ctx, &row, fmt.Sprint(
						`FROM users u JOIN posts p ON p.user_id = u.id`,
						` WHERE u.name like `, c.dialect.Placeholder(0),
						` ORDER BY u.id, p.id`,
					), "% Ribeiro")

					tt.AssertNoErr(t, err)
					tt.AssertEqual(t, row.User.ID, joao.ID)
					tt.AssertEqual(t, row.User.Name, "João Ribeiro")
					tt.AssertEqual(t, row.Post.Title, "João Post1")
				})

				t.Run("should handle column tags as case-insensitive as SQL does", func(t *testing.T) {
					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					_, err := db.ExecContext(ctx, `INSERT INTO users (name, age, address) VALUES ('Count Olivia', 0, '{"country":"US"}')`)
					tt.AssertNoErr(t, err)

					c := newTestDB(db, driver)

					var row struct {
						Count int `ksql:"myCount"`
					}
					err = c.QueryOne(ctx, &row, `SELECT count(*) as myCount FROM users WHERE name='Count Olivia'`)
					tt.AssertNoErr(t, err)
					tt.AssertEqual(t, row.Count, 1)
				})
			})
		}

		t.Run("should report error if input is not a pointer to struct", func(t *testing.T) {
			db, closer := newDBAdapter(t)
			defer closer.Close()

			ctx := context.Background()

			_, err := db.ExecContext(ctx, `INSERT INTO users (name, age, address) VALUES ('Andréa Sá', 0, '{"country":"US"}')`)
			tt.AssertNoErr(t, err)

			_, err = db.ExecContext(ctx, `INSERT INTO users (name, age, address) VALUES ('Caio Sá', 0, '{"country":"BR"}')`)
			tt.AssertNoErr(t, err)

			c := newTestDB(db, driver)

			err = c.QueryOne(ctx, &[]user{}, `SELECT * FROM users WHERE name like `+c.dialect.Placeholder(0), "% Sá")
			tt.AssertErrContains(t, err, "pointer to struct")

			err = c.QueryOne(ctx, user{}, `SELECT * FROM users WHERE name like `+c.dialect.Placeholder(0), "% Sá")
			tt.AssertErrContains(t, err, "pointer to struct")
		})

		t.Run("should report error if it receives a nil pointer to a struct", func(t *testing.T) {
			db, closer := newDBAdapter(t)
			defer closer.Close()

			ctx := context.Background()
			c := newTestDB(db, driver)
			var u *user
			err := c.QueryOne(ctx, u, `SELECT * FROM users`)
			tt.AssertErrContains(t, err, "expected a valid pointer", "received a nil pointer")
		})

		t.Run("should report error if the query is not valid", func(t *testing.T) {
			db, closer := newDBAdapter(t)
			defer closer.Close()

			ctx := context.Background()
			c := newTestDB(db, driver)
			var u user
			err := c.QueryOne(ctx, &u, `SELECT * FROM not a valid query`)
			tt.AssertErrContains(t, err, "error running query")
		})

		t.Run("should report error if using nested struct and the query starts with SELECT", func(t *testing.T) {
			db, closer := newDBAdapter(t)
			defer closer.Close()

			ctx := context.Background()
			c := newTestDB(db, driver)
			var row struct {
				User user `tablename:"users"`
				Post post `tablename:"posts"`
			}
			err := c.QueryOne(ctx, &row, `SELECT * FROM users u JOIN posts p ON u.id = p.user_id LIMIT 1`)
			tt.AssertErrContains(t, err, "nested struct", "feature")
		})

		t.Run("should report error if a private field has a ksql tag", func(t *testing.T) {
			db, closer := newDBAdapter(t)
			defer closer.Close()

			ctx := context.Background()
			_, err := db.ExecContext(ctx, `INSERT INTO users (name, age, address) VALUES ('Olivia', 0, '{"country":"US"}')`)
			tt.AssertNoErr(t, err)

			c := newTestDB(db, driver)

			var row struct {
				count int `ksql:"my_count"`
			}
			err = c.QueryOne(ctx, &row, `SELECT count(*) as my_count FROM users`)
			tt.AssertErrContains(t, err, "unexported", "my_count")
		})
	})
}

// InsertTest runs all tests for making sure the Insert function is
// working for a given adapter and driver.
func InsertTest(
	t *testing.T,
	driver string,
	connStr string,
	newDBAdapter func(t *testing.T) (DBAdapter, io.Closer),
) {
	t.Run("Insert", func(t *testing.T) {
		t.Run("success cases", func(t *testing.T) {
			t.Run("single primary key tables", func(t *testing.T) {
				err := createTables(driver, connStr)
				if err != nil {
					t.Fatal("could not create test table!, reason:", err.Error())
				}

				t.Run("should insert one user correctly", func(t *testing.T) {
					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, driver)

					u := user{
						Name: "Fernanda",
						Address: address{
							Country: "Brazil",
						},
					}

					err := c.Insert(ctx, usersTable, &u)
					tt.AssertNoErr(t, err)
					tt.AssertNotEqual(t, u.ID, 0)

					result := user{}
					err = getUserByID(c.db, c.dialect, &result, u.ID)
					tt.AssertNoErr(t, err)

					tt.AssertEqual(t, result.Name, u.Name)
					tt.AssertEqual(t, result.Address, u.Address)
				})

				t.Run("should insert ignoring the ID with multiple ids", func(t *testing.T) {
					if supportedDialects[driver].InsertMethod() != insertWithLastInsertID {
						return
					}

					// Using columns "id" and "name" as IDs:
					table := NewTable("users", "id", "name")

					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, driver)

					u := user{
						Name: "No ID returned",
						Age:  3434, // Random number to avoid false positives on this test

						Address: address{
							Country: "Brazil 3434",
						},
					}

					err = c.Insert(ctx, table, &u)
					tt.AssertNoErr(t, err)
					tt.AssertEqual(t, u.ID, uint(0))

					result := user{}
					err = getUserByName(c.db, driver, &result, "No ID returned")
					tt.AssertNoErr(t, err)

					tt.AssertEqual(t, result.Age, u.Age)
					tt.AssertEqual(t, result.Address, u.Address)
				})

				t.Run("should work with anonymous structs", func(t *testing.T) {
					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, driver)
					err = c.Insert(ctx, usersTable, &struct {
						ID      int                    `ksql:"id"`
						Name    string                 `ksql:"name"`
						Address map[string]interface{} `ksql:"address,json"`
					}{Name: "fake-name", Address: map[string]interface{}{"city": "bar"}})
					tt.AssertNoErr(t, err)
				})

				t.Run("should work with preset IDs", func(t *testing.T) {
					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, driver)

					usersByName := NewTable("users", "name")

					err = c.Insert(ctx, usersByName, &struct {
						Name string `ksql:"name"`
						Age  int    `ksql:"age"`
					}{Name: "Preset Name", Age: 5455})
					tt.AssertNoErr(t, err)

					var inserted user
					err := getUserByName(db, driver, &inserted, "Preset Name")
					tt.AssertNoErr(t, err)
					tt.AssertEqual(t, inserted.Age, 5455)
				})
			})

			t.Run("composite key tables", func(t *testing.T) {
				err := createTables(driver, connStr)
				if err != nil {
					t.Fatal("could not create test table!, reason:", err.Error())
				}

				t.Run("should insert in composite key tables correctly", func(t *testing.T) {
					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, driver)

					table := NewTable("user_permissions", "id", "user_id", "perm_id")
					err = c.Insert(ctx, table, &userPermission{
						UserID: 1,
						PermID: 42,
					})
					tt.AssertNoErr(t, err)

					userPerms, err := getUserPermissionsByUser(db, driver, 1)
					tt.AssertNoErr(t, err)
					tt.AssertEqual(t, len(userPerms), 1)
					tt.AssertEqual(t, userPerms[0].UserID, 1)
					tt.AssertEqual(t, userPerms[0].PermID, 42)
				})

				t.Run("should accept partially provided values for composite key tables", func(t *testing.T) {
					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, driver)

					// Table defined with 3 values, but we'll provide only 2,
					// the third will be generated for the purposes of this test:
					table := NewTable("user_permissions", "id", "user_id", "perm_id")
					permission := userPermission{
						UserID: 2,
						PermID: 42,
					}
					err = c.Insert(ctx, table, &permission)
					tt.AssertNoErr(t, err)

					userPerms, err := getUserPermissionsByUser(db, driver, 2)
					tt.AssertNoErr(t, err)

					// Should retrieve the generated ID from the database,
					// only if the database supports returning multiple values:
					switch c.dialect.InsertMethod() {
					case insertWithNoIDRetrieval, insertWithLastInsertID:
						tt.AssertEqual(t, permission.ID, 0)
						tt.AssertEqual(t, len(userPerms), 1)
						tt.AssertEqual(t, userPerms[0].UserID, 2)
						tt.AssertEqual(t, userPerms[0].PermID, 42)
					case insertWithReturning, insertWithOutput:
						tt.AssertNotEqual(t, permission.ID, 0)
						tt.AssertEqual(t, len(userPerms), 1)
						tt.AssertEqual(t, userPerms[0].ID, permission.ID)
						tt.AssertEqual(t, userPerms[0].UserID, 2)
						tt.AssertEqual(t, userPerms[0].PermID, 42)
					}
				})
			})
		})

		t.Run("testing error cases", func(t *testing.T) {
			err := createTables(driver, connStr)
			if err != nil {
				t.Fatal("could not create test table!, reason:", err.Error())
			}

			t.Run("should report error for invalid input types", func(t *testing.T) {
				db, closer := newDBAdapter(t)
				defer closer.Close()

				ctx := context.Background()
				c := newTestDB(db, driver)

				err = c.Insert(ctx, usersTable, "foo")
				tt.AssertNotEqual(t, err, nil)

				err = c.Insert(ctx, usersTable, nullable.String("foo"))
				tt.AssertNotEqual(t, err, nil)

				err = c.Insert(ctx, usersTable, map[string]interface{}{
					"name": "foo",
					"age":  12,
				})
				tt.AssertNotEqual(t, err, nil)

				cantInsertSlice := []interface{}{
					&user{Name: "foo", Age: 22},
					&user{Name: "bar", Age: 32},
				}
				err = c.Insert(ctx, usersTable, cantInsertSlice)
				tt.AssertNotEqual(t, err, nil)

				// We might want to support this in the future, but not for now:
				err = c.Insert(ctx, usersTable, user{Name: "not a ptr to user", Age: 42})
				tt.AssertNotEqual(t, err, nil)
			})

			t.Run("should report error if for some reason the insertMethod is invalid", func(t *testing.T) {
				db, closer := newDBAdapter(t)
				defer closer.Close()

				ctx := context.Background()
				c := newTestDB(db, driver)

				// This is an invalid value:
				c.dialect = brokenDialect{}

				err = c.Insert(ctx, usersTable, &user{Name: "foo"})
				tt.AssertNotEqual(t, err, nil)
			})

			t.Run("should report error if it receives a nil pointer to a struct", func(t *testing.T) {
				db, closer := newDBAdapter(t)
				defer closer.Close()

				ctx := context.Background()
				c := newTestDB(db, driver)

				var u *user
				err := c.Insert(ctx, usersTable, u)
				tt.AssertNotEqual(t, err, nil)
			})

			t.Run("should report error if table contains an empty ID name", func(t *testing.T) {
				db, closer := newDBAdapter(t)
				defer closer.Close()

				ctx := context.Background()
				c := newTestDB(db, driver)

				err := c.Insert(ctx, NewTable("users", ""), &user{Name: "fake-name"})
				tt.AssertErrContains(t, err, "ksql.Table", "ID", "empty string")
			})

			t.Run("should report error if ksql.Table.name is empty", func(t *testing.T) {
				db, closer := newDBAdapter(t)
				defer closer.Close()

				ctx := context.Background()
				c := newTestDB(db, driver)

				err := c.Insert(ctx, NewTable("", "id"), &user{Name: "fake-name"})
				tt.AssertErrContains(t, err, "ksql.Table", "table name", "empty string")
			})

			t.Run("should not panic if a column doesn't exist in the database", func(t *testing.T) {
				db, closer := newDBAdapter(t)
				defer closer.Close()

				ctx := context.Background()
				c := newTestDB(db, driver)

				err = c.Insert(ctx, usersTable, &struct {
					ID                string `ksql:"id"`
					NonExistingColumn int    `ksql:"non_existing"`
					Name              string `ksql:"name"`
				}{NonExistingColumn: 42, Name: "fake-name"})

				tt.AssertErrContains(t, err, "column", "non_existing")
			})

			t.Run("should not panic if the ID column doesn't exist in the database", func(t *testing.T) {
				db, closer := newDBAdapter(t)
				defer closer.Close()

				ctx := context.Background()
				c := newTestDB(db, driver)

				brokenTable := NewTable("users", "non_existing_id")

				_ = c.Insert(ctx, brokenTable, &struct {
					ID   string `ksql:"non_existing_id"`
					Age  int    `ksql:"age"`
					Name string `ksql:"name"`
				}{Age: 42, Name: "fake-name"})
			})

			t.Run("should not panic if the ID column is missing in the struct", func(t *testing.T) {
				db, closer := newDBAdapter(t)
				defer closer.Close()

				ctx := context.Background()
				c := newTestDB(db, driver)

				err = c.Insert(ctx, usersTable, &struct {
					Age  int    `ksql:"age"`
					Name string `ksql:"name"`
				}{Age: 42, Name: "Inserted With no ID"})
				tt.AssertNoErr(t, err)

				var u user
				err = getUserByName(db, driver, &u, "Inserted With no ID")
				tt.AssertNoErr(t, err)
				tt.AssertNotEqual(t, u.ID, uint(0))
				tt.AssertEqual(t, u.Age, 42)
			})
		})
	})
}

type brokenDialect struct{}

func (brokenDialect) InsertMethod() insertMethod {
	return insertMethod(42)
}

func (brokenDialect) Escape(str string) string {
	return str
}

func (brokenDialect) Placeholder(idx int) string {
	return "?"
}

func (brokenDialect) DriverName() string {
	return "fake-driver-name"
}

// DeleteTest runs all tests for making sure the Delete function is
// working for a given adapter and driver.
func DeleteTest(
	t *testing.T,
	driver string,
	connStr string,
	newDBAdapter func(t *testing.T) (DBAdapter, io.Closer),
) {
	t.Run("Delete", func(t *testing.T) {
		err := createTables(driver, connStr)
		if err != nil {
			t.Fatal("could not create test table!, reason:", err.Error())
		}

		t.Run("should delete from tables with a single primary key correctly", func(t *testing.T) {
			tests := []struct {
				desc               string
				deletionKeyForUser func(u user) interface{}
			}{
				{
					desc: "passing only the ID as key",
					deletionKeyForUser: func(u user) interface{} {
						return u.ID
					},
				},
				{
					desc: "passing only the entire user",
					deletionKeyForUser: func(u user) interface{} {
						return u
					},
				},
				{
					desc: "passing the address of the user",
					deletionKeyForUser: func(u user) interface{} {
						return &u
					},
				},
			}

			for _, test := range tests {
				t.Run(test.desc, func(t *testing.T) {
					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, driver)

					u1 := user{
						Name: "Fernanda",
					}

					err := c.Insert(ctx, usersTable, &u1)
					tt.AssertNoErr(t, err)
					tt.AssertNotEqual(t, u1.ID, uint(0))

					result := user{}
					err = getUserByID(c.db, c.dialect, &result, u1.ID)
					tt.AssertNoErr(t, err)
					tt.AssertEqual(t, result.ID, u1.ID)

					u2 := user{
						Name: "Won't be deleted",
					}

					err = c.Insert(ctx, usersTable, &u2)
					tt.AssertNoErr(t, err)
					tt.AssertNotEqual(t, u2.ID, uint(0))

					result = user{}
					err = getUserByID(c.db, c.dialect, &result, u2.ID)
					tt.AssertNoErr(t, err)
					tt.AssertEqual(t, result.ID, u2.ID)

					err = c.Delete(ctx, usersTable, test.deletionKeyForUser(u1))
					tt.AssertNoErr(t, err)

					result = user{}
					err = getUserByID(c.db, c.dialect, &result, u1.ID)
					tt.AssertEqual(t, err, sql.ErrNoRows)

					result = user{}
					err = getUserByID(c.db, c.dialect, &result, u2.ID)
					tt.AssertNoErr(t, err)

					tt.AssertNotEqual(t, result.ID, uint(0))
					tt.AssertEqual(t, result.Name, "Won't be deleted")
				})
			}
		})

		t.Run("should delete from tables with composite primary keys correctly", func(t *testing.T) {
			t.Run("using structs", func(t *testing.T) {
				db, closer := newDBAdapter(t)
				defer closer.Close()

				ctx := context.Background()
				c := newTestDB(db, driver)

				// This permission should not be deleted, we'll use the id to check it:
				p0 := userPermission{
					UserID: 1,
					PermID: 44,
				}
				err = createUserPermission(db, c.dialect, p0)
				tt.AssertNoErr(t, err)

				p0, err = getUserPermissionBySecondaryKeys(db, c.dialect, 1, 44)
				tt.AssertNoErr(t, err)
				tt.AssertNotEqual(t, p0.ID, 0)

				p1 := userPermission{
					UserID: 1,
					PermID: 42,
				}
				err = createUserPermission(db, c.dialect, p1)
				tt.AssertNoErr(t, err)

				err = c.Delete(ctx, userPermissionsTable, p1)
				tt.AssertNoErr(t, err)

				userPerms, err := getUserPermissionsByUser(db, driver, 1)
				tt.AssertNoErr(t, err)
				tt.AssertEqual(t, len(userPerms), 1)
				tt.AssertEqual(t, userPerms[0].UserID, 1)
				tt.AssertEqual(t, userPerms[0].PermID, 44)
			})

			t.Run("using maps", func(t *testing.T) {
				db, closer := newDBAdapter(t)
				defer closer.Close()

				ctx := context.Background()
				c := newTestDB(db, driver)

				// This permission should not be deleted, we'll use the id to check it:
				p0 := userPermission{
					UserID: 2,
					PermID: 44,
				}
				err = createUserPermission(db, c.dialect, p0)
				tt.AssertNoErr(t, err)

				p0, err = getUserPermissionBySecondaryKeys(db, c.dialect, 1, 44)
				tt.AssertNoErr(t, err)
				tt.AssertNotEqual(t, p0.ID, 0)

				p1 := userPermission{
					UserID: 2,
					PermID: 42,
				}
				err = createUserPermission(db, c.dialect, p1)
				tt.AssertNoErr(t, err)

				err = c.Delete(ctx, userPermissionsTable, map[string]interface{}{
					"user_id": 2,
					"perm_id": 42,
				})
				tt.AssertNoErr(t, err)

				userPerms, err := getUserPermissionsByUser(db, driver, 2)
				tt.AssertNoErr(t, err)
				tt.AssertEqual(t, len(userPerms), 1)
				tt.AssertEqual(t, userPerms[0].UserID, 2)
				tt.AssertEqual(t, userPerms[0].PermID, 44)
			})
		})

		t.Run("should return ErrRecordNotFound if no rows were deleted", func(t *testing.T) {
			db, closer := newDBAdapter(t)
			defer closer.Close()

			ctx := context.Background()
			c := newTestDB(db, driver)

			err = c.Delete(ctx, usersTable, 4200)
			tt.AssertEqual(t, err, ErrRecordNotFound)
		})

		t.Run("should report error if it receives a nil pointer to a struct", func(t *testing.T) {
			db, closer := newDBAdapter(t)
			defer closer.Close()

			ctx := context.Background()
			c := newTestDB(db, driver)

			var u *user
			err := c.Delete(ctx, usersTable, u)
			tt.AssertNotEqual(t, err, nil)
		})

		t.Run("should report error if one of the ids is missing from the input", func(t *testing.T) {
			t.Run("single id", func(t *testing.T) {
				t.Run("struct with missing attr", func(t *testing.T) {
					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, driver)

					err := c.Delete(ctx, NewTable("users", "id"), &struct {
						// Missing ID
						Name string `ksql:"name"`
					}{Name: "fake-name"})
					tt.AssertErrContains(t, err, "missing required", "id")
				})

				t.Run("struct with NULL attr", func(t *testing.T) {
					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, driver)

					err := c.Delete(ctx, NewTable("users", "id"), &struct {
						// Null ID
						ID   *int   `ksql:"id"`
						Name string `ksql:"name"`
					}{Name: "fake-name"})
					tt.AssertErrContains(t, err, "missing required", "id")
				})

				t.Run("struct with zero attr", func(t *testing.T) {
					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, driver)

					err := c.Delete(ctx, NewTable("users", "id"), &struct {
						// Uninitialized ID
						ID   int    `ksql:"id"`
						Name string `ksql:"name"`
					}{Name: "fake-name"})
					tt.AssertErrContains(t, err, "invalid value", "0", "id")
				})
			})

			t.Run("multiple ids", func(t *testing.T) {
				t.Run("struct with missing attr", func(t *testing.T) {
					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, driver)

					err := c.Delete(ctx, userPermissionsTable, &struct {
						// Missing PermID
						UserID int    `ksql:"user_id"`
						Name   string `ksql:"name"`
					}{
						UserID: 1,
						Name:   "fake-name",
					})
					tt.AssertErrContains(t, err, "missing required", "perm_id")
				})

				t.Run("map with missing attr", func(t *testing.T) {
					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, driver)

					err := c.Delete(ctx, userPermissionsTable, map[string]interface{}{
						// Missing PermID
						"user_id": 1,
						"name":    "fake-name",
					})
					tt.AssertErrContains(t, err, "missing required", "perm_id")
				})

				t.Run("struct with NULL attr", func(t *testing.T) {
					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, driver)

					err := c.Delete(ctx, userPermissionsTable, &struct {
						UserID int    `ksql:"user_id"`
						PermID *int   `ksql:"perm_id"`
						Name   string `ksql:"name"`
					}{
						// Null Perm ID
						UserID: 1,
						PermID: nil,
						Name:   "fake-name",
					})
					tt.AssertErrContains(t, err, "missing required", "perm_id")
				})

				t.Run("map with NULL attr", func(t *testing.T) {
					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, driver)

					err := c.Delete(ctx, userPermissionsTable, map[string]interface{}{
						// Null Perm ID
						"user_id": 1,
						"perm_id": nil,
						"name":    "fake-name",
					})
					tt.AssertErrContains(t, err, "invalid value", "nil", "perm_id")
				})

				t.Run("struct with zero attr", func(t *testing.T) {
					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, driver)

					err := c.Delete(ctx, userPermissionsTable, &struct {
						UserID int    `ksql:"user_id"`
						PermID int    `ksql:"perm_id"`
						Name   string `ksql:"name"`
					}{
						// Zero Perm ID
						UserID: 1,
						PermID: 0,
						Name:   "fake-name",
					})
					tt.AssertErrContains(t, err, "invalid value", "0", "perm_id")
				})

				t.Run("map with zero attr", func(t *testing.T) {
					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, driver)

					err := c.Delete(ctx, userPermissionsTable, map[string]interface{}{
						// Zero Perm ID
						"user_id": 1,
						"perm_id": 0,
						"name":    "fake-name",
					})
					tt.AssertErrContains(t, err, "invalid value", "0", "perm_id")
				})
			})
		})

		t.Run("should report error if table contains an empty ID name", func(t *testing.T) {
			db, closer := newDBAdapter(t)
			defer closer.Close()

			ctx := context.Background()
			c := newTestDB(db, driver)

			err := c.Delete(ctx, NewTable("users", ""), &user{ID: 42, Name: "fake-name"})
			tt.AssertErrContains(t, err, "ksql.Table", "ID", "empty string")
		})

		t.Run("should report error if ksql.Table.name is empty", func(t *testing.T) {
			db, closer := newDBAdapter(t)
			defer closer.Close()

			ctx := context.Background()
			c := newTestDB(db, driver)

			err := c.Delete(ctx, NewTable("", "id"), &user{Name: "fake-name"})
			tt.AssertErrContains(t, err, "ksql.Table", "table name", "empty string")
		})
	})
}

// PatchTest runs all tests for making sure the Patch function is
// working for a given adapter and driver.
func PatchTest(
	t *testing.T,
	driver string,
	connStr string,
	newDBAdapter func(t *testing.T) (DBAdapter, io.Closer),
) {
	t.Run("Patch", func(t *testing.T) {
		err := createTables(driver, connStr)
		if err != nil {
			t.Fatal("could not create test table!, reason:", err.Error())
		}

		t.Run("should update one user{} correctly", func(t *testing.T) {
			db, closer := newDBAdapter(t)
			defer closer.Close()

			ctx := context.Background()
			c := newTestDB(db, driver)

			u := user{
				Name: "Letícia",
			}
			_, err := db.ExecContext(ctx, `INSERT INTO users (name, age) VALUES ('Letícia', 0)`)
			tt.AssertNoErr(t, err)

			err = getUserByName(db, driver, &u, "Letícia")
			tt.AssertNoErr(t, err)
			tt.AssertNotEqual(t, u.ID, uint(0))

			err = c.Update(ctx, usersTable, user{
				ID:   u.ID,
				Name: "Thayane",
			})
			tt.AssertNoErr(t, err)

			var result user
			err = getUserByID(c.db, c.dialect, &result, u.ID)
			tt.AssertNoErr(t, err)
			tt.AssertEqual(t, result.Name, "Thayane")
		})

		t.Run("should update one &user{} correctly", func(t *testing.T) {
			db, closer := newDBAdapter(t)
			defer closer.Close()

			ctx := context.Background()
			c := newTestDB(db, driver)

			u := user{
				Name: "Letícia",
			}
			_, err := db.ExecContext(ctx, `INSERT INTO users (name, age) VALUES ('Letícia', 0)`)
			tt.AssertNoErr(t, err)

			err = getUserByName(db, driver, &u, "Letícia")
			tt.AssertNoErr(t, err)
			tt.AssertNotEqual(t, u.ID, uint(0))

			err = c.Update(ctx, usersTable, &user{
				ID:   u.ID,
				Name: "Thayane",
			})
			tt.AssertNoErr(t, err)

			var result user
			err = getUserByID(c.db, c.dialect, &result, u.ID)
			tt.AssertNoErr(t, err)
			tt.AssertEqual(t, result.Name, "Thayane")
		})

		t.Run("should update tables with composite keys correctly", func(t *testing.T) {
			db, closer := newDBAdapter(t)
			defer closer.Close()

			ctx := context.Background()
			c := newTestDB(db, driver)

			err = createUserPermission(db, c.dialect, userPermission{
				UserID: 42,
				PermID: 43,
				Type:   "existingFakeType",
			})
			tt.AssertNoErr(t, err)

			existingPerm, err := getUserPermissionBySecondaryKeys(db, c.dialect, 42, 43)
			tt.AssertNoErr(t, err)
			tt.AssertNotEqual(t, existingPerm.ID, 0)
			tt.AssertEqual(t, existingPerm.Type, "existingFakeType")

			err = c.Update(ctx, NewTable("user_permissions", "id", "user_id", "perm_id"), &userPermission{
				ID:     existingPerm.ID,
				UserID: 42,
				PermID: 43,
				Type:   "newFakeType",
			})
			tt.AssertNoErr(t, err)

			newPerm, err := getUserPermissionBySecondaryKeys(db, c.dialect, 42, 43)
			tt.AssertNoErr(t, err)
			tt.AssertEqual(t, newPerm, userPermission{
				ID:     existingPerm.ID,
				UserID: 42,
				PermID: 43,
				Type:   "newFakeType",
			})
		})

		t.Run("should ignore null pointers on partial updates", func(t *testing.T) {
			db, closer := newDBAdapter(t)
			defer closer.Close()

			ctx := context.Background()
			c := newTestDB(db, driver)

			type partialUser struct {
				ID   uint   `ksql:"id"`
				Name string `ksql:"name"`
				Age  *int   `ksql:"age"`
			}

			_, err := db.ExecContext(ctx, `INSERT INTO users (name, age) VALUES ('Letícia', 22)`)
			tt.AssertNoErr(t, err)

			var u user
			err = getUserByName(db, driver, &u, "Letícia")
			tt.AssertNoErr(t, err)
			tt.AssertNotEqual(t, u.ID, uint(0))

			err = c.Update(ctx, usersTable, partialUser{
				ID: u.ID,
				// Should be updated because it is not null, just empty:
				Name: "",
				// Should not be updated because it is null:
				Age: nil,
			})
			tt.AssertNoErr(t, err)

			var result user
			err = getUserByID(c.db, c.dialect, &result, u.ID)
			tt.AssertNoErr(t, err)
			tt.AssertEqual(t, result.Name, "")
			tt.AssertEqual(t, result.Age, 22)
		})

		t.Run("should update valid pointers on partial updates", func(t *testing.T) {
			db, closer := newDBAdapter(t)
			defer closer.Close()

			ctx := context.Background()
			c := newTestDB(db, driver)

			type partialUser struct {
				ID   uint   `ksql:"id"`
				Name string `ksql:"name"`
				Age  *int   `ksql:"age"`
			}

			_, err := db.ExecContext(ctx, `INSERT INTO users (name, age) VALUES ('Letícia', 22)`)
			tt.AssertNoErr(t, err)

			var u user
			err = getUserByName(db, driver, &u, "Letícia")
			tt.AssertNoErr(t, err)
			tt.AssertNotEqual(t, u.ID, uint(0))

			// Should update all fields:
			err = c.Update(ctx, usersTable, partialUser{
				ID:   u.ID,
				Name: "Thay",
				Age:  nullable.Int(42),
			})
			tt.AssertNoErr(t, err)

			var result user
			err = getUserByID(c.db, c.dialect, &result, u.ID)
			tt.AssertNoErr(t, err)

			tt.AssertEqual(t, result.Name, "Thay")
			tt.AssertEqual(t, result.Age, 42)
		})

		t.Run("should return ErrRecordNotFound when asked to update an inexistent user", func(t *testing.T) {
			db, closer := newDBAdapter(t)
			defer closer.Close()

			ctx := context.Background()
			c := newTestDB(db, driver)

			err = c.Update(ctx, usersTable, user{
				ID:   4200,
				Name: "Thayane",
			})
			tt.AssertEqual(t, err, ErrRecordNotFound)
		})

		t.Run("should report database errors correctly", func(t *testing.T) {
			db, closer := newDBAdapter(t)
			defer closer.Close()

			ctx := context.Background()
			c := newTestDB(db, driver)

			err = c.Update(ctx, NewTable("non_existing_table"), user{
				ID:   1,
				Name: "Thayane",
			})
			tt.AssertNotEqual(t, err, nil)
		})

		t.Run("should report error if it receives a nil pointer to a struct", func(t *testing.T) {
			db, closer := newDBAdapter(t)
			defer closer.Close()

			ctx := context.Background()
			c := newTestDB(db, driver)

			var u *user
			err := c.Update(ctx, usersTable, u)
			tt.AssertNotEqual(t, err, nil)
		})

		t.Run("should report error if the id is missing", func(t *testing.T) {
			t.Run("with a single primary key", func(t *testing.T) {
				db, closer := newDBAdapter(t)
				defer closer.Close()

				ctx := context.Background()
				c := newTestDB(db, driver)

				err := c.Update(ctx, usersTable, &user{
					// Missing ID
					Name: "Jane",
				})
				tt.AssertErrContains(t, err, "invalid value", "0", "'id'")
			})

			t.Run("with composite keys", func(t *testing.T) {
				db, closer := newDBAdapter(t)
				defer closer.Close()

				ctx := context.Background()
				c := newTestDB(db, driver)

				err := c.Update(ctx, NewTable("user_permissions", "id", "user_id", "perm_id"), &userPermission{
					ID: 1,
					// Missing UserID
					PermID: 42,
				})
				tt.AssertErrContains(t, err, "invalid value", "0", "'user_id'")
			})
		})
	})
}

// QueryChunksTest runs all tests for making sure the QueryChunks function is
// working for a given adapter and driver.
func QueryChunksTest(
	t *testing.T,
	driver string,
	connStr string,
	newDBAdapter func(t *testing.T) (DBAdapter, io.Closer),
) {
	t.Run("QueryChunks", func(t *testing.T) {
		variations := []struct {
			desc        string
			queryPrefix string
		}{
			{
				desc:        "with select *",
				queryPrefix: "SELECT * ",
			},
			{
				desc:        "building the SELECT part of the query internally",
				queryPrefix: "",
			},
		}
		for _, variation := range variations {
			t.Run(variation.desc, func(t *testing.T) {
				t.Run("should query a single row correctly", func(t *testing.T) {
					err := createTables(driver, connStr)
					if err != nil {
						t.Fatal("could not create test table!, reason:", err.Error())
					}

					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, driver)

					_ = c.Insert(ctx, usersTable, &user{
						Name:    "User1",
						Address: address{Country: "BR"},
					})

					var length int
					var u user
					err = c.QueryChunks(ctx, ChunkParser{
						Query:  variation.queryPrefix + `FROM users WHERE name = ` + c.dialect.Placeholder(0),
						Params: []interface{}{"User1"},

						ChunkSize: 100,
						ForEachChunk: func(users []user) error {
							length = len(users)
							if length > 0 {
								u = users[0]
							}
							return nil
						},
					})

					tt.AssertNoErr(t, err)
					tt.AssertEqual(t, length, 1)
					tt.AssertNotEqual(t, u.ID, uint(0))
					tt.AssertEqual(t, u.Name, "User1")
					tt.AssertEqual(t, u.Address.Country, "BR")
				})

				t.Run("should query one chunk correctly", func(t *testing.T) {
					err := createTables(driver, connStr)
					if err != nil {
						t.Fatal("could not create test table!, reason:", err.Error())
					}

					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, driver)

					_ = c.Insert(ctx, usersTable, &user{Name: "User1", Address: address{Country: "US"}})
					_ = c.Insert(ctx, usersTable, &user{Name: "User2", Address: address{Country: "BR"}})

					var lengths []int
					var users []user
					err = c.QueryChunks(ctx, ChunkParser{
						Query:  variation.queryPrefix + `from users where name like ` + c.dialect.Placeholder(0) + ` order by name asc;`,
						Params: []interface{}{"User%"},

						ChunkSize: 2,
						ForEachChunk: func(buffer []user) error {
							users = append(users, buffer...)
							lengths = append(lengths, len(buffer))
							return nil
						},
					})

					tt.AssertNoErr(t, err)
					tt.AssertEqual(t, len(lengths), 1)
					tt.AssertEqual(t, lengths[0], 2)

					tt.AssertNotEqual(t, users[0].ID, uint(0))
					tt.AssertEqual(t, users[0].Name, "User1")
					tt.AssertEqual(t, users[0].Address.Country, "US")

					tt.AssertNotEqual(t, users[1].ID, uint(0))
					tt.AssertEqual(t, users[1].Name, "User2")
					tt.AssertEqual(t, users[1].Address.Country, "BR")
				})

				t.Run("should query chunks of 1 correctly", func(t *testing.T) {
					err := createTables(driver, connStr)
					if err != nil {
						t.Fatal("could not create test table!, reason:", err.Error())
					}

					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, driver)

					_ = c.Insert(ctx, usersTable, &user{Name: "User1", Address: address{Country: "US"}})
					_ = c.Insert(ctx, usersTable, &user{Name: "User2", Address: address{Country: "BR"}})

					var lengths []int
					var users []user
					err = c.QueryChunks(ctx, ChunkParser{
						Query:  variation.queryPrefix + `from users where name like ` + c.dialect.Placeholder(0) + ` order by name asc;`,
						Params: []interface{}{"User%"},

						ChunkSize: 1,
						ForEachChunk: func(buffer []user) error {
							lengths = append(lengths, len(buffer))
							users = append(users, buffer...)
							return nil
						},
					})

					tt.AssertNoErr(t, err)
					tt.AssertEqual(t, len(users), 2)
					tt.AssertEqual(t, lengths, []int{1, 1})

					tt.AssertNotEqual(t, users[0].ID, uint(0))
					tt.AssertEqual(t, users[0].Name, "User1")
					tt.AssertEqual(t, users[0].Address.Country, "US")

					tt.AssertNotEqual(t, users[1].ID, uint(0))
					tt.AssertEqual(t, users[1].Name, "User2")
					tt.AssertEqual(t, users[1].Address.Country, "BR")
				})

				t.Run("should load partially filled chunks correctly", func(t *testing.T) {
					err := createTables(driver, connStr)
					if err != nil {
						t.Fatal("could not create test table!, reason:", err.Error())
					}

					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, driver)

					_ = c.Insert(ctx, usersTable, &user{Name: "User1"})
					_ = c.Insert(ctx, usersTable, &user{Name: "User2"})
					_ = c.Insert(ctx, usersTable, &user{Name: "User3"})

					var lengths []int
					var users []user
					err = c.QueryChunks(ctx, ChunkParser{
						Query:  variation.queryPrefix + `from users where name like ` + c.dialect.Placeholder(0) + ` order by name asc;`,
						Params: []interface{}{"User%"},

						ChunkSize: 2,
						ForEachChunk: func(buffer []user) error {
							lengths = append(lengths, len(buffer))
							users = append(users, buffer...)
							return nil
						},
					})

					tt.AssertNoErr(t, err)
					tt.AssertEqual(t, len(users), 3)
					tt.AssertNotEqual(t, users[0].ID, uint(0))
					tt.AssertEqual(t, users[0].Name, "User1")
					tt.AssertNotEqual(t, users[1].ID, uint(0))
					tt.AssertEqual(t, users[1].Name, "User2")
					tt.AssertNotEqual(t, users[2].ID, uint(0))
					tt.AssertEqual(t, users[2].Name, "User3")
					tt.AssertEqual(t, lengths, []int{2, 1})
				})

				// xxx
				t.Run("should query joined tables correctly", func(t *testing.T) {
					// This test only makes sense with no query prefix
					if variation.queryPrefix != "" {
						return
					}

					db, closer := newDBAdapter(t)
					defer closer.Close()

					joao := user{
						Name: "Thiago Ribeiro",
						Age:  24,
					}
					thatiana := user{
						Name: "Thatiana Ribeiro",
						Age:  20,
					}

					ctx := context.Background()
					c := newTestDB(db, driver)
					_ = c.Insert(ctx, usersTable, &joao)
					_ = c.Insert(ctx, usersTable, &thatiana)

					_, err := db.ExecContext(ctx, fmt.Sprint(`INSERT INTO posts (user_id, title) VALUES (`, thatiana.ID, `, 'Thatiana Post1')`))
					tt.AssertNoErr(t, err)
					_, err = db.ExecContext(ctx, fmt.Sprint(`INSERT INTO posts (user_id, title) VALUES (`, thatiana.ID, `, 'Thatiana Post2')`))
					tt.AssertNoErr(t, err)
					_, err = db.ExecContext(ctx, fmt.Sprint(`INSERT INTO posts (user_id, title) VALUES (`, joao.ID, `, 'Thiago Post1')`))
					tt.AssertNoErr(t, err)

					var lengths []int
					var users []user
					var posts []post
					err = c.QueryChunks(ctx, ChunkParser{
						Query: fmt.Sprint(
							`FROM users u JOIN posts p ON p.user_id = u.id`,
							` WHERE u.name like `, c.dialect.Placeholder(0),
							` ORDER BY u.id, p.id`,
						),
						Params: []interface{}{"% Ribeiro"},

						ChunkSize: 2,
						ForEachChunk: func(chunk []struct {
							User user `tablename:"u"`
							Post post `tablename:"p"`
						}) error {
							lengths = append(lengths, len(chunk))
							for _, row := range chunk {
								users = append(users, row.User)
								posts = append(posts, row.Post)
							}
							return nil
						},
					})

					tt.AssertNoErr(t, err)
					tt.AssertEqual(t, len(posts), 3)

					tt.AssertEqual(t, users[0].ID, joao.ID)
					tt.AssertEqual(t, users[0].Name, "Thiago Ribeiro")
					tt.AssertEqual(t, posts[0].Title, "Thiago Post1")

					tt.AssertEqual(t, users[1].ID, thatiana.ID)
					tt.AssertEqual(t, users[1].Name, "Thatiana Ribeiro")
					tt.AssertEqual(t, posts[1].Title, "Thatiana Post1")

					tt.AssertEqual(t, users[2].ID, thatiana.ID)
					tt.AssertEqual(t, users[2].Name, "Thatiana Ribeiro")
					tt.AssertEqual(t, posts[2].Title, "Thatiana Post2")
				})

				t.Run("should abort the first iteration when the callback returns an ErrAbortIteration", func(t *testing.T) {
					err := createTables(driver, connStr)
					if err != nil {
						t.Fatal("could not create test table!, reason:", err.Error())
					}

					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, driver)

					_ = c.Insert(ctx, usersTable, &user{Name: "User1"})
					_ = c.Insert(ctx, usersTable, &user{Name: "User2"})
					_ = c.Insert(ctx, usersTable, &user{Name: "User3"})

					var lengths []int
					var users []user
					err = c.QueryChunks(ctx, ChunkParser{
						Query:  variation.queryPrefix + `from users where name like ` + c.dialect.Placeholder(0) + ` order by name asc;`,
						Params: []interface{}{"User%"},

						ChunkSize: 2,
						ForEachChunk: func(buffer []user) error {
							lengths = append(lengths, len(buffer))
							users = append(users, buffer...)
							return ErrAbortIteration
						},
					})

					tt.AssertNoErr(t, err)
					tt.AssertEqual(t, len(users), 2)
					tt.AssertNotEqual(t, users[0].ID, uint(0))
					tt.AssertEqual(t, users[0].Name, "User1")
					tt.AssertNotEqual(t, users[1].ID, uint(0))
					tt.AssertEqual(t, users[1].Name, "User2")
					tt.AssertEqual(t, lengths, []int{2})
				})

				t.Run("should abort the last iteration when the callback returns an ErrAbortIteration", func(t *testing.T) {
					err := createTables(driver, connStr)
					if err != nil {
						t.Fatal("could not create test table!, reason:", err.Error())
					}

					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, driver)

					_ = c.Insert(ctx, usersTable, &user{Name: "User1"})
					_ = c.Insert(ctx, usersTable, &user{Name: "User2"})
					_ = c.Insert(ctx, usersTable, &user{Name: "User3"})

					returnVals := []error{nil, ErrAbortIteration}
					var lengths []int
					var users []user
					err = c.QueryChunks(ctx, ChunkParser{
						Query:  variation.queryPrefix + `from users where name like ` + c.dialect.Placeholder(0) + ` order by name asc;`,
						Params: []interface{}{"User%"},

						ChunkSize: 2,
						ForEachChunk: func(buffer []user) error {
							lengths = append(lengths, len(buffer))
							users = append(users, buffer...)

							return shiftErrSlice(&returnVals)
						},
					})

					tt.AssertNoErr(t, err)
					tt.AssertEqual(t, len(users), 3)
					tt.AssertNotEqual(t, users[0].ID, uint(0))
					tt.AssertEqual(t, users[0].Name, "User1")
					tt.AssertNotEqual(t, users[1].ID, uint(0))
					tt.AssertEqual(t, users[1].Name, "User2")
					tt.AssertNotEqual(t, users[2].ID, uint(0))
					tt.AssertEqual(t, users[2].Name, "User3")
					tt.AssertEqual(t, lengths, []int{2, 1})
				})

				t.Run("should return error if the callback returns an error in the first iteration", func(t *testing.T) {
					err := createTables(driver, connStr)
					if err != nil {
						t.Fatal("could not create test table!, reason:", err.Error())
					}

					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, driver)

					_ = c.Insert(ctx, usersTable, &user{Name: "User1"})
					_ = c.Insert(ctx, usersTable, &user{Name: "User2"})
					_ = c.Insert(ctx, usersTable, &user{Name: "User3"})

					var lengths []int
					var users []user
					err = c.QueryChunks(ctx, ChunkParser{
						Query:  variation.queryPrefix + `from users where name like ` + c.dialect.Placeholder(0) + ` order by name asc;`,
						Params: []interface{}{"User%"},

						ChunkSize: 2,
						ForEachChunk: func(buffer []user) error {
							lengths = append(lengths, len(buffer))
							users = append(users, buffer...)
							return errors.New("fake error msg")
						},
					})

					tt.AssertNotEqual(t, err, nil)
					tt.AssertEqual(t, len(users), 2)
					tt.AssertNotEqual(t, users[0].ID, uint(0))
					tt.AssertEqual(t, users[0].Name, "User1")
					tt.AssertNotEqual(t, users[1].ID, uint(0))
					tt.AssertEqual(t, users[1].Name, "User2")
					tt.AssertEqual(t, lengths, []int{2})
				})

				t.Run("should return error if the callback returns an error in the last iteration", func(t *testing.T) {
					err := createTables(driver, connStr)
					if err != nil {
						t.Fatal("could not create test table!, reason:", err.Error())
					}

					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, driver)

					_ = c.Insert(ctx, usersTable, &user{Name: "User1"})
					_ = c.Insert(ctx, usersTable, &user{Name: "User2"})
					_ = c.Insert(ctx, usersTable, &user{Name: "User3"})

					returnVals := []error{nil, errors.New("fake error msg")}
					var lengths []int
					var users []user
					err = c.QueryChunks(ctx, ChunkParser{
						Query:  variation.queryPrefix + `from users where name like ` + c.dialect.Placeholder(0) + ` order by name asc;`,
						Params: []interface{}{"User%"},

						ChunkSize: 2,
						ForEachChunk: func(buffer []user) error {
							lengths = append(lengths, len(buffer))
							users = append(users, buffer...)

							return shiftErrSlice(&returnVals)
						},
					})

					tt.AssertNotEqual(t, err, nil)
					tt.AssertEqual(t, len(users), 3)
					tt.AssertNotEqual(t, users[0].ID, uint(0))
					tt.AssertEqual(t, users[0].Name, "User1")
					tt.AssertNotEqual(t, users[1].ID, uint(0))
					tt.AssertEqual(t, users[1].Name, "User2")
					tt.AssertNotEqual(t, users[2].ID, uint(0))
					tt.AssertEqual(t, users[2].Name, "User3")
					tt.AssertEqual(t, lengths, []int{2, 1})
				})

				t.Run("should report error if the input function is invalid", func(t *testing.T) {
					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, driver)

					funcs := []interface{}{
						nil,
						"not a function",
						func() error {
							return nil
						},
						func(extraInputValue []user, extra []user) error {
							return nil
						},
						func(invalidArgType string) error {
							return nil
						},
						func(missingReturnType []user) {
						},
						func(users []user) string {
							return ""
						},
						func(extraReturnValue []user) ([]user, error) {
							return nil, nil
						},
						func(notSliceOfStructs []string) error {
							return nil
						},
					}

					for _, fn := range funcs {
						err := c.QueryChunks(ctx, ChunkParser{
							Query:  variation.queryPrefix + `FROM users`,
							Params: []interface{}{},

							ChunkSize:    2,
							ForEachChunk: fn,
						})
						tt.AssertNotEqual(t, err, nil)
					}
				})

				t.Run("should report error if the query is not valid", func(t *testing.T) {
					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, driver)
					err := c.QueryChunks(ctx, ChunkParser{
						Query:  `SELECT * FROM not a valid query`,
						Params: []interface{}{},

						ChunkSize: 2,
						ForEachChunk: func(buffer []user) error {
							return nil
						},
					})
					tt.AssertNotEqual(t, err, nil)
				})

				t.Run("should report error if using nested struct and the query starts with SELECT", func(t *testing.T) {
					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, driver)

					err := c.QueryChunks(ctx, ChunkParser{
						Query:  `SELECT * FROM users u JOIN posts p ON u.id = p.user_id`,
						Params: []interface{}{},

						ChunkSize: 2,
						ForEachChunk: func(buffer []struct {
							User user `tablename:"users"`
							Post post `tablename:"posts"`
						}) error {
							return nil
						},
					})

					tt.AssertErrContains(t, err, "nested struct", "feature")
				})
			})
		}
	})
}

type MockTxBeginner struct {
	DBAdapter
	BeginTxFn func(ctx context.Context) (Tx, error)
}

func (b MockTxBeginner) BeginTx(ctx context.Context) (Tx, error) {
	return b.BeginTxFn(ctx)
}

// TransactionTest runs all tests for making sure the Transaction function is
// working for a given adapter and driver.
func TransactionTest(
	t *testing.T,
	driver string,
	connStr string,
	newDBAdapter func(t *testing.T) (DBAdapter, io.Closer),
) {
	t.Run("Transaction", func(t *testing.T) {
		t.Run("should query a single row correctly", func(t *testing.T) {
			err := createTables(driver, connStr)
			if err != nil {
				t.Fatal("could not create test table!, reason:", err.Error())
			}

			db, closer := newDBAdapter(t)
			defer closer.Close()

			ctx := context.Background()
			c := newTestDB(db, driver)

			_ = c.Insert(ctx, usersTable, &user{Name: "User1"})
			_ = c.Insert(ctx, usersTable, &user{Name: "User2"})

			var users []user
			err = c.Transaction(ctx, func(db Provider) error {
				db.Query(ctx, &users, "SELECT * FROM users ORDER BY id ASC")
				return nil
			})
			tt.AssertNoErr(t, err)

			tt.AssertEqual(t, len(users), 2)
			tt.AssertEqual(t, users[0].Name, "User1")
			tt.AssertEqual(t, users[1].Name, "User2")
		})

		t.Run("should work normally in nested transactions", func(t *testing.T) {
			err := createTables(driver, connStr)
			if err != nil {
				t.Fatal("could not create test table!, reason:", err.Error())
			}

			db, closer := newDBAdapter(t)
			defer closer.Close()

			ctx := context.Background()
			c := newTestDB(db, driver)

			u := user{
				Name: "User1",
			}
			err = c.Insert(ctx, usersTable, &u)
			tt.AssertNoErr(t, err)
			tt.AssertNotEqual(t, u.ID, 0)

			var updatedUser user
			err = c.Transaction(ctx, func(db Provider) error {
				u.Age = 42
				err = db.Patch(ctx, usersTable, &u)
				if err != nil {
					return err
				}

				return db.Transaction(ctx, func(db Provider) error {
					return db.QueryOne(ctx, &updatedUser, "FROM users WHERE id = "+c.dialect.Placeholder(0), u.ID)
				})
			})
			tt.AssertNoErr(t, err)

			tt.AssertEqual(t, updatedUser.ID, u.ID)
			tt.AssertEqual(t, updatedUser.Age, 42)
		})

		t.Run("should rollback when there are errors", func(t *testing.T) {
			err := createTables(driver, connStr)
			if err != nil {
				t.Fatal("could not create test table!, reason:", err.Error())
			}

			db, closer := newDBAdapter(t)
			defer closer.Close()

			ctx := context.Background()
			c := newTestDB(db, driver)

			u1 := user{Name: "User1", Age: 42}
			u2 := user{Name: "User2", Age: 42}
			_ = c.Insert(ctx, usersTable, &u1)
			_ = c.Insert(ctx, usersTable, &u2)

			err = c.Transaction(ctx, func(db Provider) error {
				err = db.Insert(ctx, usersTable, &user{Name: "User3"})
				tt.AssertNoErr(t, err)
				err = db.Insert(ctx, usersTable, &user{Name: "User4"})
				tt.AssertNoErr(t, err)
				_, err = db.Exec(ctx, "UPDATE users SET age = 22")
				tt.AssertNoErr(t, err)

				return errors.New("fake-error")
			})
			tt.AssertNotEqual(t, err, nil)
			tt.AssertEqual(t, err.Error(), "fake-error")

			var users []user
			err = c.Query(ctx, &users, "SELECT * FROM users ORDER BY id ASC")
			tt.AssertNoErr(t, err)

			tt.AssertEqual(t, users, []user{u1, u2})
		})

		t.Run("should report error when BeginTx() fails", func(t *testing.T) {
			db, closer := newDBAdapter(t)
			defer closer.Close()

			ctx := context.Background()
			c := newTestDB(db, driver)

			cMock := MockTxBeginner{
				DBAdapter: c.db,
				BeginTxFn: func(ctx context.Context) (Tx, error) {
					return nil, fmt.Errorf("fakeErrMsg")
				},
			}
			c.db = cMock

			err := c.Transaction(ctx, func(db Provider) error {
				return nil
			})
			tt.AssertErrContains(t, err, "KSQL", "fakeErrMsg")
		})
	})
}

// ScanRowsTest runs all tests for making sure the ScanRows feature is
// working for a given adapter and driver.
func ScanRowsTest(
	t *testing.T,
	driver string,
	connStr string,
	newDBAdapter func(t *testing.T) (DBAdapter, io.Closer),
) {
	t.Run("ScanRows", func(t *testing.T) {
		t.Run("should scan users correctly", func(t *testing.T) {
			err := createTables(driver, connStr)
			if err != nil {
				t.Fatal("could not create test table!, reason:", err.Error())
			}

			dialect := supportedDialects[driver]
			ctx := context.TODO()
			db, closer := newDBAdapter(t)
			defer closer.Close()
			c := newTestDB(db, driver)
			_ = c.Insert(ctx, usersTable, &user{Name: "User1", Age: 22})
			_ = c.Insert(ctx, usersTable, &user{Name: "User2", Age: 14})
			_ = c.Insert(ctx, usersTable, &user{Name: "User3", Age: 43})

			rows, err := db.QueryContext(ctx, "SELECT * FROM users WHERE name='User2'")
			tt.AssertNoErr(t, err)
			defer rows.Close()

			tt.AssertEqual(t, rows.Next(), true)

			var u user
			err = scanRows(dialect, rows, &u)
			tt.AssertNoErr(t, err)

			tt.AssertEqual(t, u.Name, "User2")
			tt.AssertEqual(t, u.Age, 14)
		})

		t.Run("should ignore extra columns from query", func(t *testing.T) {
			err := createTables(driver, connStr)
			if err != nil {
				t.Fatal("could not create test table!, reason:", err.Error())
			}

			dialect := supportedDialects[driver]
			ctx := context.TODO()
			db, closer := newDBAdapter(t)
			defer closer.Close()
			c := newTestDB(db, driver)
			_ = c.Insert(ctx, usersTable, &user{Name: "User1", Age: 22})

			rows, err := db.QueryContext(ctx, "SELECT * FROM users WHERE name='User1'")
			tt.AssertNoErr(t, err)
			defer rows.Close()

			tt.AssertEqual(t, rows.Next(), true)

			var u struct {
				ID  int `ksql:"id"`
				Age int `ksql:"age"`

				// Omitted for testing purposes:
				// Name string `ksql:"name"`
			}
			err = scanRows(dialect, rows, &u)
			tt.AssertNoErr(t, err)

			tt.AssertEqual(t, u.Age, 22)
		})

		t.Run("should report error for closed rows", func(t *testing.T) {
			err := createTables(driver, connStr)
			if err != nil {
				t.Fatal("could not create test table!, reason:", err.Error())
			}

			dialect := supportedDialects[driver]
			ctx := context.TODO()
			db, closer := newDBAdapter(t)
			defer closer.Close()

			rows, err := db.QueryContext(ctx, "SELECT * FROM users WHERE name='User2'")
			tt.AssertNoErr(t, err)

			var u user
			err = rows.Close()
			tt.AssertNoErr(t, err)
			err = scanRows(dialect, rows, &u)
			tt.AssertNotEqual(t, err, nil)
		})

		t.Run("should report if record is not a pointer", func(t *testing.T) {
			err := createTables(driver, connStr)
			if err != nil {
				t.Fatal("could not create test table!, reason:", err.Error())
			}

			dialect := supportedDialects[driver]
			ctx := context.TODO()
			db, closer := newDBAdapter(t)
			defer closer.Close()

			rows, err := db.QueryContext(ctx, "SELECT * FROM users WHERE name='User2'")
			tt.AssertNoErr(t, err)
			defer rows.Close()

			var u user
			err = scanRows(dialect, rows, u)
			tt.AssertErrContains(t, err, "ksql", "expected", "pointer to struct", "user")
		})

		t.Run("should report if record is not a pointer to struct", func(t *testing.T) {
			err := createTables(driver, connStr)
			if err != nil {
				t.Fatal("could not create test table!, reason:", err.Error())
			}

			dialect := supportedDialects[driver]
			ctx := context.TODO()
			db, closer := newDBAdapter(t)
			defer closer.Close()

			rows, err := db.QueryContext(ctx, "SELECT * FROM users WHERE name='User2'")
			tt.AssertNoErr(t, err)
			defer rows.Close()

			var u map[string]interface{}
			err = scanRows(dialect, rows, &u)
			tt.AssertErrContains(t, err, "ksql", "expected", "pointer to struct", "map[string]interface")
		})
	})
}

func createTables(driver string, connStr string) error {
	if connStr == "" {
		return fmt.Errorf("unsupported driver: '%s'", driver)
	}

	db, err := sql.Open(driver, connStr)
	if err != nil {
		return err
	}
	defer db.Close()

	db.Exec(`DROP TABLE users`)

	switch driver {
	case "sqlite3":
		_, err = db.Exec(`CREATE TABLE users (
		  id INTEGER PRIMARY KEY,
			age INTEGER,
			name TEXT,
			address BLOB
		)`)
	case "postgres":
		_, err = db.Exec(`CREATE TABLE users (
		  id serial PRIMARY KEY,
			age INT,
			name VARCHAR(50),
			address jsonb
		)`)
	case "mysql":
		_, err = db.Exec(`CREATE TABLE users (
			id INT AUTO_INCREMENT PRIMARY KEY,
			age INT,
			name VARCHAR(50),
			address JSON
		)`)
	case "sqlserver":
		_, err = db.Exec(`CREATE TABLE users (
			id INT IDENTITY(1,1) PRIMARY KEY,
			age INT,
			name VARCHAR(50),
			address NVARCHAR(4000)
		)`)
	}
	if err != nil {
		return fmt.Errorf("failed to create new users table: %s", err.Error())
	}

	db.Exec(`DROP TABLE posts`)

	switch driver {
	case "sqlite3":
		_, err = db.Exec(`CREATE TABLE posts (
		  id INTEGER PRIMARY KEY,
		  user_id INTEGER,
			title TEXT
		)`)
	case "postgres":
		_, err = db.Exec(`CREATE TABLE posts (
		  id serial PRIMARY KEY,
			user_id INT,
			title VARCHAR(50)
		)`)
	case "mysql":
		_, err = db.Exec(`CREATE TABLE posts (
			id INT AUTO_INCREMENT PRIMARY KEY,
			user_id INT,
			title VARCHAR(50)
		)`)
	case "sqlserver":
		_, err = db.Exec(`CREATE TABLE posts (
			id INT IDENTITY(1,1) PRIMARY KEY,
			user_id INT,
			title VARCHAR(50)
		)`)
	}
	if err != nil {
		return fmt.Errorf("failed to create new posts table: %s", err.Error())
	}

	db.Exec(`DROP TABLE user_permissions`)

	switch driver {
	case "sqlite3":
		_, err = db.Exec(`CREATE TABLE user_permissions (
			id INTEGER PRIMARY KEY,
			user_id INTEGER,
			perm_id INTEGER,
			type TEXT,
			UNIQUE (user_id, perm_id)
		)`)
	case "postgres":
		_, err = db.Exec(`CREATE TABLE user_permissions (
			id serial PRIMARY KEY,
			user_id INT,
			perm_id INT,
			type VARCHAR(50),
			UNIQUE (user_id, perm_id)
		)`)
	case "mysql":
		_, err = db.Exec(`CREATE TABLE user_permissions (
			id INT AUTO_INCREMENT PRIMARY KEY,
			user_id INT,
			perm_id INT,
			type VARCHAR(50),
			UNIQUE KEY (user_id, perm_id)
		)`)
	case "sqlserver":
		_, err = db.Exec(`CREATE TABLE user_permissions (
			id INT IDENTITY(1,1) PRIMARY KEY,
			user_id INT,
			perm_id INT,
			type VARCHAR(50),
			CONSTRAINT unique_1 UNIQUE (user_id, perm_id)
		)`)
	}
	if err != nil {
		return fmt.Errorf("failed to create new user_permissions table: %s", err.Error())
	}

	return nil
}

func newTestDB(db DBAdapter, driver string) DB {
	return DB{
		driver:  driver,
		dialect: supportedDialects[driver],
		db:      db,
	}
}

func shiftErrSlice(errs *[]error) error {
	err := (*errs)[0]
	*errs = (*errs)[1:]
	return err
}

func getUserByID(db DBAdapter, dialect Dialect, result *user, id uint) error {
	rows, err := db.QueryContext(context.TODO(), `SELECT id, name, age, address FROM users WHERE id=`+dialect.Placeholder(0), id)
	if err != nil {
		return err
	}
	defer rows.Close()

	if rows.Next() == false {
		if rows.Err() != nil {
			return rows.Err()
		}
		return sql.ErrNoRows
	}

	value := jsonSerializable{
		DriverName: dialect.DriverName(),
		Attr:       &result.Address,
	}

	err = rows.Scan(&result.ID, &result.Name, &result.Age, &value)
	if err != nil {
		return err
	}

	return nil
}

func getUserByName(db DBAdapter, driver string, result *user, name string) error {
	dialect := supportedDialects[driver]

	rows, err := db.QueryContext(context.TODO(), `SELECT id, name, age, address FROM users WHERE name=`+dialect.Placeholder(0), name)
	if err != nil {
		return err
	}
	defer rows.Close()

	if rows.Next() == false {
		if rows.Err() != nil {
			return rows.Err()
		}
		return sql.ErrNoRows
	}

	var rawAddr []byte
	err = rows.Scan(&result.ID, &result.Name, &result.Age, &rawAddr)
	if err != nil {
		return err
	}

	if rawAddr == nil {
		return nil
	}

	return json.Unmarshal(rawAddr, &result.Address)
}

func createUserPermission(db DBAdapter, dialect Dialect, userPerm userPermission) error {
	_, err := db.ExecContext(context.TODO(),
		`INSERT INTO user_permissions (user_id, perm_id, type)
		VALUES (`+dialect.Placeholder(0)+`, `+dialect.Placeholder(1)+`, `+dialect.Placeholder(2)+`)`,
		userPerm.UserID, userPerm.PermID, userPerm.Type,
	)

	return err
}

func getUserPermissionBySecondaryKeys(db DBAdapter, dialect Dialect, userID int, permID int) (userPermission, error) {
	rows, err := db.QueryContext(context.TODO(),
		`SELECT id, user_id, perm_id, type FROM user_permissions WHERE user_id=`+dialect.Placeholder(0)+` AND perm_id=`+dialect.Placeholder(1),
		userID, permID,
	)
	if err != nil {
		return userPermission{}, err
	}
	defer rows.Close()

	if rows.Next() == false {
		if rows.Err() != nil {
			return userPermission{}, rows.Err()
		}
		return userPermission{}, sql.ErrNoRows
	}

	var result userPermission
	err = rows.Scan(&result.ID, &result.UserID, &result.PermID, &result.Type)
	if err != nil {
		return userPermission{}, err
	}

	return result, nil
}

func getUserPermissionsByUser(db DBAdapter, driver string, userID int) (results []userPermission, _ error) {
	dialect := supportedDialects[driver]

	rows, err := db.QueryContext(context.TODO(),
		`SELECT id, user_id, perm_id FROM user_permissions WHERE user_id=`+dialect.Placeholder(0),
		userID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var userPerm userPermission
		err := rows.Scan(&userPerm.ID, &userPerm.UserID, &userPerm.PermID)
		if err != nil {
			return nil, err
		}
		results = append(results, userPerm)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return results, nil
}
