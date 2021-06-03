package ksql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/ditointernet/go-assert"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"github.com/vingarcia/ksql/nullable"
)

type User struct {
	ID   uint   `ksql:"id"`
	Name string `ksql:"name"`
	Age  int    `ksql:"age"`

	Address Address `ksql:"address,json"`
}

type Address struct {
	Street string `json:"street"`
	Number string `json:"number"`

	City    string `json:"city"`
	State   string `json:"state"`
	Country string `json:"country"`
}

type Post struct {
	ID     int    `ksql:"id"`
	UserID uint   `ksql:"user_id"`
	Title  string `ksql:"title"`
}

func TestQuery(t *testing.T) {
	for driver := range supportedDialects {
		t.Run(driver, func(t *testing.T) {
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
						err := createTables(driver)
						if err != nil {
							t.Fatal("could not create test table!, reason:", err.Error())
						}

						t.Run("should return 0 results correctly", func(t *testing.T) {
							db := connectDB(t, driver)
							defer db.Close()

							ctx := context.Background()
							c := newTestDB(db, driver, "users")
							var users []User
							err := c.Query(ctx, &users, variation.queryPrefix+`FROM users WHERE id=1;`)
							assert.Equal(t, nil, err)
							assert.Equal(t, 0, len(users))

							users = []User{}
							err = c.Query(ctx, &users, variation.queryPrefix+`FROM users WHERE id=1;`)
							assert.Equal(t, nil, err)
							assert.Equal(t, 0, len(users))
						})

						t.Run("should return a user correctly", func(t *testing.T) {
							db := connectDB(t, driver)
							defer db.Close()

							_, err := db.Exec(`INSERT INTO users (name, age, address) VALUES ('Bia', 0, '{"country":"BR"}')`)
							assert.Equal(t, nil, err)

							ctx := context.Background()
							c := newTestDB(db, driver, "users")
							var users []User
							err = c.Query(ctx, &users, variation.queryPrefix+`FROM users WHERE name=`+c.dialect.Placeholder(0), "Bia")

							assert.Equal(t, nil, err)
							assert.Equal(t, 1, len(users))
							assert.NotEqual(t, uint(0), users[0].ID)
							assert.Equal(t, "Bia", users[0].Name)
							assert.Equal(t, "BR", users[0].Address.Country)
						})

						t.Run("should return multiple users correctly", func(t *testing.T) {
							db := connectDB(t, driver)
							defer db.Close()

							_, err := db.Exec(`INSERT INTO users (name, age, address) VALUES ('João Garcia', 0, '{"country":"US"}')`)
							assert.Equal(t, nil, err)

							_, err = db.Exec(`INSERT INTO users (name, age, address) VALUES ('Bia Garcia', 0, '{"country":"BR"}')`)
							assert.Equal(t, nil, err)

							ctx := context.Background()
							c := newTestDB(db, driver, "users")
							var users []User
							err = c.Query(ctx, &users, variation.queryPrefix+`FROM users WHERE name like `+c.dialect.Placeholder(0), "% Garcia")

							assert.Equal(t, nil, err)
							assert.Equal(t, 2, len(users))

							assert.NotEqual(t, uint(0), users[0].ID)
							assert.Equal(t, "João Garcia", users[0].Name)
							assert.Equal(t, "US", users[0].Address.Country)

							assert.NotEqual(t, uint(0), users[1].ID)
							assert.Equal(t, "Bia Garcia", users[1].Name)
							assert.Equal(t, "BR", users[1].Address.Country)
						})

						t.Run("should query joined tables correctly", func(t *testing.T) {
							db := connectDB(t, driver)
							defer db.Close()

							// This test only makes sense with no query prefix
							if variation.queryPrefix != "" {
								return
							}

							_, err := db.Exec(`INSERT INTO users (name, age, address) VALUES ('João Ribeiro', 0, '{"country":"US"}')`)
							assert.Equal(t, nil, err)
							var joaoID uint
							db.QueryRow(`SELECT id FROM users WHERE name = 'João Ribeiro'`).Scan(&joaoID)

							_, err = db.Exec(`INSERT INTO users (name, age, address) VALUES ('Bia Ribeiro', 0, '{"country":"BR"}')`)
							assert.Equal(t, nil, err)
							var biaID uint
							db.QueryRow(`SELECT id FROM users WHERE name = 'Bia Ribeiro'`).Scan(&biaID)

							_, err = db.Exec(fmt.Sprint(`INSERT INTO posts (user_id, title) VALUES (`, biaID, `, 'Bia Post1')`))
							assert.Equal(t, nil, err)
							_, err = db.Exec(fmt.Sprint(`INSERT INTO posts (user_id, title) VALUES (`, biaID, `, 'Bia Post2')`))
							assert.Equal(t, nil, err)
							_, err = db.Exec(fmt.Sprint(`INSERT INTO posts (user_id, title) VALUES (`, joaoID, `, 'João Post1')`))
							assert.Equal(t, nil, err)

							ctx := context.Background()
							c := newTestDB(db, driver, "users")
							var rows []struct {
								User User `tablename:"u"`
								Post Post `tablename:"p"`
							}
							err = c.Query(ctx, &rows, fmt.Sprint(
								`FROM users u JOIN posts p ON p.user_id = u.id`,
								` WHERE u.name like `, c.dialect.Placeholder(0),
								` ORDER BY u.id, p.id`,
							), "% Ribeiro")

							assert.Equal(t, nil, err)
							assert.Equal(t, 3, len(rows))

							assert.Equal(t, joaoID, rows[0].User.ID)
							assert.Equal(t, "João Ribeiro", rows[0].User.Name)
							assert.Equal(t, "João Post1", rows[0].Post.Title)

							assert.Equal(t, biaID, rows[1].User.ID)
							assert.Equal(t, "Bia Ribeiro", rows[1].User.Name)
							assert.Equal(t, "Bia Post1", rows[1].Post.Title)

							assert.Equal(t, biaID, rows[2].User.ID)
							assert.Equal(t, "Bia Ribeiro", rows[2].User.Name)
							assert.Equal(t, "Bia Post2", rows[2].Post.Title)
						})
					})

					t.Run("using slice of pointers to structs", func(t *testing.T) {
						err := createTables(driver)
						if err != nil {
							t.Fatal("could not create test table!, reason:", err.Error())
						}

						t.Run("should return 0 results correctly", func(t *testing.T) {
							db := connectDB(t, driver)
							defer db.Close()

							ctx := context.Background()
							c := newTestDB(db, driver, "users")
							var users []*User
							err := c.Query(ctx, &users, variation.queryPrefix+`FROM users WHERE id=1;`)
							assert.Equal(t, nil, err)
							assert.Equal(t, 0, len(users))

							users = []*User{}
							err = c.Query(ctx, &users, variation.queryPrefix+`FROM users WHERE id=1;`)
							assert.Equal(t, nil, err)
							assert.Equal(t, 0, len(users))
						})

						t.Run("should return a user correctly", func(t *testing.T) {
							db := connectDB(t, driver)
							defer db.Close()

							_, err := db.Exec(`INSERT INTO users (name, age, address) VALUES ('Bia', 0, '{"country":"BR"}')`)
							assert.Equal(t, nil, err)

							ctx := context.Background()
							c := newTestDB(db, driver, "users")
							var users []*User
							err = c.Query(ctx, &users, variation.queryPrefix+`FROM users WHERE name=`+c.dialect.Placeholder(0), "Bia")

							assert.Equal(t, nil, err)
							assert.Equal(t, 1, len(users))
							assert.NotEqual(t, uint(0), users[0].ID)
							assert.Equal(t, "Bia", users[0].Name)
							assert.Equal(t, "BR", users[0].Address.Country)
						})

						t.Run("should return multiple users correctly", func(t *testing.T) {
							db := connectDB(t, driver)
							defer db.Close()

							_, err := db.Exec(`INSERT INTO users (name, age, address) VALUES ('João Garcia', 0, '{"country":"US"}')`)
							assert.Equal(t, nil, err)

							_, err = db.Exec(`INSERT INTO users (name, age, address) VALUES ('Bia Garcia', 0, '{"country":"BR"}')`)
							assert.Equal(t, nil, err)

							ctx := context.Background()
							c := newTestDB(db, driver, "users")
							var users []*User
							err = c.Query(ctx, &users, variation.queryPrefix+`FROM users WHERE name like `+c.dialect.Placeholder(0), "% Garcia")

							assert.Equal(t, nil, err)
							assert.Equal(t, 2, len(users))

							assert.NotEqual(t, uint(0), users[0].ID)
							assert.Equal(t, "João Garcia", users[0].Name)
							assert.Equal(t, "US", users[0].Address.Country)

							assert.NotEqual(t, uint(0), users[1].ID)
							assert.Equal(t, "Bia Garcia", users[1].Name)
							assert.Equal(t, "BR", users[1].Address.Country)
						})

						t.Run("should query joined tables correctly", func(t *testing.T) {
							db := connectDB(t, driver)
							defer db.Close()

							// This test only makes sense with no query prefix
							if variation.queryPrefix != "" {
								return
							}

							_, err := db.Exec(`INSERT INTO users (name, age, address) VALUES ('João Ribeiro', 0, '{"country":"US"}')`)
							assert.Equal(t, nil, err)
							var joaoID uint
							db.QueryRow(`SELECT id FROM users WHERE name = 'João Ribeiro'`).Scan(&joaoID)

							_, err = db.Exec(`INSERT INTO users (name, age, address) VALUES ('Bia Ribeiro', 0, '{"country":"BR"}')`)
							assert.Equal(t, nil, err)
							var biaID uint
							db.QueryRow(`SELECT id FROM users WHERE name = 'Bia Ribeiro'`).Scan(&biaID)

							_, err = db.Exec(fmt.Sprint(`INSERT INTO posts (user_id, title) VALUES (`, biaID, `, 'Bia Post1')`))
							assert.Equal(t, nil, err)
							_, err = db.Exec(fmt.Sprint(`INSERT INTO posts (user_id, title) VALUES (`, biaID, `, 'Bia Post2')`))
							assert.Equal(t, nil, err)
							_, err = db.Exec(fmt.Sprint(`INSERT INTO posts (user_id, title) VALUES (`, joaoID, `, 'João Post1')`))
							assert.Equal(t, nil, err)

							ctx := context.Background()
							c := newTestDB(db, driver, "users")
							var rows []*struct {
								User User `tablename:"u"`
								Post Post `tablename:"p"`
							}
							err = c.Query(ctx, &rows, fmt.Sprint(
								`FROM users u JOIN posts p ON p.user_id = u.id`,
								` WHERE u.name like `, c.dialect.Placeholder(0),
								` ORDER BY u.id, p.id`,
							), "% Ribeiro")

							assert.Equal(t, nil, err)
							assert.Equal(t, 3, len(rows))

							assert.Equal(t, joaoID, rows[0].User.ID)
							assert.Equal(t, "João Ribeiro", rows[0].User.Name)
							assert.Equal(t, "João Post1", rows[0].Post.Title)

							assert.Equal(t, biaID, rows[1].User.ID)
							assert.Equal(t, "Bia Ribeiro", rows[1].User.Name)
							assert.Equal(t, "Bia Post1", rows[1].Post.Title)

							assert.Equal(t, biaID, rows[2].User.ID)
							assert.Equal(t, "Bia Ribeiro", rows[2].User.Name)
							assert.Equal(t, "Bia Post2", rows[2].Post.Title)
						})
					})
				})
			}

			t.Run("testing error cases", func(t *testing.T) {
				err := createTables(driver)
				if err != nil {
					t.Fatal("could not create test table!, reason:", err.Error())
				}

				t.Run("should report error if input is not a pointer to a slice of structs", func(t *testing.T) {
					db := connectDB(t, driver)
					defer db.Close()

					_, err := db.Exec(`INSERT INTO users (name, age) VALUES ('Andréa Sá', 0)`)
					assert.Equal(t, nil, err)

					_, err = db.Exec(`INSERT INTO users (name, age) VALUES ('Caio Sá', 0)`)
					assert.Equal(t, nil, err)

					ctx := context.Background()
					c := newTestDB(db, driver, "users")
					err = c.Query(ctx, &User{}, `SELECT * FROM users WHERE name like `+c.dialect.Placeholder(0), "% Sá")
					assert.NotEqual(t, nil, err)

					err = c.Query(ctx, []*User{}, `SELECT * FROM users WHERE name like `+c.dialect.Placeholder(0), "% Sá")
					assert.NotEqual(t, nil, err)

					var i int
					err = c.Query(ctx, &i, `SELECT * FROM users WHERE name like `+c.dialect.Placeholder(0), "% Sá")
					assert.NotEqual(t, nil, err)

					err = c.Query(ctx, &[]int{}, `SELECT * FROM users WHERE name like `+c.dialect.Placeholder(0), "% Sá")
					assert.NotEqual(t, nil, err)
				})

				t.Run("should report error if the query is not valid", func(t *testing.T) {
					db := connectDB(t, driver)
					defer db.Close()

					ctx := context.Background()
					c := newTestDB(db, driver, "users")
					var users []User
					err = c.Query(ctx, &users, `SELECT * FROM not a valid query`)
					assert.NotEqual(t, nil, err)
				})
			})

			t.Run("should report error for nested structs with invalid types", func(t *testing.T) {
				t.Run("int", func(t *testing.T) {
					db := connectDB(t, driver)
					defer db.Close()

					ctx := context.Background()
					c := newTestDB(db, driver, "users")
					var rows []struct {
						Foo int `tablename:"foo"`
					}
					err := c.Query(ctx, &rows, fmt.Sprint(
						`FROM users u JOIN posts p ON p.user_id = u.id`,
						` WHERE u.name like `, c.dialect.Placeholder(0),
						` ORDER BY u.id, p.id`,
					), "% Ribeiro")

					assert.NotEqual(t, nil, err)
					msg := err.Error()
					for _, str := range []string{"foo", "int"} {
						assert.Equal(t, true, strings.Contains(msg, str), fmt.Sprintf("missing expected substr '%s' in error message: '%s'", str, msg))
					}
				})

				t.Run("*struct", func(t *testing.T) {
					db := connectDB(t, driver)
					defer db.Close()

					ctx := context.Background()
					c := newTestDB(db, driver, "users")
					var rows []struct {
						Foo *User `tablename:"foo"`
					}
					err := c.Query(ctx, &rows, fmt.Sprint(
						`FROM users u JOIN posts p ON p.user_id = u.id`,
						` WHERE u.name like `, c.dialect.Placeholder(0),
						` ORDER BY u.id, p.id`,
					), "% Ribeiro")

					assert.NotEqual(t, nil, err)
					msg := err.Error()
					for _, str := range []string{"foo", "*ksql.User"} {
						assert.Equal(t, true, strings.Contains(msg, str), fmt.Sprintf("missing expected substr '%s' in error message: '%s'", str, msg))
					}
				})
			})
		})
	}
}

func TestQueryOne(t *testing.T) {
	for driver := range supportedDialects {
		t.Run(driver, func(t *testing.T) {
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
				err := createTables(driver)
				if err != nil {
					t.Fatal("could not create test table!, reason:", err.Error())
				}

				t.Run(variation.desc, func(t *testing.T) {
					t.Run("should return RecordNotFoundErr when there are no results", func(t *testing.T) {
						db := connectDB(t, driver)
						defer db.Close()

						ctx := context.Background()
						c := newTestDB(db, driver, "users")
						u := User{}
						err := c.QueryOne(ctx, &u, variation.queryPrefix+`FROM users WHERE id=1;`)
						assert.Equal(t, ErrRecordNotFound, err)
					})

					t.Run("should return a user correctly", func(t *testing.T) {
						db := connectDB(t, driver)
						defer db.Close()

						_, err := db.Exec(`INSERT INTO users (name, age, address) VALUES ('Bia', 0, '{"country":"BR"}')`)
						assert.Equal(t, nil, err)

						ctx := context.Background()
						c := newTestDB(db, driver, "users")
						u := User{}
						err = c.QueryOne(ctx, &u, variation.queryPrefix+`FROM users WHERE name=`+c.dialect.Placeholder(0), "Bia")

						assert.Equal(t, nil, err)
						assert.NotEqual(t, uint(0), u.ID)
						assert.Equal(t, "Bia", u.Name)
						assert.Equal(t, Address{
							Country: "BR",
						}, u.Address)
					})

					t.Run("should return only the first result on multiples matches", func(t *testing.T) {
						db := connectDB(t, driver)
						defer db.Close()

						_, err := db.Exec(`INSERT INTO users (name, age, address) VALUES ('Andréa Sá', 0, '{"country":"US"}')`)
						assert.Equal(t, nil, err)

						_, err = db.Exec(`INSERT INTO users (name, age, address) VALUES ('Caio Sá', 0, '{"country":"BR"}')`)
						assert.Equal(t, nil, err)

						ctx := context.Background()
						c := newTestDB(db, driver, "users")

						var u User
						err = c.QueryOne(ctx, &u, variation.queryPrefix+`FROM users WHERE name like `+c.dialect.Placeholder(0)+` ORDER BY id ASC`, "% Sá")
						assert.Equal(t, nil, err)
						assert.Equal(t, "Andréa Sá", u.Name)
						assert.Equal(t, 0, u.Age)
						assert.Equal(t, Address{
							Country: "US",
						}, u.Address)
					})

					t.Run("should query joined tables correctly", func(t *testing.T) {
						db := connectDB(t, driver)
						defer db.Close()

						// This test only makes sense with no query prefix
						if variation.queryPrefix != "" {
							return
						}

						_, err := db.Exec(`INSERT INTO users (name, age, address) VALUES ('João Ribeiro', 0, '{"country":"US"}')`)
						assert.Equal(t, nil, err)
						var joaoID uint
						db.QueryRow(`SELECT id FROM users WHERE name = 'João Ribeiro'`).Scan(&joaoID)

						_, err = db.Exec(fmt.Sprint(`INSERT INTO posts (user_id, title) VALUES (`, joaoID, `, 'João Post1')`))
						assert.Equal(t, nil, err)

						ctx := context.Background()
						c := newTestDB(db, driver, "users")
						var row struct {
							User User `tablename:"u"`
							Post Post `tablename:"p"`
						}
						err = c.QueryOne(ctx, &row, fmt.Sprint(
							`FROM users u JOIN posts p ON p.user_id = u.id`,
							` WHERE u.name like `, c.dialect.Placeholder(0),
							` ORDER BY u.id, p.id`,
						), "% Ribeiro")

						assert.Equal(t, nil, err)
						assert.Equal(t, joaoID, row.User.ID)
						assert.Equal(t, "João Ribeiro", row.User.Name)
						assert.Equal(t, "João Post1", row.Post.Title)
					})
				})
			}

			t.Run("should report error if input is not a pointer to struct", func(t *testing.T) {
				db := connectDB(t, driver)
				defer db.Close()

				_, err := db.Exec(`INSERT INTO users (name, age, address) VALUES ('Andréa Sá', 0, '{"country":"US"}')`)
				assert.Equal(t, nil, err)

				_, err = db.Exec(`INSERT INTO users (name, age, address) VALUES ('Caio Sá', 0, '{"country":"BR"}')`)
				assert.Equal(t, nil, err)

				ctx := context.Background()
				c := newTestDB(db, driver, "users")

				err = c.QueryOne(ctx, &[]User{}, `SELECT * FROM users WHERE name like `+c.dialect.Placeholder(0), "% Sá")
				assert.NotEqual(t, nil, err)

				err = c.QueryOne(ctx, User{}, `SELECT * FROM users WHERE name like `+c.dialect.Placeholder(0), "% Sá")
				assert.NotEqual(t, nil, err)
			})

			t.Run("should report error if the query is not valid", func(t *testing.T) {
				db := connectDB(t, driver)
				defer db.Close()

				ctx := context.Background()
				c := newTestDB(db, driver, "users")
				var user User
				err := c.QueryOne(ctx, &user, `SELECT * FROM not a valid query`)
				assert.NotEqual(t, nil, err)
			})
		})
	}
}

func TestInsert(t *testing.T) {
	for driver := range supportedDialects {
		t.Run(driver, func(t *testing.T) {
			t.Run("using slice of structs", func(t *testing.T) {
				err := createTables(driver)
				if err != nil {
					t.Fatal("could not create test table!, reason:", err.Error())
				}

				t.Run("should insert one user correctly", func(t *testing.T) {
					db := connectDB(t, driver)
					defer db.Close()

					ctx := context.Background()
					c := newTestDB(db, driver, "users")

					u := User{
						Name: "Fernanda",
						Address: Address{
							Country: "Brazil",
						},
					}

					err := c.Insert(ctx, &u)
					assert.Equal(t, nil, err)
					assert.NotEqual(t, 0, u.ID)

					result := User{}
					err = getUserByID(c.db, c.dialect, &result, u.ID)
					assert.Equal(t, nil, err)

					assert.Equal(t, u.Name, result.Name)
					assert.Equal(t, u.Address, result.Address)
				})

				t.Run("should insert ignoring the ID for sqlite and multiple ids", func(t *testing.T) {
					if supportedDialects[driver].InsertMethod() != insertWithLastInsertID {
						return
					}

					db := connectDB(t, driver)
					defer db.Close()

					ctx := context.Background()
					// Using columns "id" and "name" as IDs:
					c, err := New(driver, connectionString[driver], Config{
						TableName: "users",
						IDColumns: []string{"id", "name"},
					})
					assert.Equal(t, nil, err)

					u := User{
						Name: "No ID returned",
						Age:  3434, // Random number to avoid false positives on this test

						Address: Address{
							Country: "Brazil 3434",
						},
					}

					err = c.Insert(ctx, &u)
					assert.Equal(t, nil, err)
					assert.Equal(t, uint(0), u.ID)

					result := User{}
					err = getUserByName(c.db, c.dialect, &result, "No ID returned")
					assert.Equal(t, nil, err)

					assert.Equal(t, u.Age, result.Age)
					assert.Equal(t, u.Address, result.Address)
				})
			})

			t.Run("testing error cases", func(t *testing.T) {
				err := createTables(driver)
				if err != nil {
					t.Fatal("could not create test table!, reason:", err.Error())
				}

				t.Run("should report error for invalid input types", func(t *testing.T) {
					db := connectDB(t, driver)
					defer db.Close()

					ctx := context.Background()
					c := newTestDB(db, driver, "users")

					err = c.Insert(ctx, "foo")
					assert.NotEqual(t, nil, err)

					err = c.Insert(ctx, nullable.String("foo"))
					assert.NotEqual(t, nil, err)

					err = c.Insert(ctx, map[string]interface{}{
						"name": "foo",
						"age":  12,
					})
					assert.NotEqual(t, nil, err)

					ifUserForgetToExpandList := []interface{}{
						&User{Name: "foo", Age: 22},
						&User{Name: "bar", Age: 32},
					}
					err = c.Insert(ctx, ifUserForgetToExpandList)
					assert.NotEqual(t, nil, err)

					// We might want to support this in the future, but not for now:
					err = c.Insert(ctx, User{Name: "not a ptr to user", Age: 42})
					assert.NotEqual(t, nil, err)
				})

				t.Run("should report error if for some reason the insertMethod is invalid", func(t *testing.T) {
					db := connectDB(t, driver)
					defer db.Close()

					ctx := context.Background()
					c := newTestDB(db, driver, "users")

					// This is an invalid value:
					c.insertMethod = insertMethod(42)

					err = c.Insert(ctx, &User{Name: "foo"})
					assert.NotEqual(t, nil, err)
				})
			})
		})
	}
}

func TestDelete(t *testing.T) {
	for driver := range supportedDialects {
		t.Run(driver, func(t *testing.T) {
			err := createTables(driver)
			if err != nil {
				t.Fatal("could not create test table!, reason:", err.Error())
			}

			t.Run("should ignore empty lists of ids", func(t *testing.T) {
				db := connectDB(t, driver)
				defer db.Close()

				ctx := context.Background()
				c := newTestDB(db, driver, "users")

				u := User{
					Name: "Won't be deleted",
				}

				err := c.Insert(ctx, &u)
				assert.Equal(t, nil, err)
				assert.NotEqual(t, uint(0), u.ID)

				result := User{}
				err = getUserByID(c.db, c.dialect, &result, u.ID)
				assert.Equal(t, nil, err)

				assert.Equal(t, u.ID, result.ID)

				err = c.Delete(ctx)
				assert.Equal(t, nil, err)

				result = User{}
				err = getUserByID(c.db, c.dialect, &result, u.ID)
				assert.Equal(t, nil, err)
				assert.Equal(t, u.ID, result.ID)
			})

			t.Run("should delete one id correctly", func(t *testing.T) {
				db := connectDB(t, driver)
				defer db.Close()

				ctx := context.Background()
				c := newTestDB(db, driver, "users")

				u1 := User{
					Name: "Fernanda",
				}

				err := c.Insert(ctx, &u1)
				assert.Equal(t, nil, err)
				assert.NotEqual(t, uint(0), u1.ID)

				result := User{}
				err = getUserByID(c.db, c.dialect, &result, u1.ID)
				assert.Equal(t, nil, err)
				assert.Equal(t, u1.ID, result.ID)

				u2 := User{
					Name: "Won't be deleted",
				}

				err = c.Insert(ctx, &u2)
				assert.Equal(t, nil, err)
				assert.NotEqual(t, uint(0), u2.ID)

				result = User{}
				err = getUserByID(c.db, c.dialect, &result, u2.ID)
				assert.Equal(t, nil, err)
				assert.Equal(t, u2.ID, result.ID)

				err = c.Delete(ctx, u1.ID)
				assert.Equal(t, nil, err)

				result = User{}
				err = getUserByID(c.db, c.dialect, &result, u1.ID)
				assert.Equal(t, sql.ErrNoRows, err)

				result = User{}
				err = getUserByID(c.db, c.dialect, &result, u2.ID)
				assert.Equal(t, nil, err)

				assert.NotEqual(t, uint(0), result.ID)
				assert.Equal(t, "Won't be deleted", result.Name)
			})

			t.Run("should delete multiple ids correctly", func(t *testing.T) {
				db := connectDB(t, driver)
				defer db.Close()

				ctx := context.Background()
				c := newTestDB(db, driver, "users")

				u1 := User{
					Name: "Fernanda",
				}
				err := c.Insert(ctx, &u1)
				assert.Equal(t, nil, err)
				assert.NotEqual(t, uint(0), u1.ID)

				u2 := User{
					Name: "Juliano",
				}
				err = c.Insert(ctx, &u2)
				assert.Equal(t, nil, err)
				assert.NotEqual(t, uint(0), u2.ID)

				u3 := User{
					Name: "This won't be deleted",
				}
				err = c.Insert(ctx, &u3)
				assert.Equal(t, nil, err)
				assert.NotEqual(t, uint(0), u3.ID)

				result := User{}
				err = getUserByID(c.db, c.dialect, &result, u1.ID)
				assert.Equal(t, nil, err)
				assert.Equal(t, u1.ID, result.ID)

				result = User{}
				err = getUserByID(c.db, c.dialect, &result, u2.ID)
				assert.Equal(t, nil, err)
				assert.Equal(t, u2.ID, result.ID)

				result = User{}
				err = getUserByID(c.db, c.dialect, &result, u3.ID)
				assert.Equal(t, nil, err)
				assert.Equal(t, u3.ID, result.ID)

				err = c.Delete(ctx, u1.ID, u2.ID)
				assert.Equal(t, nil, err)

				results := []User{}
				err = getUsersByID(c.db, c.dialect, &results, u1.ID, u2.ID, u3.ID)
				assert.Equal(t, nil, err)

				assert.Equal(t, 1, len(results))
				assert.Equal(t, "This won't be deleted", results[0].Name)
			})
		})
	}
}

func TestUpdate(t *testing.T) {
	for driver := range supportedDialects {
		t.Run(driver, func(t *testing.T) {
			err := createTables(driver)
			if err != nil {
				t.Fatal("could not create test table!, reason:", err.Error())
			}

			t.Run("should update one user correctly", func(t *testing.T) {
				db := connectDB(t, driver)
				defer db.Close()

				ctx := context.Background()
				c := newTestDB(db, driver, "users")

				u := User{
					Name: "Letícia",
				}
				_, err := db.Exec(`INSERT INTO users (name, age) VALUES ('Letícia', 0)`)
				assert.Equal(t, nil, err)

				row := db.QueryRow(`SELECT id FROM users WHERE name = 'Letícia'`)
				assert.Equal(t, nil, row.Err())
				err = row.Scan(&u.ID)
				assert.Equal(t, nil, err)
				assert.NotEqual(t, uint(0), u.ID)

				err = c.Update(ctx, User{
					ID:   u.ID,
					Name: "Thayane",
				})
				assert.Equal(t, nil, err)

				var result User
				err = getUserByID(c.db, c.dialect, &result, u.ID)
				assert.Equal(t, nil, err)
				assert.Equal(t, "Thayane", result.Name)
			})

			t.Run("should update one user correctly", func(t *testing.T) {
				db := connectDB(t, driver)
				defer db.Close()

				ctx := context.Background()
				c := newTestDB(db, driver, "users")

				u := User{
					Name: "Letícia",
				}
				_, err := db.Exec(`INSERT INTO users (name, age) VALUES ('Letícia', 0)`)
				assert.Equal(t, nil, err)

				row := db.QueryRow(`SELECT id FROM users WHERE name = 'Letícia'`)
				assert.Equal(t, nil, row.Err())
				err = row.Scan(&u.ID)
				assert.Equal(t, nil, err)
				assert.NotEqual(t, uint(0), u.ID)

				err = c.Update(ctx, User{
					ID:   u.ID,
					Name: "Thayane",
				})
				assert.Equal(t, nil, err)

				var result User
				err = getUserByID(c.db, c.dialect, &result, u.ID)
				assert.Equal(t, nil, err)
				assert.Equal(t, "Thayane", result.Name)
			})

			t.Run("should ignore null pointers on partial updates", func(t *testing.T) {
				db := connectDB(t, driver)
				defer db.Close()

				ctx := context.Background()
				c := newTestDB(db, driver, "users")

				type partialUser struct {
					ID   uint   `ksql:"id"`
					Name string `ksql:"name"`
					Age  *int   `ksql:"age"`
				}
				u := partialUser{
					Name: "Letícia",
					Age:  nullable.Int(22),
				}
				_, err := db.Exec(`INSERT INTO users (name, age) VALUES ('Letícia', 22)`)
				assert.Equal(t, nil, err)

				row := db.QueryRow(`SELECT id FROM users WHERE name = 'Letícia'`)
				assert.Equal(t, nil, row.Err())
				err = row.Scan(&u.ID)
				assert.Equal(t, nil, err)
				assert.NotEqual(t, uint(0), u.ID)

				err = c.Update(ctx, partialUser{
					ID: u.ID,
					// Should be updated because it is not null, just empty:
					Name: "",
					// Should not be updated because it is null:
					Age: nil,
				})
				assert.Equal(t, nil, err)

				var result User
				err = getUserByID(c.db, c.dialect, &result, u.ID)
				assert.Equal(t, nil, err)
				assert.Equal(t, "", result.Name)
				assert.Equal(t, 22, result.Age)
			})

			t.Run("should update valid pointers on partial updates", func(t *testing.T) {
				db := connectDB(t, driver)
				defer db.Close()

				ctx := context.Background()
				c := newTestDB(db, driver, "users")

				type partialUser struct {
					ID   uint   `ksql:"id"`
					Name string `ksql:"name"`
					Age  *int   `ksql:"age"`
				}
				u := partialUser{
					Name: "Letícia",
					Age:  nullable.Int(22),
				}
				_, err := db.Exec(`INSERT INTO users (name, age) VALUES ('Letícia', 22)`)
				assert.Equal(t, nil, err)

				row := db.QueryRow(`SELECT id FROM users WHERE name = 'Letícia'`)
				assert.Equal(t, nil, row.Err())
				err = row.Scan(&u.ID)
				assert.Equal(t, nil, err)
				assert.NotEqual(t, uint(0), u.ID)

				// Should update all fields:
				err = c.Update(ctx, partialUser{
					ID:   u.ID,
					Name: "Thay",
					Age:  nullable.Int(42),
				})
				assert.Equal(t, nil, err)

				var result User
				err = getUserByID(c.db, c.dialect, &result, u.ID)
				assert.Equal(t, nil, err)

				assert.Equal(t, "Thay", result.Name)
				assert.Equal(t, 42, result.Age)
			})

			t.Run("should report database errors correctly", func(t *testing.T) {
				db := connectDB(t, driver)
				defer db.Close()

				ctx := context.Background()
				c := newTestDB(db, driver, "non_existing_table")

				err = c.Update(ctx, User{
					ID:   1,
					Name: "Thayane",
				})
				assert.NotEqual(t, nil, err)
			})
		})
	}
}

func TestQueryChunks(t *testing.T) {
	for driver := range supportedDialects {
		t.Run(driver, func(t *testing.T) {
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
						err := createTables(driver)
						if err != nil {
							t.Fatal("could not create test table!, reason:", err.Error())
						}

						db := connectDB(t, driver)
						defer db.Close()

						ctx := context.Background()
						c := newTestDB(db, driver, "users")

						_ = c.Insert(ctx, &User{
							Name:    "User1",
							Address: Address{Country: "BR"},
						})

						var length int
						var u User
						err = c.QueryChunks(ctx, ChunkParser{
							Query:  variation.queryPrefix + `FROM users WHERE name = ` + c.dialect.Placeholder(0),
							Params: []interface{}{"User1"},

							ChunkSize: 100,
							ForEachChunk: func(users []User) error {
								length = len(users)
								if length > 0 {
									u = users[0]
								}
								return nil
							},
						})

						assert.Equal(t, nil, err)
						assert.Equal(t, 1, length)
						assert.NotEqual(t, uint(0), u.ID)
						assert.Equal(t, "User1", u.Name)
						assert.Equal(t, "BR", u.Address.Country)
					})

					t.Run("should query one chunk correctly", func(t *testing.T) {
						err := createTables(driver)
						if err != nil {
							t.Fatal("could not create test table!, reason:", err.Error())
						}

						db := connectDB(t, driver)
						defer db.Close()

						ctx := context.Background()
						c := newTestDB(db, driver, "users")

						_ = c.Insert(ctx, &User{Name: "User1", Address: Address{Country: "US"}})
						_ = c.Insert(ctx, &User{Name: "User2", Address: Address{Country: "BR"}})

						var lengths []int
						var users []User
						err = c.QueryChunks(ctx, ChunkParser{
							Query:  variation.queryPrefix + `from users where name like ` + c.dialect.Placeholder(0) + ` order by name asc;`,
							Params: []interface{}{"User%"},

							ChunkSize: 2,
							ForEachChunk: func(buffer []User) error {
								users = append(users, buffer...)
								lengths = append(lengths, len(buffer))
								return nil
							},
						})

						assert.Equal(t, nil, err)
						assert.Equal(t, 1, len(lengths))
						assert.Equal(t, 2, lengths[0])

						assert.NotEqual(t, uint(0), users[0].ID)
						assert.Equal(t, "User1", users[0].Name)
						assert.Equal(t, "US", users[0].Address.Country)

						assert.NotEqual(t, uint(0), users[1].ID)
						assert.Equal(t, "User2", users[1].Name)
						assert.Equal(t, "BR", users[1].Address.Country)
					})

					t.Run("should query chunks of 1 correctly", func(t *testing.T) {
						err := createTables(driver)
						if err != nil {
							t.Fatal("could not create test table!, reason:", err.Error())
						}

						db := connectDB(t, driver)
						defer db.Close()

						ctx := context.Background()
						c := newTestDB(db, driver, "users")

						_ = c.Insert(ctx, &User{Name: "User1", Address: Address{Country: "US"}})
						_ = c.Insert(ctx, &User{Name: "User2", Address: Address{Country: "BR"}})

						var lengths []int
						var users []User
						err = c.QueryChunks(ctx, ChunkParser{
							Query:  variation.queryPrefix + `from users where name like ` + c.dialect.Placeholder(0) + ` order by name asc;`,
							Params: []interface{}{"User%"},

							ChunkSize: 1,
							ForEachChunk: func(buffer []User) error {
								lengths = append(lengths, len(buffer))
								users = append(users, buffer...)
								return nil
							},
						})

						assert.Equal(t, nil, err)
						assert.Equal(t, 2, len(users))
						assert.Equal(t, []int{1, 1}, lengths)

						assert.NotEqual(t, uint(0), users[0].ID)
						assert.Equal(t, "User1", users[0].Name)
						assert.Equal(t, "US", users[0].Address.Country)

						assert.NotEqual(t, uint(0), users[1].ID)
						assert.Equal(t, "User2", users[1].Name)
						assert.Equal(t, "BR", users[1].Address.Country)
					})

					t.Run("should load partially filled chunks correctly", func(t *testing.T) {
						err := createTables(driver)
						if err != nil {
							t.Fatal("could not create test table!, reason:", err.Error())
						}

						db := connectDB(t, driver)
						defer db.Close()

						ctx := context.Background()
						c := newTestDB(db, driver, "users")

						_ = c.Insert(ctx, &User{Name: "User1"})
						_ = c.Insert(ctx, &User{Name: "User2"})
						_ = c.Insert(ctx, &User{Name: "User3"})

						var lengths []int
						var users []User
						err = c.QueryChunks(ctx, ChunkParser{
							Query:  variation.queryPrefix + `from users where name like ` + c.dialect.Placeholder(0) + ` order by name asc;`,
							Params: []interface{}{"User%"},

							ChunkSize: 2,
							ForEachChunk: func(buffer []User) error {
								lengths = append(lengths, len(buffer))
								users = append(users, buffer...)
								return nil
							},
						})

						assert.Equal(t, nil, err)
						assert.Equal(t, 3, len(users))
						assert.NotEqual(t, uint(0), users[0].ID)
						assert.Equal(t, "User1", users[0].Name)
						assert.NotEqual(t, uint(0), users[1].ID)
						assert.Equal(t, "User2", users[1].Name)
						assert.NotEqual(t, uint(0), users[2].ID)
						assert.Equal(t, "User3", users[2].Name)
						assert.Equal(t, []int{2, 1}, lengths)
					})

					// xxx
					t.Run("should query joined tables correctly", func(t *testing.T) {
						// This test only makes sense with no query prefix
						if variation.queryPrefix != "" {
							return
						}

						db := connectDB(t, driver)
						defer db.Close()

						joao := User{
							Name: "Thiago Ribeiro",
							Age:  24,
						}
						thatiana := User{
							Name: "Thatiana Ribeiro",
							Age:  20,
						}

						ctx := context.Background()
						c := newTestDB(db, driver, "users")
						_ = c.Insert(ctx, &joao)
						_ = c.Insert(ctx, &thatiana)

						_, err := db.Exec(fmt.Sprint(`INSERT INTO posts (user_id, title) VALUES (`, thatiana.ID, `, 'Thatiana Post1')`))
						assert.Equal(t, nil, err)
						_, err = db.Exec(fmt.Sprint(`INSERT INTO posts (user_id, title) VALUES (`, thatiana.ID, `, 'Thatiana Post2')`))
						assert.Equal(t, nil, err)
						_, err = db.Exec(fmt.Sprint(`INSERT INTO posts (user_id, title) VALUES (`, joao.ID, `, 'Thiago Post1')`))
						assert.Equal(t, nil, err)

						var lengths []int
						var users []User
						var posts []Post
						err = c.QueryChunks(ctx, ChunkParser{
							Query: fmt.Sprint(
								`FROM users u JOIN posts p ON p.user_id = u.id`,
								` WHERE u.name like `, c.dialect.Placeholder(0),
								` ORDER BY u.id, p.id`,
							),
							Params: []interface{}{"% Ribeiro"},

							ChunkSize: 2,
							ForEachChunk: func(chunk []struct {
								User User `tablename:"u"`
								Post Post `tablename:"p"`
							}) error {
								lengths = append(lengths, len(chunk))
								for _, row := range chunk {
									users = append(users, row.User)
									posts = append(posts, row.Post)
								}
								return nil
							},
						})

						assert.Equal(t, nil, err)
						assert.Equal(t, 3, len(posts))

						assert.Equal(t, joao.ID, users[0].ID)
						assert.Equal(t, "Thiago Ribeiro", users[0].Name)
						assert.Equal(t, "Thiago Post1", posts[0].Title)

						assert.Equal(t, thatiana.ID, users[1].ID)
						assert.Equal(t, "Thatiana Ribeiro", users[1].Name)
						assert.Equal(t, "Thatiana Post1", posts[1].Title)

						assert.Equal(t, thatiana.ID, users[2].ID)
						assert.Equal(t, "Thatiana Ribeiro", users[2].Name)
						assert.Equal(t, "Thatiana Post2", posts[2].Title)
					})

					t.Run("should abort the first iteration when the callback returns an ErrAbortIteration", func(t *testing.T) {
						err := createTables(driver)
						if err != nil {
							t.Fatal("could not create test table!, reason:", err.Error())
						}

						db := connectDB(t, driver)
						defer db.Close()

						ctx := context.Background()
						c := newTestDB(db, driver, "users")

						_ = c.Insert(ctx, &User{Name: "User1"})
						_ = c.Insert(ctx, &User{Name: "User2"})
						_ = c.Insert(ctx, &User{Name: "User3"})

						var lengths []int
						var users []User
						err = c.QueryChunks(ctx, ChunkParser{
							Query:  variation.queryPrefix + `from users where name like ` + c.dialect.Placeholder(0) + ` order by name asc;`,
							Params: []interface{}{"User%"},

							ChunkSize: 2,
							ForEachChunk: func(buffer []User) error {
								lengths = append(lengths, len(buffer))
								users = append(users, buffer...)
								return ErrAbortIteration
							},
						})

						assert.Equal(t, nil, err)
						assert.Equal(t, 2, len(users))
						assert.NotEqual(t, uint(0), users[0].ID)
						assert.Equal(t, "User1", users[0].Name)
						assert.NotEqual(t, uint(0), users[1].ID)
						assert.Equal(t, "User2", users[1].Name)
						assert.Equal(t, []int{2}, lengths)
					})

					t.Run("should abort the last iteration when the callback returns an ErrAbortIteration", func(t *testing.T) {
						err := createTables(driver)
						if err != nil {
							t.Fatal("could not create test table!, reason:", err.Error())
						}

						db := connectDB(t, driver)
						defer db.Close()

						ctx := context.Background()
						c := newTestDB(db, driver, "users")

						_ = c.Insert(ctx, &User{Name: "User1"})
						_ = c.Insert(ctx, &User{Name: "User2"})
						_ = c.Insert(ctx, &User{Name: "User3"})

						returnVals := []error{nil, ErrAbortIteration}
						var lengths []int
						var users []User
						err = c.QueryChunks(ctx, ChunkParser{
							Query:  variation.queryPrefix + `from users where name like ` + c.dialect.Placeholder(0) + ` order by name asc;`,
							Params: []interface{}{"User%"},

							ChunkSize: 2,
							ForEachChunk: func(buffer []User) error {
								lengths = append(lengths, len(buffer))
								users = append(users, buffer...)

								return shiftErrSlice(&returnVals)
							},
						})

						assert.Equal(t, nil, err)
						assert.Equal(t, 3, len(users))
						assert.NotEqual(t, uint(0), users[0].ID)
						assert.Equal(t, "User1", users[0].Name)
						assert.NotEqual(t, uint(0), users[1].ID)
						assert.Equal(t, "User2", users[1].Name)
						assert.NotEqual(t, uint(0), users[2].ID)
						assert.Equal(t, "User3", users[2].Name)
						assert.Equal(t, []int{2, 1}, lengths)
					})

					t.Run("should return error if the callback returns an error in the first iteration", func(t *testing.T) {
						err := createTables(driver)
						if err != nil {
							t.Fatal("could not create test table!, reason:", err.Error())
						}

						db := connectDB(t, driver)
						defer db.Close()

						ctx := context.Background()
						c := newTestDB(db, driver, "users")

						_ = c.Insert(ctx, &User{Name: "User1"})
						_ = c.Insert(ctx, &User{Name: "User2"})
						_ = c.Insert(ctx, &User{Name: "User3"})

						var lengths []int
						var users []User
						err = c.QueryChunks(ctx, ChunkParser{
							Query:  variation.queryPrefix + `from users where name like ` + c.dialect.Placeholder(0) + ` order by name asc;`,
							Params: []interface{}{"User%"},

							ChunkSize: 2,
							ForEachChunk: func(buffer []User) error {
								lengths = append(lengths, len(buffer))
								users = append(users, buffer...)
								return errors.New("fake error msg")
							},
						})

						assert.NotEqual(t, nil, err)
						assert.Equal(t, 2, len(users))
						assert.NotEqual(t, uint(0), users[0].ID)
						assert.Equal(t, "User1", users[0].Name)
						assert.NotEqual(t, uint(0), users[1].ID)
						assert.Equal(t, "User2", users[1].Name)
						assert.Equal(t, []int{2}, lengths)
					})

					t.Run("should return error if the callback returns an error in the last iteration", func(t *testing.T) {
						err := createTables(driver)
						if err != nil {
							t.Fatal("could not create test table!, reason:", err.Error())
						}

						db := connectDB(t, driver)
						defer db.Close()

						ctx := context.Background()
						c := newTestDB(db, driver, "users")

						_ = c.Insert(ctx, &User{Name: "User1"})
						_ = c.Insert(ctx, &User{Name: "User2"})
						_ = c.Insert(ctx, &User{Name: "User3"})

						returnVals := []error{nil, errors.New("fake error msg")}
						var lengths []int
						var users []User
						err = c.QueryChunks(ctx, ChunkParser{
							Query:  variation.queryPrefix + `from users where name like ` + c.dialect.Placeholder(0) + ` order by name asc;`,
							Params: []interface{}{"User%"},

							ChunkSize: 2,
							ForEachChunk: func(buffer []User) error {
								lengths = append(lengths, len(buffer))
								users = append(users, buffer...)

								return shiftErrSlice(&returnVals)
							},
						})

						assert.NotEqual(t, nil, err)
						assert.Equal(t, 3, len(users))
						assert.NotEqual(t, uint(0), users[0].ID)
						assert.Equal(t, "User1", users[0].Name)
						assert.NotEqual(t, uint(0), users[1].ID)
						assert.Equal(t, "User2", users[1].Name)
						assert.NotEqual(t, uint(0), users[2].ID)
						assert.Equal(t, "User3", users[2].Name)
						assert.Equal(t, []int{2, 1}, lengths)
					})

					t.Run("should report error if the input function is invalid", func(t *testing.T) {
						db := connectDB(t, driver)
						defer db.Close()

						ctx := context.Background()
						c := newTestDB(db, driver, "users")

						funcs := []interface{}{
							nil,
							"not a function",
							func() error {
								return nil
							},
							func(extraInputValue []User, extra []User) error {
								return nil
							},
							func(invalidArgType string) error {
								return nil
							},
							func(missingReturnType []User) {
								return
							},
							func(users []User) string {
								return ""
							},
							func(extraReturnValue []User) ([]User, error) {
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
							assert.NotEqual(t, nil, err)
						}
					})

					t.Run("should report error if the query is not valid", func(t *testing.T) {
						db := connectDB(t, driver)
						defer db.Close()

						ctx := context.Background()
						c := newTestDB(db, driver, "users")
						err := c.QueryChunks(ctx, ChunkParser{
							Query:  `SELECT * FROM not a valid query`,
							Params: []interface{}{},

							ChunkSize: 2,
							ForEachChunk: func(buffer []User) error {
								return nil
							},
						})
						assert.NotEqual(t, nil, err)
					})
				})
			}
		})
	}
}

func TestTransaction(t *testing.T) {
	for driver := range supportedDialects {
		t.Run(driver, func(t *testing.T) {
			t.Run("should query a single row correctly", func(t *testing.T) {
				err := createTables(driver)
				if err != nil {
					t.Fatal("could not create test table!, reason:", err.Error())
				}

				db := connectDB(t, driver)
				defer db.Close()

				ctx := context.Background()
				c := newTestDB(db, driver, "users")

				_ = c.Insert(ctx, &User{Name: "User1"})
				_ = c.Insert(ctx, &User{Name: "User2"})

				var users []User
				err = c.Transaction(ctx, func(db SQLProvider) error {
					db.Query(ctx, &users, "SELECT * FROM users ORDER BY id ASC")
					return nil
				})
				assert.Equal(t, nil, err)

				assert.Equal(t, 2, len(users))
				assert.Equal(t, "User1", users[0].Name)
				assert.Equal(t, "User2", users[1].Name)
			})

			t.Run("should rollback when there are errors", func(t *testing.T) {
				err := createTables(driver)
				if err != nil {
					t.Fatal("could not create test table!, reason:", err.Error())
				}

				db := connectDB(t, driver)
				defer db.Close()

				ctx := context.Background()
				c := newTestDB(db, driver, "users")

				u1 := User{Name: "User1", Age: 42}
				u2 := User{Name: "User2", Age: 42}
				_ = c.Insert(ctx, &u1)
				_ = c.Insert(ctx, &u2)

				err = c.Transaction(ctx, func(db SQLProvider) error {
					err = db.Insert(ctx, &User{Name: "User3"})
					assert.Equal(t, nil, err)
					err = db.Insert(ctx, &User{Name: "User4"})
					assert.Equal(t, nil, err)
					err = db.Exec(ctx, "UPDATE users SET age = 22")
					assert.Equal(t, nil, err)

					return errors.New("fake-error")
				})
				assert.NotEqual(t, nil, err)
				assert.Equal(t, "fake-error", err.Error())

				var users []User
				err = c.Query(ctx, &users, "SELECT * FROM users ORDER BY id ASC")
				assert.Equal(t, nil, err)

				assert.Equal(t, []User{u1, u2}, users)
			})
		})
	}
}

func TestScanRows(t *testing.T) {
	t.Run("should scan users correctly", func(t *testing.T) {
		err := createTables("sqlite3")
		if err != nil {
			t.Fatal("could not create test table!, reason:", err.Error())
		}

		dialect := supportedDialects["sqlite3"]
		ctx := context.TODO()
		db := connectDB(t, "sqlite3")
		defer db.Close()
		c := newTestDB(db, "sqlite3", "users")
		_ = c.Insert(ctx, &User{Name: "User1", Age: 22})
		_ = c.Insert(ctx, &User{Name: "User2", Age: 14})
		_ = c.Insert(ctx, &User{Name: "User3", Age: 43})

		rows, err := db.QueryContext(ctx, "select * from users where name='User2'")
		assert.Equal(t, nil, err)
		defer rows.Close()

		assert.Equal(t, true, rows.Next())

		var u User
		err = scanRows(dialect, rows, &u)
		assert.Equal(t, nil, err)

		assert.Equal(t, "User2", u.Name)
		assert.Equal(t, 14, u.Age)
	})

	t.Run("should ignore extra columns from query", func(t *testing.T) {
		err := createTables("sqlite3")
		if err != nil {
			t.Fatal("could not create test table!, reason:", err.Error())
		}

		dialect := supportedDialects["sqlite3"]
		ctx := context.TODO()
		db := connectDB(t, "sqlite3")
		defer db.Close()
		c := newTestDB(db, "sqlite3", "users")
		_ = c.Insert(ctx, &User{Name: "User1", Age: 22})

		rows, err := db.QueryContext(ctx, "SELECT * FROM users WHERE name='User1'")
		assert.Equal(t, nil, err)
		defer rows.Close()

		assert.Equal(t, true, rows.Next())

		var user struct {
			ID  int `ksql:"id"`
			Age int `ksql:"age"`

			// Omitted for testing purposes:
			// Name string `ksql:"name"`
		}
		err = scanRows(dialect, rows, &user)
		assert.Equal(t, nil, err)

		assert.Equal(t, 22, user.Age)
	})

	t.Run("should report error for closed rows", func(t *testing.T) {
		err := createTables("sqlite3")
		if err != nil {
			t.Fatal("could not create test table!, reason:", err.Error())
		}

		dialect := supportedDialects["sqlite3"]
		ctx := context.TODO()
		db := connectDB(t, "sqlite3")
		defer db.Close()

		rows, err := db.QueryContext(ctx, "select * from users where name='User2'")
		assert.Equal(t, nil, err)

		var u User
		err = rows.Close()
		assert.Equal(t, nil, err)
		err = scanRows(dialect, rows, &u)
		assert.NotEqual(t, nil, err)
	})

	t.Run("should report if record is not a pointer", func(t *testing.T) {
		err := createTables("sqlite3")
		if err != nil {
			t.Fatal("could not create test table!, reason:", err.Error())
		}

		dialect := supportedDialects["sqlite3"]
		ctx := context.TODO()
		db := connectDB(t, "sqlite3")
		defer db.Close()

		rows, err := db.QueryContext(ctx, "select * from users where name='User2'")
		assert.Equal(t, nil, err)

		var u User
		err = scanRows(dialect, rows, u)
		assert.NotEqual(t, nil, err)
	})

	t.Run("should report if record is not a pointer to struct", func(t *testing.T) {
		err := createTables("sqlite3")
		if err != nil {
			t.Fatal("could not create test table!, reason:", err.Error())
		}

		dialect := supportedDialects["sqlite3"]
		ctx := context.TODO()
		db := connectDB(t, "sqlite3")
		defer db.Close()

		rows, err := db.QueryContext(ctx, "select * from users where name='User2'")
		assert.Equal(t, nil, err)

		var u map[string]interface{}
		err = scanRows(dialect, rows, &u)
		assert.NotEqual(t, nil, err)
	})
}

var connectionString = map[string]string{
	"postgres":  "host=localhost port=5432 user=postgres password=postgres dbname=ksql sslmode=disable",
	"sqlite3":   "/tmp/ksql.db",
	"mysql":     "root:mysql@(127.0.0.1:3306)/ksql?timeout=30s",
	"sqlserver": "sqlserver://sa:Sqls3rv3r@127.0.0.1:1433?databaseName=ksql",
}

func createTables(driver string) error {
	connStr := connectionString[driver]
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
		return fmt.Errorf("failed to create new users table: %s", err.Error())
	}

	return nil
}

func newTestDB(db *sql.DB, driver string, tableName string, ids ...string) DB {
	if len(ids) == 0 {
		ids = []string{"id"}
	}

	return DB{
		driver:    driver,
		dialect:   supportedDialects[driver],
		db:        db,
		tableName: tableName,

		idCols:       ids,
		insertMethod: supportedDialects[driver].InsertMethod(),
	}
}

func connectDB(t *testing.T, driver string) *sql.DB {
	connStr := connectionString[driver]
	if connStr == "" {
		panic(fmt.Sprintf("unsupported driver: '%s'", driver))
	}

	db, err := sql.Open(driver, connStr)
	if err != nil {
		t.Fatal(err.Error())
	}
	return db
}

func shiftErrSlice(errs *[]error) error {
	err := (*errs)[0]
	*errs = (*errs)[1:]
	return err
}

func getUsersByID(dbi sqlProvider, dialect dialect, resultsPtr *[]User, ids ...uint) error {
	db := dbi.(*sql.DB)

	placeholders := make([]string, len(ids))
	params := make([]interface{}, len(ids))
	for i := range ids {
		params[i] = ids[i]
		placeholders[i] = dialect.Placeholder(i)
	}

	results := []User{}
	rows, err := db.Query(
		fmt.Sprintf(
			"SELECT id, name, age FROM users WHERE id IN (%s)",
			strings.Join(placeholders, ", "),
		),
		params...,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var u User
		err = rows.Scan(&u.ID, &u.Name, &u.Age)
		if err != nil {
			return err
		}
		results = append(results, u)
	}
	if rows.Err() != nil {
		return rows.Err()
	}
	if err := rows.Close(); err != nil {
		return err
	}

	*resultsPtr = results
	return nil
}

func getUserByID(dbi sqlProvider, dialect dialect, result *User, id uint) error {
	db := dbi.(*sql.DB)

	row := db.QueryRow(`SELECT id, name, age, address FROM users WHERE id=`+dialect.Placeholder(0), id)
	if row.Err() != nil {
		return row.Err()
	}

	value := jsonSerializable{
		DriverName: dialect.DriverName(),
		Attr:       &result.Address,
	}

	err := row.Scan(&result.ID, &result.Name, &result.Age, &value)
	if err != nil {
		return err
	}

	return nil
}

func getUserByName(dbi sqlProvider, dialect dialect, result *User, name string) error {
	db := dbi.(*sql.DB)

	row := db.QueryRow(`SELECT id, name, age, address FROM users WHERE name=`+dialect.Placeholder(0), name)
	if row.Err() != nil {
		return row.Err()
	}

	var rawAddr []byte
	err := row.Scan(&result.ID, &result.Name, &result.Age, &rawAddr)
	if err != nil {
		return err
	}

	if rawAddr == nil {
		return nil
	}

	return json.Unmarshal(rawAddr, &result.Address)
}
