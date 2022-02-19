package ksql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/ditointernet/go-assert"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/vingarcia/ksql/nullable"

	tt "github.com/vingarcia/ksql/internal/testtools"
)

var UsersTable = NewTable("users")

type User struct {
	ID   uint   `ksql:"id"`
	Name string `ksql:"name"`
	Age  int    `ksql:"age"`

	Address Address `ksql:"address,json"`

	// This attr has no ksql tag, thus, it should be ignored:
	AttrThatShouldBeIgnored string
}

type Address struct {
	Street string `json:"street"`
	Number string `json:"number"`

	City    string `json:"city"`
	State   string `json:"state"`
	Country string `json:"country"`
}

var PostsTable = NewTable("posts")

type Post struct {
	ID     int    `ksql:"id"`
	UserID uint   `ksql:"user_id"`
	Title  string `ksql:"title"`
}

var UserPermissionsTable = NewTable("user_permissions", "user_id", "perm_id")

type UserPermission struct {
	ID     int `ksql:"id"`
	UserID int `ksql:"user_id"`
	PermID int `ksql:"perm_id"`
}

type testConfig struct {
	driver      string
	adapterName string
}

var supportedConfigs = []testConfig{
	{
		driver:      "sqlite3",
		adapterName: "sql",
	},
	{
		driver:      "postgres",
		adapterName: "sql",
	},
	{
		driver:      "mysql",
		adapterName: "sql",
	},
	{
		driver:      "sqlserver",
		adapterName: "sql",
	},
	{
		driver:      "postgres",
		adapterName: "pgx",
	},
}

func TestQuery(t *testing.T) {
	for _, config := range supportedConfigs {
		QueryTest(t,
			config,
			func(t *testing.T) (DBAdapter, io.Closer) {
				db, close := connectDB(t, config)
				return db, close
			},
		)
	}
}

func QueryTest(
	t *testing.T,
	config testConfig,
	newDBAdapter func(t *testing.T) (DBAdapter, io.Closer),
) {
	t.Run(config.driver, func(t *testing.T) {
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
					err := createTables(config.driver)
					if err != nil {
						t.Fatal("could not create test table!, reason:", err.Error())
					}

					t.Run("should return 0 results correctly", func(t *testing.T) {
						db, closer := newDBAdapter(t)
						defer closer.Close()

						ctx := context.Background()
						c := newTestDB(db, config.driver)
						var users []User
						err := c.Query(ctx, &users, variation.queryPrefix+`FROM users WHERE id=1;`)
						tt.AssertNoErr(t, err)
						tt.AssertEqual(t, 0, len(users))

						users = []User{}
						err = c.Query(ctx, &users, variation.queryPrefix+`FROM users WHERE id=1;`)
						tt.AssertEqual(t, nil, err)
						tt.AssertEqual(t, 0, len(users))
					})

					t.Run("should return a user correctly", func(t *testing.T) {
						db, closer := newDBAdapter(t)
						defer closer.Close()

						_, err := db.ExecContext(context.TODO(), `INSERT INTO users (name, age, address) VALUES ('Bia', 0, '{"country":"BR"}')`)
						tt.AssertNoErr(t, err)

						ctx := context.Background()
						c := newTestDB(db, config.driver)
						var users []User
						err = c.Query(ctx, &users, variation.queryPrefix+`FROM users WHERE name=`+c.dialect.Placeholder(0), "Bia")

						tt.AssertNoErr(t, err)
						tt.AssertEqual(t, 1, len(users))
						tt.AssertNotEqual(t, uint(0), users[0].ID)
						tt.AssertEqual(t, "Bia", users[0].Name)
						tt.AssertEqual(t, "BR", users[0].Address.Country)
					})

					t.Run("should return multiple users correctly", func(t *testing.T) {
						db, closer := newDBAdapter(t)
						defer closer.Close()

						_, err := db.ExecContext(context.TODO(), `INSERT INTO users (name, age, address) VALUES ('João Garcia', 0, '{"country":"US"}')`)
						tt.AssertNoErr(t, err)

						_, err = db.ExecContext(context.TODO(), `INSERT INTO users (name, age, address) VALUES ('Bia Garcia', 0, '{"country":"BR"}')`)
						tt.AssertNoErr(t, err)

						ctx := context.Background()
						c := newTestDB(db, config.driver)
						var users []User
						err = c.Query(ctx, &users, variation.queryPrefix+`FROM users WHERE name like `+c.dialect.Placeholder(0), "% Garcia")

						tt.AssertNoErr(t, err)
						tt.AssertEqual(t, 2, len(users))

						tt.AssertNotEqual(t, uint(0), users[0].ID)
						tt.AssertEqual(t, "João Garcia", users[0].Name)
						tt.AssertEqual(t, "US", users[0].Address.Country)

						tt.AssertNotEqual(t, uint(0), users[1].ID)
						tt.AssertEqual(t, "Bia Garcia", users[1].Name)
						tt.AssertEqual(t, "BR", users[1].Address.Country)
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
						var joao User
						getUserByName(db, config.driver, &joao, "João Ribeiro")
						tt.AssertNoErr(t, err)

						_, err = db.ExecContext(context.TODO(), `INSERT INTO users (name, age, address) VALUES ('Bia Ribeiro', 0, '{"country":"BR"}')`)
						tt.AssertNoErr(t, err)
						var bia User
						getUserByName(db, config.driver, &bia, "Bia Ribeiro")

						_, err = db.ExecContext(context.TODO(), fmt.Sprint(`INSERT INTO posts (user_id, title) VALUES (`, bia.ID, `, 'Bia Post1')`))
						tt.AssertNoErr(t, err)
						_, err = db.ExecContext(context.TODO(), fmt.Sprint(`INSERT INTO posts (user_id, title) VALUES (`, bia.ID, `, 'Bia Post2')`))
						tt.AssertNoErr(t, err)
						_, err = db.ExecContext(context.TODO(), fmt.Sprint(`INSERT INTO posts (user_id, title) VALUES (`, joao.ID, `, 'João Post1')`))
						tt.AssertNoErr(t, err)

						ctx := context.Background()
						c := newTestDB(db, config.driver)
						var rows []struct {
							User User `tablename:"u"`
							Post Post `tablename:"p"`

							// This one has no ksql or tablename tag,
							// so it should just be ignored to avoid strange
							// unexpected errors:
							ExtraStructThatShouldBeIgnored User
						}
						err = c.Query(ctx, &rows, fmt.Sprint(
							`FROM users u JOIN posts p ON p.user_id = u.id`,
							` WHERE u.name like `, c.dialect.Placeholder(0),
							` ORDER BY u.id, p.id`,
						), "% Ribeiro")

						tt.AssertNoErr(t, err)
						tt.AssertEqual(t, 3, len(rows))

						tt.AssertEqual(t, joao.ID, rows[0].User.ID)
						tt.AssertEqual(t, "João Ribeiro", rows[0].User.Name)
						tt.AssertEqual(t, "João Post1", rows[0].Post.Title)

						tt.AssertEqual(t, bia.ID, rows[1].User.ID)
						tt.AssertEqual(t, "Bia Ribeiro", rows[1].User.Name)
						tt.AssertEqual(t, "Bia Post1", rows[1].Post.Title)

						tt.AssertEqual(t, bia.ID, rows[2].User.ID)
						tt.AssertEqual(t, "Bia Ribeiro", rows[2].User.Name)
						tt.AssertEqual(t, "Bia Post2", rows[2].Post.Title)
					})
				})

				t.Run("using slice of pointers to structs", func(t *testing.T) {
					err := createTables(config.driver)
					if err != nil {
						t.Fatal("could not create test table!, reason:", err.Error())
					}

					t.Run("should return 0 results correctly", func(t *testing.T) {
						db, closer := newDBAdapter(t)
						defer closer.Close()

						ctx := context.Background()
						c := newTestDB(db, config.driver)
						var users []*User
						err := c.Query(ctx, &users, variation.queryPrefix+`FROM users WHERE id=1;`)
						tt.AssertNoErr(t, err)
						tt.AssertEqual(t, 0, len(users))

						users = []*User{}
						err = c.Query(ctx, &users, variation.queryPrefix+`FROM users WHERE id=1;`)
						tt.AssertNoErr(t, err)
						tt.AssertEqual(t, 0, len(users))
					})

					t.Run("should return a user correctly", func(t *testing.T) {
						db, closer := newDBAdapter(t)
						defer closer.Close()

						ctx := context.Background()

						_, err := db.ExecContext(ctx, `INSERT INTO users (name, age, address) VALUES ('Bia', 0, '{"country":"BR"}')`)
						tt.AssertNoErr(t, err)

						c := newTestDB(db, config.driver)
						var users []*User
						err = c.Query(ctx, &users, variation.queryPrefix+`FROM users WHERE name=`+c.dialect.Placeholder(0), "Bia")

						tt.AssertEqual(t, nil, err)
						tt.AssertEqual(t, 1, len(users))
						tt.AssertNotEqual(t, uint(0), users[0].ID)
						tt.AssertEqual(t, "Bia", users[0].Name)
						tt.AssertEqual(t, "BR", users[0].Address.Country)
					})

					t.Run("should return multiple users correctly", func(t *testing.T) {
						db, closer := newDBAdapter(t)
						defer closer.Close()

						ctx := context.Background()

						_, err := db.ExecContext(ctx, `INSERT INTO users (name, age, address) VALUES ('João Garcia', 0, '{"country":"US"}')`)
						tt.AssertNoErr(t, err)

						_, err = db.ExecContext(ctx, `INSERT INTO users (name, age, address) VALUES ('Bia Garcia', 0, '{"country":"BR"}')`)
						tt.AssertNoErr(t, err)

						c := newTestDB(db, config.driver)
						var users []*User
						err = c.Query(ctx, &users, variation.queryPrefix+`FROM users WHERE name like `+c.dialect.Placeholder(0), "% Garcia")

						tt.AssertNoErr(t, err)
						tt.AssertEqual(t, 2, len(users))

						tt.AssertNotEqual(t, uint(0), users[0].ID)
						tt.AssertEqual(t, "João Garcia", users[0].Name)
						tt.AssertEqual(t, "US", users[0].Address.Country)

						tt.AssertNotEqual(t, uint(0), users[1].ID)
						tt.AssertEqual(t, "Bia Garcia", users[1].Name)
						tt.AssertEqual(t, "BR", users[1].Address.Country)
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
						var joao User
						getUserByName(db, config.driver, &joao, "João Ribeiro")

						_, err = db.ExecContext(ctx, `INSERT INTO users (name, age, address) VALUES ('Bia Ribeiro', 0, '{"country":"BR"}')`)
						assert.Equal(t, nil, err)
						var bia User
						getUserByName(db, config.driver, &bia, "Bia Ribeiro")

						_, err = db.ExecContext(ctx, fmt.Sprint(`INSERT INTO posts (user_id, title) VALUES (`, bia.ID, `, 'Bia Post1')`))
						tt.AssertNoErr(t, err)
						_, err = db.ExecContext(ctx, fmt.Sprint(`INSERT INTO posts (user_id, title) VALUES (`, bia.ID, `, 'Bia Post2')`))
						tt.AssertNoErr(t, err)
						_, err = db.ExecContext(ctx, fmt.Sprint(`INSERT INTO posts (user_id, title) VALUES (`, joao.ID, `, 'João Post1')`))
						tt.AssertNoErr(t, err)

						c := newTestDB(db, config.driver)
						var rows []*struct {
							User User `tablename:"u"`
							Post Post `tablename:"p"`
						}
						err = c.Query(ctx, &rows, fmt.Sprint(
							`FROM users u JOIN posts p ON p.user_id = u.id`,
							` WHERE u.name like `, c.dialect.Placeholder(0),
							` ORDER BY u.id, p.id`,
						), "% Ribeiro")

						tt.AssertNoErr(t, err)
						tt.AssertEqual(t, 3, len(rows))

						tt.AssertEqual(t, joao.ID, rows[0].User.ID)
						tt.AssertEqual(t, "João Ribeiro", rows[0].User.Name)
						tt.AssertEqual(t, "João Post1", rows[0].Post.Title)

						tt.AssertEqual(t, bia.ID, rows[1].User.ID)
						tt.AssertEqual(t, "Bia Ribeiro", rows[1].User.Name)
						tt.AssertEqual(t, "Bia Post1", rows[1].Post.Title)

						tt.AssertEqual(t, bia.ID, rows[2].User.ID)
						tt.AssertEqual(t, "Bia Ribeiro", rows[2].User.Name)
						tt.AssertEqual(t, "Bia Post2", rows[2].Post.Title)
					})
				})
			})
		}

		t.Run("testing error cases", func(t *testing.T) {
			err := createTables(config.driver)
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

				c := newTestDB(db, config.driver)
				err = c.Query(ctx, &User{}, `SELECT * FROM users WHERE name like `+c.dialect.Placeholder(0), "% Sá")
				tt.AssertErrContains(t, err, "expected", "to be a slice", "User")

				err = c.Query(ctx, []*User{}, `SELECT * FROM users WHERE name like `+c.dialect.Placeholder(0), "% Sá")
				tt.AssertErrContains(t, err, "expected", "slice of structs", "User")

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
				c := newTestDB(db, config.driver)
				var users []User
				err := c.Query(ctx, &users, `SELECT * FROM not a valid query`)
				tt.AssertErrContains(t, err, "error running query")
			})

			t.Run("should report error if using nested struct and the query starts with SELECT", func(t *testing.T) {
				db, closer := newDBAdapter(t)
				defer closer.Close()

				ctx := context.Background()
				c := newTestDB(db, config.driver)
				var rows []struct {
					User User `tablename:"users"`
					Post Post `tablename:"posts"`
				}
				err := c.Query(ctx, &rows, `SELECT * FROM users u JOIN posts p ON u.id = p.user_id`)
				tt.AssertErrContains(t, err, "nested struct", "feature")
			})

			t.Run("should report error for nested structs with invalid types", func(t *testing.T) {
				t.Run("int", func(t *testing.T) {
					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, config.driver)
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
					c := newTestDB(db, config.driver)
					var rows []struct {
						Foo *User `tablename:"foo"`
					}
					err := c.Query(ctx, &rows, fmt.Sprint(
						`FROM users u JOIN posts p ON p.user_id = u.id`,
						` WHERE u.name like `, c.dialect.Placeholder(0),
						` ORDER BY u.id, p.id`,
					), "% Ribeiro")

					tt.AssertErrContains(t, err, "foo", "*ksql.User")
				})
			})

			t.Run("should report error if nested struct is invalid", func(t *testing.T) {
				db, closer := newDBAdapter(t)
				defer closer.Close()

				ctx := context.Background()
				c := newTestDB(db, config.driver)
				var rows []struct {
					User User `tablename:"users"`
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

func TestQueryOne(t *testing.T) {
	for _, config := range supportedConfigs {
		QueryOneTest(t,
			config,
			func(t *testing.T) (DBAdapter, io.Closer) {
				db, close := connectDB(t, config)
				return db, close
			},
		)
	}
}

func QueryOneTest(
	t *testing.T,
	config testConfig,
	newDBAdapter func(t *testing.T) (DBAdapter, io.Closer),
) {
	t.Run(config.driver, func(t *testing.T) {
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
			err := createTables(config.driver)
			if err != nil {
				t.Fatal("could not create test table!, reason:", err.Error())
			}

			t.Run(variation.desc, func(t *testing.T) {
				t.Run("should return RecordNotFoundErr when there are no results", func(t *testing.T) {
					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, config.driver)
					u := User{}
					err := c.QueryOne(ctx, &u, variation.queryPrefix+`FROM users WHERE id=1;`)
					tt.AssertEqual(t, ErrRecordNotFound, err)
				})

				t.Run("should return a user correctly", func(t *testing.T) {
					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()

					_, err := db.ExecContext(ctx, `INSERT INTO users (name, age, address) VALUES ('Bia', 0, '{"country":"BR"}')`)
					tt.AssertNoErr(t, err)

					c := newTestDB(db, config.driver)
					u := User{}
					err = c.QueryOne(ctx, &u, variation.queryPrefix+`FROM users WHERE name=`+c.dialect.Placeholder(0), "Bia")

					tt.AssertNoErr(t, err)
					tt.AssertNotEqual(t, uint(0), u.ID)
					tt.AssertEqual(t, "Bia", u.Name)
					tt.AssertEqual(t, Address{
						Country: "BR",
					}, u.Address)
				})

				t.Run("should return only the first result on multiples matches", func(t *testing.T) {
					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()

					_, err := db.ExecContext(ctx, `INSERT INTO users (name, age, address) VALUES ('Andréa Sá', 0, '{"country":"US"}')`)
					tt.AssertNoErr(t, err)

					_, err = db.ExecContext(ctx, `INSERT INTO users (name, age, address) VALUES ('Caio Sá', 0, '{"country":"BR"}')`)
					tt.AssertNoErr(t, err)

					c := newTestDB(db, config.driver)

					var u User
					err = c.QueryOne(ctx, &u, variation.queryPrefix+`FROM users WHERE name like `+c.dialect.Placeholder(0)+` ORDER BY id ASC`, "% Sá")
					tt.AssertNoErr(t, err)
					tt.AssertEqual(t, "Andréa Sá", u.Name)
					tt.AssertEqual(t, 0, u.Age)
					tt.AssertEqual(t, Address{
						Country: "US",
					}, u.Address)
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
					var joao User
					getUserByName(db, config.driver, &joao, "João Ribeiro")

					_, err = db.ExecContext(ctx, fmt.Sprint(`INSERT INTO posts (user_id, title) VALUES (`, joao.ID, `, 'João Post1')`))
					tt.AssertNoErr(t, err)

					c := newTestDB(db, config.driver)
					var row struct {
						User User `tablename:"u"`
						Post Post `tablename:"p"`
					}
					err = c.QueryOne(ctx, &row, fmt.Sprint(
						`FROM users u JOIN posts p ON p.user_id = u.id`,
						` WHERE u.name like `, c.dialect.Placeholder(0),
						` ORDER BY u.id, p.id`,
					), "% Ribeiro")

					tt.AssertNoErr(t, err)
					tt.AssertEqual(t, joao.ID, row.User.ID)
					tt.AssertEqual(t, "João Ribeiro", row.User.Name)
					tt.AssertEqual(t, "João Post1", row.Post.Title)
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

			c := newTestDB(db, config.driver)

			err = c.QueryOne(ctx, &[]User{}, `SELECT * FROM users WHERE name like `+c.dialect.Placeholder(0), "% Sá")
			tt.AssertErrContains(t, err, "pointer to struct")

			err = c.QueryOne(ctx, User{}, `SELECT * FROM users WHERE name like `+c.dialect.Placeholder(0), "% Sá")
			tt.AssertErrContains(t, err, "pointer to struct")
		})

		t.Run("should report error if it receives a nil pointer to a struct", func(t *testing.T) {
			db, closer := newDBAdapter(t)
			defer closer.Close()

			ctx := context.Background()
			c := newTestDB(db, config.driver)
			var user *User
			err := c.QueryOne(ctx, user, `SELECT * FROM users`)
			tt.AssertErrContains(t, err, "expected a valid pointer", "received a nil pointer")
		})

		t.Run("should report error if the query is not valid", func(t *testing.T) {
			db, closer := newDBAdapter(t)
			defer closer.Close()

			ctx := context.Background()
			c := newTestDB(db, config.driver)
			var user User
			err := c.QueryOne(ctx, &user, `SELECT * FROM not a valid query`)
			tt.AssertErrContains(t, err, "error running query")
		})

		t.Run("should report error if using nested struct and the query starts with SELECT", func(t *testing.T) {
			db, closer := newDBAdapter(t)
			defer closer.Close()

			ctx := context.Background()
			c := newTestDB(db, config.driver)
			var row struct {
				User User `tablename:"users"`
				Post Post `tablename:"posts"`
			}
			err := c.QueryOne(ctx, &row, `SELECT * FROM users u JOIN posts p ON u.id = p.user_id LIMIT 1`)
			tt.AssertErrContains(t, err, "nested struct", "feature")
		})
	})
}

func TestInsert(t *testing.T) {
	for _, config := range supportedConfigs {
		InsertTest(t,
			config,
			func(t *testing.T) (DBAdapter, io.Closer) {
				db, close := connectDB(t, config)
				return db, close
			},
		)
	}
}

func InsertTest(
	t *testing.T,
	config testConfig,
	newDBAdapter func(t *testing.T) (DBAdapter, io.Closer),
) {
	t.Run(config.driver, func(t *testing.T) {
		t.Run("success cases", func(t *testing.T) {
			t.Run("single primary key tables", func(t *testing.T) {
				err := createTables(config.driver)
				if err != nil {
					t.Fatal("could not create test table!, reason:", err.Error())
				}

				t.Run("should insert one user correctly", func(t *testing.T) {
					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, config.driver)

					u := User{
						Name: "Fernanda",
						Address: Address{
							Country: "Brazil",
						},
					}

					err := c.Insert(ctx, UsersTable, &u)
					assert.Equal(t, nil, err)
					assert.NotEqual(t, 0, u.ID)

					result := User{}
					err = getUserByID(c.db, c.dialect, &result, u.ID)
					assert.Equal(t, nil, err)

					assert.Equal(t, u.Name, result.Name)
					assert.Equal(t, u.Address, result.Address)
				})

				t.Run("should insert ignoring the ID for sqlite and multiple ids", func(t *testing.T) {
					if supportedDialects[config.driver].InsertMethod() != insertWithLastInsertID {
						return
					}

					// Using columns "id" and "name" as IDs:
					table := NewTable("users", "id", "name")

					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, config.driver)

					u := User{
						Name: "No ID returned",
						Age:  3434, // Random number to avoid false positives on this test

						Address: Address{
							Country: "Brazil 3434",
						},
					}

					err = c.Insert(ctx, table, &u)
					assert.Equal(t, nil, err)
					assert.Equal(t, uint(0), u.ID)

					result := User{}
					err = getUserByName(c.db, config.driver, &result, "No ID returned")
					assert.Equal(t, nil, err)

					assert.Equal(t, u.Age, result.Age)
					assert.Equal(t, u.Address, result.Address)
				})

				t.Run("should work with anonymous structs", func(t *testing.T) {
					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, config.driver)
					err = c.Insert(ctx, UsersTable, &struct {
						ID      int                    `ksql:"id"`
						Name    string                 `ksql:"name"`
						Address map[string]interface{} `ksql:"address,json"`
					}{Name: "fake-name", Address: map[string]interface{}{"city": "bar"}})
					assert.Equal(t, nil, err)
				})

				t.Run("should work with preset IDs", func(t *testing.T) {
					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, config.driver)

					usersByName := NewTable("users", "name")

					err = c.Insert(ctx, usersByName, &struct {
						Name string `ksql:"name"`
						Age  int    `ksql:"age"`
					}{Name: "Preset Name", Age: 5455})
					assert.Equal(t, nil, err)

					var inserted User
					err := getUserByName(db, config.driver, &inserted, "Preset Name")
					assert.Equal(t, nil, err)
					assert.Equal(t, 5455, inserted.Age)
				})
			})

			t.Run("composite key tables", func(t *testing.T) {
				err := createTables(config.driver)
				if err != nil {
					t.Fatal("could not create test table!, reason:", err.Error())
				}

				t.Run("should insert in composite key tables correctly", func(t *testing.T) {
					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, config.driver)

					table := NewTable("user_permissions", "id", "user_id", "perm_id")
					err = c.Insert(ctx, table, &UserPermission{
						UserID: 1,
						PermID: 42,
					})
					tt.AssertNoErr(t, err)

					userPerms, err := getUserPermissionsByUser(db, config.driver, 1)
					tt.AssertNoErr(t, err)
					tt.AssertEqual(t, len(userPerms), 1)
					tt.AssertEqual(t, userPerms[0].UserID, 1)
					tt.AssertEqual(t, userPerms[0].PermID, 42)
				})

				t.Run("should accept partially provided values for composite key tables", func(t *testing.T) {
					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, config.driver)

					// Table defined with 3 values, but we'll provide only 2,
					// the third will be generated for the purposes of this test:
					table := NewTable("user_permissions", "id", "user_id", "perm_id")
					permission := UserPermission{
						UserID: 2,
						PermID: 42,
					}
					err = c.Insert(ctx, table, &permission)
					tt.AssertNoErr(t, err)

					userPerms, err := getUserPermissionsByUser(db, config.driver, 2)
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
			err := createTables(config.driver)
			if err != nil {
				t.Fatal("could not create test table!, reason:", err.Error())
			}

			t.Run("should report error for invalid input types", func(t *testing.T) {
				db, closer := newDBAdapter(t)
				defer closer.Close()

				ctx := context.Background()
				c := newTestDB(db, config.driver)

				err = c.Insert(ctx, UsersTable, "foo")
				assert.NotEqual(t, nil, err)

				err = c.Insert(ctx, UsersTable, nullable.String("foo"))
				assert.NotEqual(t, nil, err)

				err = c.Insert(ctx, UsersTable, map[string]interface{}{
					"name": "foo",
					"age":  12,
				})
				assert.NotEqual(t, nil, err)

				cantInsertSlice := []interface{}{
					&User{Name: "foo", Age: 22},
					&User{Name: "bar", Age: 32},
				}
				err = c.Insert(ctx, UsersTable, cantInsertSlice)
				assert.NotEqual(t, nil, err)

				// We might want to support this in the future, but not for now:
				err = c.Insert(ctx, UsersTable, User{Name: "not a ptr to user", Age: 42})
				assert.NotEqual(t, nil, err)
			})

			t.Run("should report error if for some reason the insertMethod is invalid", func(t *testing.T) {
				db, closer := newDBAdapter(t)
				defer closer.Close()

				ctx := context.Background()
				c := newTestDB(db, config.driver)

				// This is an invalid value:
				c.dialect = brokenDialect{}

				err = c.Insert(ctx, UsersTable, &User{Name: "foo"})
				assert.NotEqual(t, nil, err)
			})

			t.Run("should report error if it receives a nil pointer to a struct", func(t *testing.T) {
				db, closer := newDBAdapter(t)
				defer closer.Close()

				ctx := context.Background()
				c := newTestDB(db, config.driver)

				var user *User
				err := c.Insert(ctx, UsersTable, user)
				assert.NotEqual(t, nil, err)
			})

			t.Run("should report error if table contains an empty ID name", func(t *testing.T) {
				db, closer := newDBAdapter(t)
				defer closer.Close()

				ctx := context.Background()
				c := newTestDB(db, config.driver)

				err := c.Insert(ctx, NewTable("users", ""), &User{Name: "fake-name"})
				tt.AssertErrContains(t, err, "ksql.Table", "ID", "empty string")
			})

			t.Run("should report error if ksql.Table.name is empty", func(t *testing.T) {
				db, closer := newDBAdapter(t)
				defer closer.Close()

				ctx := context.Background()
				c := newTestDB(db, config.driver)

				err := c.Insert(ctx, NewTable("", "id"), &User{Name: "fake-name"})
				tt.AssertErrContains(t, err, "ksql.Table", "table name", "empty string")
			})

			t.Run("should not panic if a column doesn't exist in the database", func(t *testing.T) {
				db, closer := newDBAdapter(t)
				defer closer.Close()

				ctx := context.Background()
				c := newTestDB(db, config.driver)

				err = c.Insert(ctx, UsersTable, &struct {
					ID                string `ksql:"id"`
					NonExistingColumn int    `ksql:"non_existing"`
					Name              string `ksql:"name"`
				}{NonExistingColumn: 42, Name: "fake-name"})
				assert.NotEqual(t, nil, err)
				msg := err.Error()
				assert.Equal(t, true, strings.Contains(msg, "column"))
				assert.Equal(t, true, strings.Contains(msg, "non_existing"))
			})

			t.Run("should not panic if the ID column doesn't exist in the database", func(t *testing.T) {
				db, closer := newDBAdapter(t)
				defer closer.Close()

				ctx := context.Background()
				c := newTestDB(db, config.driver)

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
				c := newTestDB(db, config.driver)

				err = c.Insert(ctx, UsersTable, &struct {
					Age  int    `ksql:"age"`
					Name string `ksql:"name"`
				}{Age: 42, Name: "Inserted With no ID"})
				assert.Equal(t, nil, err)

				var u User
				err = getUserByName(db, config.driver, &u, "Inserted With no ID")
				assert.Equal(t, nil, err)
				assert.NotEqual(t, uint(0), u.ID)
				assert.Equal(t, 42, u.Age)
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

func TestDelete(t *testing.T) {
	for _, config := range supportedConfigs {
		DeleteTest(t,
			config,
			func(t *testing.T) (DBAdapter, io.Closer) {
				db, close := connectDB(t, config)
				return db, close
			},
		)
	}
}

func DeleteTest(
	t *testing.T,
	config testConfig,
	newDBAdapter func(t *testing.T) (DBAdapter, io.Closer),
) {
	t.Run(config.driver, func(t *testing.T) {
		err := createTables(config.driver)
		if err != nil {
			t.Fatal("could not create test table!, reason:", err.Error())
		}

		t.Run("should delete from tables with a single primary key correctly", func(t *testing.T) {
			tests := []struct {
				desc               string
				deletionKeyForUser func(u User) interface{}
			}{
				{
					desc: "passing only the ID as key",
					deletionKeyForUser: func(u User) interface{} {
						return u.ID
					},
				},
				{
					desc: "passing only the entire user",
					deletionKeyForUser: func(u User) interface{} {
						return u
					},
				},
				{
					desc: "passing the address of the user",
					deletionKeyForUser: func(u User) interface{} {
						return &u
					},
				},
			}

			for _, test := range tests {
				t.Run(test.desc, func(t *testing.T) {
					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, config.driver)

					u1 := User{
						Name: "Fernanda",
					}

					err := c.Insert(ctx, UsersTable, &u1)
					assert.Equal(t, nil, err)
					assert.NotEqual(t, uint(0), u1.ID)

					result := User{}
					err = getUserByID(c.db, c.dialect, &result, u1.ID)
					assert.Equal(t, nil, err)
					assert.Equal(t, u1.ID, result.ID)

					u2 := User{
						Name: "Won't be deleted",
					}

					err = c.Insert(ctx, UsersTable, &u2)
					assert.Equal(t, nil, err)
					assert.NotEqual(t, uint(0), u2.ID)

					result = User{}
					err = getUserByID(c.db, c.dialect, &result, u2.ID)
					assert.Equal(t, nil, err)
					assert.Equal(t, u2.ID, result.ID)

					err = c.Delete(ctx, UsersTable, test.deletionKeyForUser(u1))
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
			}
		})

		t.Run("should delete from tables with composite primary keys correctly", func(t *testing.T) {
			t.Run("using structs", func(t *testing.T) {
				db, closer := newDBAdapter(t)
				defer closer.Close()

				ctx := context.Background()
				c := newTestDB(db, config.driver)

				// This permission should not be deleted, we'll use the id to check it:
				p0 := UserPermission{
					UserID: 1,
					PermID: 44,
				}
				err = c.Insert(ctx, NewTable("user_permissions", "id"), &p0)
				tt.AssertNoErr(t, err)
				tt.AssertNotEqual(t, p0.ID, 0)

				p1 := UserPermission{
					UserID: 1,
					PermID: 42,
				}
				err = c.Insert(ctx, NewTable("user_permissions", "id"), &p1)
				tt.AssertNoErr(t, err)

				err = c.Delete(ctx, UserPermissionsTable, p1)
				tt.AssertNoErr(t, err)

				userPerms, err := getUserPermissionsByUser(db, config.driver, 1)
				tt.AssertNoErr(t, err)
				tt.AssertEqual(t, len(userPerms), 1)
				tt.AssertEqual(t, userPerms[0].UserID, 1)
				tt.AssertEqual(t, userPerms[0].PermID, 44)
			})

			t.Run("using maps", func(t *testing.T) {
				db, closer := newDBAdapter(t)
				defer closer.Close()

				ctx := context.Background()
				c := newTestDB(db, config.driver)

				// This permission should not be deleted, we'll use the id to check it:
				p0 := UserPermission{
					UserID: 2,
					PermID: 44,
				}
				err = c.Insert(ctx, NewTable("user_permissions", "id"), &p0)
				tt.AssertNoErr(t, err)
				tt.AssertNotEqual(t, p0.ID, 0)

				p1 := UserPermission{
					UserID: 2,
					PermID: 42,
				}
				err = c.Insert(ctx, NewTable("user_permissions", "id"), &p1)
				tt.AssertNoErr(t, err)

				err = c.Delete(ctx, UserPermissionsTable, map[string]interface{}{
					"user_id": 2,
					"perm_id": 42,
				})
				tt.AssertNoErr(t, err)

				userPerms, err := getUserPermissionsByUser(db, config.driver, 2)
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
			c := newTestDB(db, config.driver)

			err = c.Delete(ctx, UsersTable, 4200)
			assert.Equal(t, ErrRecordNotFound, err)
		})

		t.Run("should report error if it receives a nil pointer to a struct", func(t *testing.T) {
			db, closer := newDBAdapter(t)
			defer closer.Close()

			ctx := context.Background()
			c := newTestDB(db, config.driver)

			var user *User
			err := c.Delete(ctx, UsersTable, user)
			assert.NotEqual(t, nil, err)
		})

		t.Run("should report error if one of the ids is missing from the input", func(t *testing.T) {
			t.Run("single id", func(t *testing.T) {
				t.Run("struct with missing attr", func(t *testing.T) {
					db, closer := newDBAdapter(t)
					defer closer.Close()

					ctx := context.Background()
					c := newTestDB(db, config.driver)

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
					c := newTestDB(db, config.driver)

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
					c := newTestDB(db, config.driver)

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
					c := newTestDB(db, config.driver)

					err := c.Delete(ctx, NewTable("user_permissions", "user_id", "perm_id"), map[string]interface{}{
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
					c := newTestDB(db, config.driver)

					err := c.Delete(ctx, NewTable("user_permissions", "user_id", "perm_id"), map[string]interface{}{
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
					c := newTestDB(db, config.driver)

					err := c.Delete(ctx, NewTable("user_permissions", "user_id", "perm_id"), map[string]interface{}{
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
			c := newTestDB(db, config.driver)

			err := c.Delete(ctx, NewTable("users", ""), &User{ID: 42, Name: "fake-name"})
			tt.AssertErrContains(t, err, "ksql.Table", "ID", "empty string")
		})

		t.Run("should report error if ksql.Table.name is empty", func(t *testing.T) {
			db, closer := newDBAdapter(t)
			defer closer.Close()

			ctx := context.Background()
			c := newTestDB(db, config.driver)

			err := c.Delete(ctx, NewTable("", "id"), &User{Name: "fake-name"})
			tt.AssertErrContains(t, err, "ksql.Table", "table name", "empty string")
		})
	})
}

func TestUpdate(t *testing.T) {
	for _, config := range supportedConfigs {
		UpdateTest(t,
			config,
			func(t *testing.T) (DBAdapter, io.Closer) {
				db, close := connectDB(t, config)
				return db, close
			},
		)
	}
}

func UpdateTest(
	t *testing.T,
	config testConfig,
	newDBAdapter func(t *testing.T) (DBAdapter, io.Closer),
) {
	t.Run(config.driver, func(t *testing.T) {
		err := createTables(config.driver)
		if err != nil {
			t.Fatal("could not create test table!, reason:", err.Error())
		}

		t.Run("should update one User{} correctly", func(t *testing.T) {
			db, closer := newDBAdapter(t)
			defer closer.Close()

			ctx := context.Background()
			c := newTestDB(db, config.driver)

			u := User{
				Name: "Letícia",
			}
			_, err := db.ExecContext(ctx, `INSERT INTO users (name, age) VALUES ('Letícia', 0)`)
			assert.Equal(t, nil, err)

			err = getUserByName(db, config.driver, &u, "Letícia")
			assert.Equal(t, nil, err)
			assert.NotEqual(t, uint(0), u.ID)

			err = c.Update(ctx, UsersTable, User{
				ID:   u.ID,
				Name: "Thayane",
			})
			assert.Equal(t, nil, err)

			var result User
			err = getUserByID(c.db, c.dialect, &result, u.ID)
			assert.Equal(t, nil, err)
			assert.Equal(t, "Thayane", result.Name)
		})

		t.Run("should update one &User{} correctly", func(t *testing.T) {
			db, closer := newDBAdapter(t)
			defer closer.Close()

			ctx := context.Background()
			c := newTestDB(db, config.driver)

			u := User{
				Name: "Letícia",
			}
			_, err := db.ExecContext(ctx, `INSERT INTO users (name, age) VALUES ('Letícia', 0)`)
			assert.Equal(t, nil, err)

			err = getUserByName(db, config.driver, &u, "Letícia")
			assert.Equal(t, nil, err)
			assert.NotEqual(t, uint(0), u.ID)

			err = c.Update(ctx, UsersTable, &User{
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
			db, closer := newDBAdapter(t)
			defer closer.Close()

			ctx := context.Background()
			c := newTestDB(db, config.driver)

			type partialUser struct {
				ID   uint   `ksql:"id"`
				Name string `ksql:"name"`
				Age  *int   `ksql:"age"`
			}

			_, err := db.ExecContext(ctx, `INSERT INTO users (name, age) VALUES ('Letícia', 22)`)
			assert.Equal(t, nil, err)

			var u User
			err = getUserByName(db, config.driver, &u, "Letícia")
			assert.Equal(t, nil, err)
			assert.NotEqual(t, uint(0), u.ID)

			err = c.Update(ctx, UsersTable, partialUser{
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
			db, closer := newDBAdapter(t)
			defer closer.Close()

			ctx := context.Background()
			c := newTestDB(db, config.driver)

			type partialUser struct {
				ID   uint   `ksql:"id"`
				Name string `ksql:"name"`
				Age  *int   `ksql:"age"`
			}

			_, err := db.ExecContext(ctx, `INSERT INTO users (name, age) VALUES ('Letícia', 22)`)
			assert.Equal(t, nil, err)

			var u User
			err = getUserByName(db, config.driver, &u, "Letícia")
			assert.Equal(t, nil, err)
			assert.NotEqual(t, uint(0), u.ID)

			// Should update all fields:
			err = c.Update(ctx, UsersTable, partialUser{
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

		t.Run("should return ErrRecordNotFound when asked to update an inexistent user", func(t *testing.T) {
			db, closer := newDBAdapter(t)
			defer closer.Close()

			ctx := context.Background()
			c := newTestDB(db, config.driver)

			err = c.Update(ctx, UsersTable, User{
				ID:   4200,
				Name: "Thayane",
			})
			assert.Equal(t, ErrRecordNotFound, err)
		})

		t.Run("should report database errors correctly", func(t *testing.T) {
			db, closer := newDBAdapter(t)
			defer closer.Close()

			ctx := context.Background()
			c := newTestDB(db, config.driver)

			err = c.Update(ctx, NewTable("non_existing_table"), User{
				ID:   1,
				Name: "Thayane",
			})
			assert.NotEqual(t, nil, err)
		})

		t.Run("should report error if it receives a nil pointer to a struct", func(t *testing.T) {
			db, closer := newDBAdapter(t)
			defer closer.Close()

			ctx := context.Background()
			c := newTestDB(db, config.driver)

			var user *User
			err := c.Update(ctx, UsersTable, user)
			assert.NotEqual(t, nil, err)
		})
	})
}

func TestQueryChunks(t *testing.T) {
	for _, config := range supportedConfigs {
		t.Run(config.driver, func(t *testing.T) {
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
						err := createTables(config.driver)
						if err != nil {
							t.Fatal("could not create test table!, reason:", err.Error())
						}

						db, closer := connectDB(t, config)
						defer closer.Close()

						ctx := context.Background()
						c := newTestDB(db, config.driver)

						_ = c.Insert(ctx, UsersTable, &User{
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
						err := createTables(config.driver)
						if err != nil {
							t.Fatal("could not create test table!, reason:", err.Error())
						}

						db, closer := connectDB(t, config)
						defer closer.Close()

						ctx := context.Background()
						c := newTestDB(db, config.driver)

						_ = c.Insert(ctx, UsersTable, &User{Name: "User1", Address: Address{Country: "US"}})
						_ = c.Insert(ctx, UsersTable, &User{Name: "User2", Address: Address{Country: "BR"}})

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
						err := createTables(config.driver)
						if err != nil {
							t.Fatal("could not create test table!, reason:", err.Error())
						}

						db, closer := connectDB(t, config)
						defer closer.Close()

						ctx := context.Background()
						c := newTestDB(db, config.driver)

						_ = c.Insert(ctx, UsersTable, &User{Name: "User1", Address: Address{Country: "US"}})
						_ = c.Insert(ctx, UsersTable, &User{Name: "User2", Address: Address{Country: "BR"}})

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
						err := createTables(config.driver)
						if err != nil {
							t.Fatal("could not create test table!, reason:", err.Error())
						}

						db, closer := connectDB(t, config)
						defer closer.Close()

						ctx := context.Background()
						c := newTestDB(db, config.driver)

						_ = c.Insert(ctx, UsersTable, &User{Name: "User1"})
						_ = c.Insert(ctx, UsersTable, &User{Name: "User2"})
						_ = c.Insert(ctx, UsersTable, &User{Name: "User3"})

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

						db, closer := connectDB(t, config)
						defer closer.Close()

						joao := User{
							Name: "Thiago Ribeiro",
							Age:  24,
						}
						thatiana := User{
							Name: "Thatiana Ribeiro",
							Age:  20,
						}

						ctx := context.Background()
						c := newTestDB(db, config.driver)
						_ = c.Insert(ctx, UsersTable, &joao)
						_ = c.Insert(ctx, UsersTable, &thatiana)

						_, err := db.ExecContext(ctx, fmt.Sprint(`INSERT INTO posts (user_id, title) VALUES (`, thatiana.ID, `, 'Thatiana Post1')`))
						assert.Equal(t, nil, err)
						_, err = db.ExecContext(ctx, fmt.Sprint(`INSERT INTO posts (user_id, title) VALUES (`, thatiana.ID, `, 'Thatiana Post2')`))
						assert.Equal(t, nil, err)
						_, err = db.ExecContext(ctx, fmt.Sprint(`INSERT INTO posts (user_id, title) VALUES (`, joao.ID, `, 'Thiago Post1')`))
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
						err := createTables(config.driver)
						if err != nil {
							t.Fatal("could not create test table!, reason:", err.Error())
						}

						db, closer := connectDB(t, config)
						defer closer.Close()

						ctx := context.Background()
						c := newTestDB(db, config.driver)

						_ = c.Insert(ctx, UsersTable, &User{Name: "User1"})
						_ = c.Insert(ctx, UsersTable, &User{Name: "User2"})
						_ = c.Insert(ctx, UsersTable, &User{Name: "User3"})

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
						err := createTables(config.driver)
						if err != nil {
							t.Fatal("could not create test table!, reason:", err.Error())
						}

						db, closer := connectDB(t, config)
						defer closer.Close()

						ctx := context.Background()
						c := newTestDB(db, config.driver)

						_ = c.Insert(ctx, UsersTable, &User{Name: "User1"})
						_ = c.Insert(ctx, UsersTable, &User{Name: "User2"})
						_ = c.Insert(ctx, UsersTable, &User{Name: "User3"})

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
						err := createTables(config.driver)
						if err != nil {
							t.Fatal("could not create test table!, reason:", err.Error())
						}

						db, closer := connectDB(t, config)
						defer closer.Close()

						ctx := context.Background()
						c := newTestDB(db, config.driver)

						_ = c.Insert(ctx, UsersTable, &User{Name: "User1"})
						_ = c.Insert(ctx, UsersTable, &User{Name: "User2"})
						_ = c.Insert(ctx, UsersTable, &User{Name: "User3"})

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
						err := createTables(config.driver)
						if err != nil {
							t.Fatal("could not create test table!, reason:", err.Error())
						}

						db, closer := connectDB(t, config)
						defer closer.Close()

						ctx := context.Background()
						c := newTestDB(db, config.driver)

						_ = c.Insert(ctx, UsersTable, &User{Name: "User1"})
						_ = c.Insert(ctx, UsersTable, &User{Name: "User2"})
						_ = c.Insert(ctx, UsersTable, &User{Name: "User3"})

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
						db, closer := connectDB(t, config)
						defer closer.Close()

						ctx := context.Background()
						c := newTestDB(db, config.driver)

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
						db, closer := connectDB(t, config)
						defer closer.Close()

						ctx := context.Background()
						c := newTestDB(db, config.driver)
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

					t.Run("should report error if using nested struct and the query starts with SELECT", func(t *testing.T) {
						db, closer := connectDB(t, config)
						defer closer.Close()

						ctx := context.Background()
						c := newTestDB(db, config.driver)

						err := c.QueryChunks(ctx, ChunkParser{
							Query:  `SELECT * FROM users u JOIN posts p ON u.id = p.user_id`,
							Params: []interface{}{},

							ChunkSize: 2,
							ForEachChunk: func(buffer []struct {
								User User `tablename:"users"`
								Post Post `tablename:"posts"`
							}) error {
								return nil
							},
						})

						assert.NotEqual(t, nil, err)
						assert.Equal(t, true, strings.Contains(err.Error(), "nested struct"), "unexpected error msg: "+err.Error())
						assert.Equal(t, true, strings.Contains(err.Error(), "feature"), "unexpected error msg: "+err.Error())
					})
				})
			}
		})
	}
}

func TestTransaction(t *testing.T) {
	for _, config := range supportedConfigs {
		t.Run(config.driver, func(t *testing.T) {
			t.Run("should query a single row correctly", func(t *testing.T) {
				err := createTables(config.driver)
				if err != nil {
					t.Fatal("could not create test table!, reason:", err.Error())
				}

				db, closer := connectDB(t, config)
				defer closer.Close()

				ctx := context.Background()
				c := newTestDB(db, config.driver)

				_ = c.Insert(ctx, UsersTable, &User{Name: "User1"})
				_ = c.Insert(ctx, UsersTable, &User{Name: "User2"})

				var users []User
				err = c.Transaction(ctx, func(db Provider) error {
					db.Query(ctx, &users, "SELECT * FROM users ORDER BY id ASC")
					return nil
				})
				assert.Equal(t, nil, err)

				assert.Equal(t, 2, len(users))
				assert.Equal(t, "User1", users[0].Name)
				assert.Equal(t, "User2", users[1].Name)
			})

			t.Run("should rollback when there are errors", func(t *testing.T) {
				err := createTables(config.driver)
				if err != nil {
					t.Fatal("could not create test table!, reason:", err.Error())
				}

				db, closer := connectDB(t, config)
				defer closer.Close()

				ctx := context.Background()
				c := newTestDB(db, config.driver)

				u1 := User{Name: "User1", Age: 42}
				u2 := User{Name: "User2", Age: 42}
				_ = c.Insert(ctx, UsersTable, &u1)
				_ = c.Insert(ctx, UsersTable, &u2)

				err = c.Transaction(ctx, func(db Provider) error {
					err = db.Insert(ctx, UsersTable, &User{Name: "User3"})
					assert.Equal(t, nil, err)
					err = db.Insert(ctx, UsersTable, &User{Name: "User4"})
					assert.Equal(t, nil, err)
					_, err = db.Exec(ctx, "UPDATE users SET age = 22")
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
		db, closer := connectDB(t, testConfig{
			driver:      "sqlite3",
			adapterName: "sql",
		})
		defer closer.Close()
		c := newTestDB(db, "sqlite3")
		_ = c.Insert(ctx, UsersTable, &User{Name: "User1", Age: 22})
		_ = c.Insert(ctx, UsersTable, &User{Name: "User2", Age: 14})
		_ = c.Insert(ctx, UsersTable, &User{Name: "User3", Age: 43})

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
		db, closer := connectDB(t, testConfig{
			driver:      "sqlite3",
			adapterName: "sql",
		})
		defer closer.Close()
		c := newTestDB(db, "sqlite3")
		_ = c.Insert(ctx, UsersTable, &User{Name: "User1", Age: 22})

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
		db, closer := connectDB(t, testConfig{
			driver:      "sqlite3",
			adapterName: "sql",
		})
		defer closer.Close()

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
		db, closer := connectDB(t, testConfig{
			driver:      "sqlite3",
			adapterName: "sql",
		})
		defer closer.Close()

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
		db, closer := connectDB(t, testConfig{
			driver:      "sqlite3",
			adapterName: "sql",
		})
		defer closer.Close()

		rows, err := db.QueryContext(ctx, "select * from users where name='User2'")
		assert.Equal(t, nil, err)

		var u map[string]interface{}
		err = scanRows(dialect, rows, &u)
		assert.NotEqual(t, nil, err)
	})
}

func TestConfigSetDefaultValues(t *testing.T) {
	config := Config{}
	config.SetDefaultValues()

	assert.Equal(t, config.MaxOpenConns, 1)
}

func TestNewAdapterWith(t *testing.T) {
	t.Run("should build new instances correctly", func(t *testing.T) {
		for dialectName := range supportedDialects {
			db, err := NewWithAdapter(
				DBAdapter(nil),
				dialectName,
			)

			assert.Equal(t, nil, err)
			assert.Equal(t, supportedDialects[dialectName], db.dialect)
			assert.Equal(t, dialectName, db.driver)
		}
	})

	t.Run("should report invalid dialectNames correctly", func(t *testing.T) {
		_, err := NewWithAdapter(
			DBAdapter(nil),
			"fake-dialect-name",
		)

		assert.NotEqual(t, nil, err)
	})
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
		return fmt.Errorf("failed to create new posts table: %s", err.Error())
	}

	db.Exec(`DROP TABLE user_permissions`)

	switch driver {
	case "sqlite3":
		_, err = db.Exec(`CREATE TABLE user_permissions (
			id INTEGER PRIMARY KEY,
			user_id INTEGER,
			perm_id INTEGER,
			UNIQUE (user_id, perm_id)
		)`)
	case "postgres":
		_, err = db.Exec(`CREATE TABLE user_permissions (
			id serial PRIMARY KEY,
			user_id INT,
			perm_id INT,
			UNIQUE (user_id, perm_id)
		)`)
	case "mysql":
		_, err = db.Exec(`CREATE TABLE user_permissions (
			id INT AUTO_INCREMENT PRIMARY KEY,
			user_id INT,
			perm_id INT,
			UNIQUE KEY (user_id, perm_id)
		)`)
	case "sqlserver":
		_, err = db.Exec(`CREATE TABLE user_permissions (
			id INT IDENTITY(1,1) PRIMARY KEY,
			user_id INT,
			perm_id INT,
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

type CloserAdapter struct {
	close func()
}

func (c CloserAdapter) Close() error {
	c.close()
	return nil
}

func connectDB(t *testing.T, config testConfig) (DBAdapter, io.Closer) {
	connStr := connectionString[config.driver]
	if connStr == "" {
		panic(fmt.Sprintf("unsupported driver: '%s'", config.driver))
	}

	switch config.adapterName {
	case "sql":
		db, err := sql.Open(config.driver, connStr)
		if err != nil {
			t.Fatal(err.Error())
		}
		return SQLAdapter{db}, db
	case "pgx":
		pool, err := pgxpool.Connect(context.TODO(), connStr)
		if err != nil {
			t.Fatal(err.Error())
		}
		return PGXAdapter{pool}, CloserAdapter{close: pool.Close}
	}

	t.Fatalf("unsupported adapter: %s", config.adapterName)
	return nil, nil
}

func shiftErrSlice(errs *[]error) error {
	err := (*errs)[0]
	*errs = (*errs)[1:]
	return err
}

func getUsersByID(db DBAdapter, dialect Dialect, resultsPtr *[]User, ids ...uint) error {
	placeholders := make([]string, len(ids))
	params := make([]interface{}, len(ids))
	for i := range ids {
		params[i] = ids[i]
		placeholders[i] = dialect.Placeholder(i)
	}

	results := []User{}
	rows, err := db.QueryContext(
		context.TODO(),
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

func getUserByID(db DBAdapter, dialect Dialect, result *User, id uint) error {
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

func getUserByName(db DBAdapter, driver string, result *User, name string) error {
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

func getUserPermissionsByUser(db DBAdapter, driver string, userID int) (results []UserPermission, _ error) {
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
		var userPerm UserPermission
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
