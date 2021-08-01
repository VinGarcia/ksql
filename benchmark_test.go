package ksql_test

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"testing"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/vingarcia/ksql"
)

var UsersTable = ksql.NewTable("users")

func BenchmarkInsert(b *testing.B) {
	ctx := context.Background()

	driver := "postgres"
	connStr := "host=localhost port=5432 user=postgres password=postgres dbname=ksql sslmode=disable"

	ksqlDB, err := ksql.New(driver, connStr, ksql.Config{
		MaxOpenConns: 1,
	})
	if err != nil {
		b.FailNow()
	}

	type User struct {
		ID   int    `ksql:"id" db:"id"`
		Name string `ksql:"name" db:"name"`
		Age  int    `ksql:"age" db:"age"`
	}

	b.Run("ksql-setup", func(b *testing.B) {
		err := recreateTable(connStr)
		if err != nil {
			b.Fatalf("error creating table: %s", err.Error())
		}

		b.Run("insert-one", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				err := ksqlDB.Insert(ctx, UsersTable, &User{
					Name: strconv.Itoa(i),
					Age:  i,
				})
				if err != nil {
					b.Fatalf("insert error: %s", err.Error())
				}
			}
		})
	})

	pgxDB, err := ksql.NewWithPGX(ctx, connStr, ksql.Config{
		MaxOpenConns: 1,
	})
	if err != nil {
		b.Fatalf("error creating pgx client: %s", err)
	}

	b.Run("pgx-adapter-setup", func(b *testing.B) {
		err := recreateTable(connStr)
		if err != nil {
			b.Fatalf("error creating table: %s", err.Error())
		}

		b.Run("insert-one", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				err := pgxDB.Insert(ctx, UsersTable, &User{
					Name: strconv.Itoa(i),
					Age:  i,
				})
				if err != nil {
					b.Fatalf("insert error: %s", err.Error())
				}
			}
		})
	})

	sqlxDB, err := sqlx.Open(driver, connStr)
	sqlxDB.SetMaxOpenConns(1)

	b.Run("sqlx-setup", func(b *testing.B) {
		err := recreateTable(connStr)
		if err != nil {
			b.Fatalf("error creating table: %s", err.Error())
		}

		query := `INSERT INTO users(name, age) VALUES (:name, :age) RETURNING id`
		b.Run("insert-one", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				user := User{
					Name: strconv.Itoa(i),
					Age:  i,
				}
				rows, err := sqlxDB.NamedQuery(query, user)
				if err != nil {
					b.Fatalf("insert error: %s", err.Error())
				}
				if !rows.Next() {
					b.Fatalf("missing id from inserted record")
				}
				rows.Scan(&user.ID)
				err = rows.Close()
				if err != nil {
					b.Fatalf("error closing rows")
				}
			}
		})
	})
}

func BenchmarkQuery(b *testing.B) {
	ctx := context.Background()

	driver := "postgres"
	connStr := "host=localhost port=5432 user=postgres password=postgres dbname=ksql sslmode=disable"

	ksqlDB, err := ksql.New(driver, connStr, ksql.Config{
		MaxOpenConns: 1,
	})
	if err != nil {
		b.FailNow()
	}

	type User struct {
		ID   int    `ksql:"id" db:"id"`
		Name string `ksql:"name" db:"name"`
		Age  int    `ksql:"age" db:"age"`
	}

	b.Run("ksql-setup", func(b *testing.B) {
		err := recreateTable(connStr)
		if err != nil {
			b.Fatalf("error creating table: %s", err.Error())
		}

		err = insertUsers(connStr, 100)
		if err != nil {
			b.Fatalf("error inserting users: %s", err.Error())
		}

		b.Run("single-row", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				var user User
				err := ksqlDB.QueryOne(ctx, &user, `SELECT * FROM users OFFSET $1 LIMIT 1`, i%100)
				if err != nil {
					b.Fatalf("query error: %s", err.Error())
				}
			}
		})

		b.Run("multiple-rows", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				var users []User
				err := ksqlDB.Query(ctx, &users, `SELECT * FROM users OFFSET $1 LIMIT 10`, i%90)
				if err != nil {
					b.Fatalf("query error: %s", err.Error())
				}
				if len(users) < 10 {
					b.Fatalf("expected 10 scanned users, but got: %d", len(users))
				}
			}
		})
	})

	pgxDB, err := ksql.NewWithPGX(ctx, connStr, ksql.Config{
		MaxOpenConns: 1,
	})
	if err != nil {
		b.Fatalf("error creating pgx client: %s", err)
	}

	b.Run("pgx-adapter-setup", func(b *testing.B) {
		err := recreateTable(connStr)
		if err != nil {
			b.Fatalf("error creating table: %s", err.Error())
		}

		err = insertUsers(connStr, 100)
		if err != nil {
			b.Fatalf("error inserting users: %s", err.Error())
		}

		b.Run("single-row", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				var user User
				err := pgxDB.QueryOne(ctx, &user, `SELECT * FROM users OFFSET $1 LIMIT 1`, i%100)
				if err != nil {
					b.Fatalf("query error: %s", err.Error())
				}
			}
		})

		b.Run("multiple-rows", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				var users []User
				err := pgxDB.Query(ctx, &users, `SELECT * FROM users OFFSET $1 LIMIT 10`, i%90)
				if err != nil {
					b.Fatalf("query error: %s", err.Error())
				}
				if len(users) < 10 {
					b.Fatalf("expected 10 scanned users, but got: %d", len(users))
				}
			}
		})
	})

	sqlxDB, err := sqlx.Open(driver, connStr)
	sqlxDB.SetMaxOpenConns(1)

	b.Run("sqlx-setup", func(b *testing.B) {
		err := recreateTable(connStr)
		if err != nil {
			b.Fatalf("error creating table: %s", err.Error())
		}

		err = insertUsers(connStr, 100)
		if err != nil {
			b.Fatalf("error inserting users: %s", err.Error())
		}

		b.Run("single-row", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				var user User
				rows, err := sqlxDB.Queryx(`SELECT * FROM users OFFSET $1 LIMIT 1`, i%100)
				if err != nil {
					b.Fatalf("insert error: %s", err.Error())
				}
				if !rows.Next() {
					b.Fatalf("missing user from inserted record, offset: %d", i%100)
				}
				rows.StructScan(&user)
				err = rows.Close()
				if err != nil {
					b.Fatalf("error closing rows")
				}
			}
		})

		b.Run("multiple-rows", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				var users []User
				rows, err := sqlxDB.Queryx(`SELECT * FROM users OFFSET $1 LIMIT 10`, i%90)
				if err != nil {
					b.Fatalf("insert error: %s", err.Error())
				}
				for j := 0; j < 10; j++ {
					if !rows.Next() {
						b.Fatalf("missing user from inserted record, offset: %d", i%100)
					}
					var user User
					rows.StructScan(&user)
					users = append(users, user)
				}
				if len(users) < 10 {
					b.Fatalf("expected 10 scanned users, but got: %d", len(users))
				}

				err = rows.Close()
				if err != nil {
					b.Fatalf("error closing rows")
				}
			}
		})
	})
}

func recreateTable(connStr string) error {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return err
	}
	defer db.Close()

	db.Exec(`DROP TABLE users`)

	_, err = db.Exec(`CREATE TABLE users (
		  id serial PRIMARY KEY,
			age INT,
			name VARCHAR(50)
		)`)
	if err != nil {
		return fmt.Errorf("failed to create new users table: %s", err.Error())
	}

	return nil
}

func insertUsers(connStr string, numUsers int) error {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return err
	}
	defer db.Close()

	for i := 0; i < numUsers; i++ {
		_, err = db.Exec(`INSERT INTO users (name, age) VALUES ($1, $2)`, strconv.Itoa(i), i)
		if err != nil {
			return fmt.Errorf("failed to insert new user: %s", err.Error())
		}
	}

	return nil
}
