package kissorm_test

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"testing"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/vingarcia/kissorm"
)

func BenchmarkInsert(b *testing.B) {
	ctx := context.Background()

	driver := "postgres"
	connStr := "host=localhost port=5432 user=postgres password=postgres dbname=kissorm sslmode=disable"

	kissormDB, err := kissorm.New(driver, connStr, 1, "users")
	if err != nil {
		b.FailNow()
	}

	type User struct {
		ID   int    `kissorm:"id" db:"id"`
		Name string `kissorm:"name" db:"name"`
		Age  int    `kissorm:"age" db:"age"`
	}

	b.Run("kissorm-setup", func(b *testing.B) {
		err := recreateTable(connStr)
		if err != nil {
			b.Fatalf("error creating table: %s", err.Error())
		}

		b.Run("insert-one", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				err := kissormDB.Insert(ctx, &User{
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
	connStr := "host=localhost port=5432 user=postgres password=postgres dbname=kissorm sslmode=disable"

	kissormDB, err := kissorm.New(driver, connStr, 1, "users")
	if err != nil {
		b.FailNow()
	}

	type User struct {
		ID   int    `kissorm:"id" db:"id"`
		Name string `kissorm:"name" db:"name"`
		Age  int    `kissorm:"age" db:"age"`
	}

	b.Run("kissorm-setup", func(b *testing.B) {
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
				err := kissormDB.QueryOne(ctx, &user, `SELECT * FROM users OFFSET $1 LIMIT 1`, i%100)
				if err != nil {
					b.Fatalf("query error: %s", err.Error())
				}
			}
		})

		b.Run("multiple-rows", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				var users []User
				err := kissormDB.Query(ctx, &users, `SELECT * FROM users OFFSET $1 LIMIT 10`, i%90)
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
