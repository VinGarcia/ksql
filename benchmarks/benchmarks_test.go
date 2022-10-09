package benchmarks

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"testing"

	_ "embed"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/vingarcia/ksql"
	"github.com/vingarcia/ksql/adapters/kpgx"
	"github.com/vingarcia/ksql/benchmarks/sqlboilergen"
	"github.com/vingarcia/ksql/benchmarks/sqlcgen"
	"github.com/volatiletech/sqlboiler/v4/boil"
	"github.com/volatiletech/sqlboiler/v4/queries/qm"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var UsersTable = ksql.NewTable("users")

func BenchmarkInsert(b *testing.B) {
	ctx := context.Background()

	driver := "postgres"
	connStr := "host=localhost port=5432 user=postgres password=postgres dbname=ksql sslmode=disable"

	type User struct {
		ID   int    `ksql:"id" db:"id"`
		Name string `ksql:"name" db:"name"`
		Age  int    `ksql:"age" db:"age"`
	}

	b.Run("ksql/sql-adapter", func(b *testing.B) {
		db, err := sql.Open(driver, connStr)
		if err != nil {
			b.Fatalf("error connecting to database: %s", err)
		}
		db.SetMaxOpenConns(1)
		ksqlDB, err := ksql.NewWithAdapter(NewSQLAdapter(db), driver)
		if err != nil {
			b.Fatalf("error creating ksql client: %s", err)
		}

		err = recreateTable(connStr)
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

	b.Run("ksql/pgx-adapter", func(b *testing.B) {
		kpgxDB, err := kpgx.New(ctx, connStr, ksql.Config{
			MaxOpenConns: 1,
		})
		if err != nil {
			b.Fatalf("error creating kpgx client: %s", err)
		}

		err = recreateTable(connStr)
		if err != nil {
			b.Fatalf("error creating table: %s", err.Error())
		}

		b.Run("insert-one", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				err := kpgxDB.Insert(ctx, UsersTable, &User{
					Name: strconv.Itoa(i),
					Age:  i,
				})
				if err != nil {
					b.Fatalf("insert error: %s", err.Error())
				}
			}
		})
	})

	b.Run("sql", func(b *testing.B) {
		sqlDB, err := sql.Open(driver, connStr)
		if err != nil {
			b.Fatalf("error creating sql client: %s", err)
		}
		sqlDB.SetMaxOpenConns(1)

		err = recreateTable(connStr)
		if err != nil {
			b.Fatalf("error creating table: %s", err.Error())
		}

		b.Run("insert-one", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				user := User{
					Name: strconv.Itoa(i),
					Age:  i,
				}
				rows, err := sqlDB.QueryContext(ctx,
					`INSERT INTO users(name, age) VALUES ($1, $2) RETURNING id`,
					user.Name, user.Age,
				)
				if err != nil {
					b.Fatalf("insert error: %s", err.Error())
				}
				if !rows.Next() {
					b.Fatalf("missing id from inserted record")
				}
				err = rows.Scan(&user.ID)
				if err != nil {
					b.Fatalf("error scanning rows")
				}
				err = rows.Close()
				if err != nil {
					b.Fatalf("error closing rows")
				}
			}
		})
	})

	b.Run("sql/prep-stmt", func(b *testing.B) {
		sqlDB, err := sql.Open(driver, connStr)
		if err != nil {
			b.Fatalf("error creating sql client: %s", err)
		}
		sqlDB.SetMaxOpenConns(1)

		err = recreateTable(connStr)
		if err != nil {
			b.Fatalf("error creating table: %s", err.Error())
		}

		insertOne, err := sqlDB.Prepare(`INSERT INTO users(name, age) VALUES ($1, $2) RETURNING id`)
		if err != nil {
			b.Fatalf("could not prepare sql insert query: %s", err.Error())
		}

		b.Run("insert-one", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				user := User{
					Name: strconv.Itoa(i),
					Age:  i,
				}
				rows, err := insertOne.QueryContext(ctx, user.Name, user.Age)
				if err != nil {
					b.Fatalf("insert error: %s", err.Error())
				}
				if !rows.Next() {
					b.Fatalf("missing id from inserted record")
				}
				err = rows.Scan(&user.ID)
				if err != nil {
					b.Fatalf("error scanning rows")
				}
				err = rows.Close()
				if err != nil {
					b.Fatalf("error closing rows")
				}
			}
		})
	})

	b.Run("sqlx", func(b *testing.B) {
		sqlxDB, err := sqlx.Open(driver, connStr)
		if err != nil {
			b.Fatalf("error creating sqlx client: %s", err)
		}
		sqlxDB.SetMaxOpenConns(1)

		err = recreateTable(connStr)
		if err != nil {
			b.Fatalf("error creating table: %s", err.Error())
		}

		b.Run("insert-one", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				user := User{
					Name: strconv.Itoa(i),
					Age:  i,
				}
				rows, err := sqlxDB.NamedQueryContext(ctx,
					`INSERT INTO users(name, age) VALUES (:name, :age) RETURNING id`,
					user,
				)
				if err != nil {
					b.Fatalf("insert error: %s", err.Error())
				}
				if !rows.Next() {
					b.Fatalf("missing id from inserted record")
				}
				err = rows.Scan(&user.ID)
				if err != nil {
					b.Fatalf("error scanning rows")
				}
				err = rows.Close()
				if err != nil {
					b.Fatalf("error closing rows")
				}
			}
		})
	})

	b.Run("sqlx/prep-stmt", func(b *testing.B) {
		sqlxDB, err := sqlx.Open(driver, connStr)
		if err != nil {
			b.Fatalf("error creating sqlx client: %s", err)
		}
		sqlxDB.SetMaxOpenConns(1)

		err = recreateTable(connStr)
		if err != nil {
			b.Fatalf("error creating table: %s", err.Error())
		}

		insertOne, err := sqlxDB.Prepare(`INSERT INTO users(name, age) VALUES ($1, $2) RETURNING id`)
		if err != nil {
			b.Fatalf("could not prepare sql insert query: %s", err.Error())
		}

		b.Run("insert-one", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				user := User{
					Name: strconv.Itoa(i),
					Age:  i,
				}
				rows, err := insertOne.QueryContext(ctx, user.Name, user.Age)
				if err != nil {
					b.Fatalf("insert error: %s", err.Error())
				}
				if !rows.Next() {
					b.Fatalf("missing id from inserted record")
				}
				err = rows.Scan(&user.ID)
				if err != nil {
					b.Fatalf("error scanning rows")
				}
				err = rows.Close()
				if err != nil {
					b.Fatalf("error closing rows")
				}
			}
		})
	})

	b.Run("pgxpool", func(b *testing.B) {
		pgxConf, err := pgxpool.ParseConfig(connStr)
		if err != nil {
			b.Fatalf("error parsing pgx client configs: %s", err)
		}

		pgxConf.MaxConns = 1
		pgxDB, err := pgxpool.ConnectConfig(ctx, pgxConf)
		if err != nil {
			b.Fatalf("error creating pgx client: %s", err)
		}

		err = recreateTable(connStr)
		if err != nil {
			b.Fatalf("error creating table: %s", err.Error())
		}

		b.Run("insert-one", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				user := User{
					Name: strconv.Itoa(i),
					Age:  i,
				}
				rows, err := pgxDB.Query(ctx,
					`INSERT INTO users(name, age) VALUES ($1, $2) RETURNING id`,
					user.Name, user.Age,
				)
				if err != nil {
					b.Fatalf("insert error: %s", err.Error())
				}
				if !rows.Next() {
					b.Fatalf("missing id from inserted record")
				}
				err = rows.Scan(&user.ID)
				if err != nil {
					b.Fatalf("error scanning rows")
				}
				rows.Close()
			}
		})
	})

	b.Run("gorm", func(b *testing.B) {
		gormDB, err := gorm.Open(postgres.Open(connStr), &gorm.Config{})
		if err != nil {
			b.Fatalf("error creating gorm client: %s", err)
		}

		err = recreateTable(connStr)
		if err != nil {
			b.Fatalf("error creating table: %s", err)
		}

		b.Run("insert-one", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				err := gormDB.Table("users").WithContext(ctx).Create(&User{
					Name: strconv.Itoa(i),
					Age:  i,
				}).Error
				if err != nil {
					b.Fatalf("insert error: %s", err)
				}
			}
		})
	})

	b.Run("sqlc", func(b *testing.B) {
		sqlDB, err := sql.Open(driver, connStr)
		if err != nil {
			b.Fatalf("error creating sql client: %s", err)
		}
		sqlDB.SetMaxOpenConns(1)

		sqlcDB := sqlcgen.New(sqlDB)

		err = recreateTable(connStr)
		if err != nil {
			b.Fatalf("error creating table: %s", err.Error())
		}

		b.Run("insert-one", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				user := sqlcgen.InsertUserParams{
					Name: strconv.Itoa(i),
					Age:  int32(i),
				}
				_, err := sqlcDB.InsertUser(ctx, user)
				if err != nil {
					b.Fatalf("insert error: %s", err.Error())
				}
			}
		})
	})

	b.Run("sqlc/prep-stmt", func(b *testing.B) {
		sqlDB, err := sql.Open(driver, connStr)
		if err != nil {
			b.Fatalf("error creating sql client: %s", err)
		}
		sqlDB.SetMaxOpenConns(1)

		sqlcDB, err := sqlcgen.Prepare(ctx, sqlDB)
		if err != nil {
			b.Fatalf("error preparing sqlc statements: %s", err)
		}

		err = recreateTable(connStr)
		if err != nil {
			b.Fatalf("error creating table: %s", err.Error())
		}

		b.Run("insert-one", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				user := sqlcgen.InsertUserParams{
					Name: strconv.Itoa(i),
					Age:  int32(i),
				}
				_, err := sqlcDB.InsertUser(ctx, user)
				if err != nil {
					b.Fatalf("insert error: %s", err.Error())
				}
			}
		})
	})

	b.Run("sqlboiler", func(b *testing.B) {
		sqlDB, err := sql.Open(driver, connStr)
		if err != nil {
			b.Fatalf("error creating sql client: %s", err)
		}
		sqlDB.SetMaxOpenConns(1)

		err = recreateTable(connStr)
		if err != nil {
			b.Fatalf("error creating table: %s", err.Error())
		}

		b.Run("insert-one", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				user := sqlboilergen.User{
					Name: strconv.Itoa(i),
					Age:  i,
				}
				err := user.Insert(ctx, sqlDB, boil.Infer())
				if err != nil {
					b.Fatalf("insert error: %s", err.Error())
				}
			}
		})
	})
}

func BenchmarkQuery(b *testing.B) {
	ctx := context.Background()

	driver := "postgres"
	connStr := "host=localhost port=5432 user=postgres password=postgres dbname=ksql sslmode=disable"

	type User struct {
		ID   int    `ksql:"id" db:"id"`
		Name string `ksql:"name" db:"name"`
		Age  int    `ksql:"age" db:"age"`
	}

	b.Run("ksql/sql-adapter", func(b *testing.B) {
		db, err := sql.Open(driver, connStr)
		if err != nil {
			b.Fatalf("error connecting to database: %s", err)
		}
		db.SetMaxOpenConns(1)
		ksqlDB, err := ksql.NewWithAdapter(NewSQLAdapter(db), driver)
		if err != nil {
			b.Fatalf("error creating ksql client: %s", err)
		}

		err = recreateTable(connStr)
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
				err := ksqlDB.QueryOne(ctx, &user, `FROM users OFFSET $1 LIMIT 1`, i%100)
				if err != nil {
					b.Fatalf("query error: %s", err.Error())
				}
			}
		})

		b.Run("multiple-rows", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				var users []User
				err := ksqlDB.Query(ctx, &users, `FROM users OFFSET $1 LIMIT 10`, i%90)
				if err != nil {
					b.Fatalf("query error: %s", err.Error())
				}
				if len(users) < 10 {
					b.Fatalf("expected 10 scanned users, but got: %d", len(users))
				}
			}
		})
	})

	b.Run("ksql/pgx-adapter", func(b *testing.B) {
		kpgxDB, err := kpgx.New(ctx, connStr, ksql.Config{
			MaxOpenConns: 1,
		})
		if err != nil {
			b.Fatalf("error creating kpgx client: %s", err)
		}

		err = recreateTable(connStr)
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
				err := kpgxDB.QueryOne(ctx, &user, `FROM users OFFSET $1 LIMIT 1`, i%100)
				if err != nil {
					b.Fatalf("query error: %s", err.Error())
				}
			}
		})

		b.Run("multiple-rows", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				var users []User
				err := kpgxDB.Query(ctx, &users, `FROM users OFFSET $1 LIMIT 10`, i%90)
				if err != nil {
					b.Fatalf("query error: %s", err.Error())
				}
				if len(users) < 10 {
					b.Fatalf("expected 10 scanned users, but got: %d", len(users))
				}
			}
		})
	})

	b.Run("sql", func(b *testing.B) {
		sqlDB, err := sql.Open(driver, connStr)
		if err != nil {
			b.Fatalf("error creating sql client: %s", err)
		}
		sqlDB.SetMaxOpenConns(1)

		err = recreateTable(connStr)
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
				rows, err := sqlDB.QueryContext(ctx, `SELECT id, name, age FROM users OFFSET $1 LIMIT 1`, i%100)
				if err != nil {
					b.Fatalf("query error: %s", err.Error())
				}
				if !rows.Next() {
					b.Fatalf("missing user from inserted record, offset: %d", i%100)
				}
				err = rows.Scan(&user.ID, &user.Name, &user.Age)
				if err != nil {
					b.Fatalf("error scanning rows")
				}
				err = rows.Close()
				if err != nil {
					b.Fatalf("error closing rows")
				}
			}
		})

		b.Run("multiple-rows", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				var users []User
				rows, err := sqlDB.QueryContext(ctx, `SELECT id, name, age FROM users OFFSET $1 LIMIT 10`, i%90)
				if err != nil {
					b.Fatalf("query error: %s", err.Error())
				}
				for j := 0; j < 10; j++ {
					if !rows.Next() {
						b.Fatalf("missing user from inserted record, offset: %d", i%90)
					}
					var user User
					err = rows.Scan(&user.ID, &user.Name, &user.Age)
					if err != nil {
						b.Fatalf("error scanning rows")
					}
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

	b.Run("sql/prep-stmt", func(b *testing.B) {
		sqlDB, err := sql.Open(driver, connStr)
		if err != nil {
			b.Fatalf("error creating sql client: %s", err)
		}
		sqlDB.SetMaxOpenConns(1)

		err = recreateTable(connStr)
		if err != nil {
			b.Fatalf("error creating table: %s", err.Error())
		}

		err = insertUsers(connStr, 100)
		if err != nil {
			b.Fatalf("error inserting users: %s", err.Error())
		}

		singleRow, err := sqlDB.Prepare(`SELECT id, name, age FROM users OFFSET $1 LIMIT 1`)
		if err != nil {
			b.Fatalf("error preparing sql statement for single row: %s", err.Error())
		}

		multipleRows, err := sqlDB.Prepare(`SELECT id, name, age FROM users OFFSET $1 LIMIT 10`)
		if err != nil {
			b.Fatalf("error preparing sql statement for multiple rows: %s", err.Error())
		}

		b.Run("single-row", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				var user User
				rows, err := singleRow.QueryContext(ctx, i%100)
				if err != nil {
					b.Fatalf("query error: %s", err.Error())
				}
				if !rows.Next() {
					b.Fatalf("missing user from inserted record, offset: %d", i%100)
				}
				err = rows.Scan(&user.ID, &user.Name, &user.Age)
				if err != nil {
					b.Fatalf("error scanning rows")
				}
				err = rows.Close()
				if err != nil {
					b.Fatalf("error closing rows")
				}
			}
		})

		b.Run("multiple-rows", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				var users []User
				rows, err := multipleRows.QueryContext(ctx, i%90)
				if err != nil {
					b.Fatalf("query error: %s", err.Error())
				}
				for j := 0; j < 10; j++ {
					if !rows.Next() {
						b.Fatalf("missing user from inserted record, offset: %d", i%90)
					}
					var user User
					err = rows.Scan(&user.ID, &user.Name, &user.Age)
					if err != nil {
						b.Fatalf("error scanning rows")
					}
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

	b.Run("sqlx", func(b *testing.B) {
		sqlxDB, err := sqlx.Open(driver, connStr)
		if err != nil {
			b.Fatalf("error creating sqlx client: %s", err)
		}
		sqlxDB.SetMaxOpenConns(1)

		err = recreateTable(connStr)
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
				rows, err := sqlxDB.QueryxContext(ctx, `SELECT id, name, age FROM users OFFSET $1 LIMIT 1`, i%100)
				if err != nil {
					b.Fatalf("query error: %s", err.Error())
				}
				if !rows.Next() {
					b.Fatalf("missing user from inserted record, offset: %d", i%100)
				}
				err = rows.StructScan(&user)
				if err != nil {
					b.Fatalf("error scanning rows")
				}
				err = rows.Close()
				if err != nil {
					b.Fatalf("error closing rows")
				}
			}
		})

		b.Run("multiple-rows", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				var users []User
				rows, err := sqlxDB.QueryxContext(ctx, `SELECT id, name, age FROM users OFFSET $1 LIMIT 10`, i%90)
				if err != nil {
					b.Fatalf("query error: %s", err.Error())
				}
				for j := 0; j < 10; j++ {
					if !rows.Next() {
						b.Fatalf("missing user from inserted record, offset: %d", i%90)
					}
					var user User
					rows.StructScan(&user)
					if err != nil {
						b.Fatalf("error scanning rows")
					}
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

	b.Run("sqlx/prep-stmt", func(b *testing.B) {
		sqlxDB, err := sqlx.Open(driver, connStr)
		if err != nil {
			b.Fatalf("error creating sqlx client: %s", err)
		}
		sqlxDB.SetMaxOpenConns(1)

		err = recreateTable(connStr)
		if err != nil {
			b.Fatalf("error creating table: %s", err.Error())
		}

		err = insertUsers(connStr, 100)
		if err != nil {
			b.Fatalf("error inserting users: %s", err.Error())
		}

		singleRow, err := sqlxDB.Preparex(`SELECT id, name, age FROM users OFFSET $1 LIMIT 1`)
		if err != nil {
			b.Fatalf("error preparing sql statement for single row: %s", err.Error())
		}

		multipleRows, err := sqlxDB.Preparex(`SELECT id, name, age FROM users OFFSET $1 LIMIT 10`)
		if err != nil {
			b.Fatalf("error preparing sql statement for multiple rows: %s", err.Error())
		}

		b.Run("single-row", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				var user User
				rows, err := singleRow.QueryxContext(ctx, i%100)
				if err != nil {
					b.Fatalf("query error: %s", err.Error())
				}
				if !rows.Next() {
					b.Fatalf("missing user from inserted record, offset: %d", i%100)
				}
				err = rows.StructScan(&user)
				if err != nil {
					b.Fatalf("error scanning rows")
				}
				err = rows.Close()
				if err != nil {
					b.Fatalf("error closing rows")
				}
			}
		})

		b.Run("multiple-rows", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				var users []User
				rows, err := multipleRows.QueryxContext(ctx, i%90)
				if err != nil {
					b.Fatalf("query error: %s", err.Error())
				}
				for j := 0; j < 10; j++ {
					if !rows.Next() {
						b.Fatalf("missing user from inserted record, offset: %d", i%90)
					}
					var user User
					rows.StructScan(&user)
					if err != nil {
						b.Fatalf("error scanning rows")
					}
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

	b.Run("pgxpool", func(b *testing.B) {
		pgxConf, err := pgxpool.ParseConfig(connStr)
		if err != nil {
			b.Fatalf("error parsing pgx client configs: %s", err)
		}

		pgxConf.MaxConns = 1
		pgxDB, err := pgxpool.ConnectConfig(ctx, pgxConf)
		if err != nil {
			b.Fatalf("error creating pgx client: %s", err)
		}

		err = recreateTable(connStr)
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
				rows, err := pgxDB.Query(ctx, `SELECT id, name, age FROM users OFFSET $1 LIMIT 1`, i%100)
				if err != nil {
					b.Fatalf("query error: %s", err.Error())
				}
				if !rows.Next() {
					b.Fatalf("missing user from inserted record, offset: %d", i%100)
				}
				err = rows.Scan(&user.ID, &user.Name, &user.Age)
				if err != nil {
					b.Fatalf("error scanning rows")
				}
				rows.Close()
			}
		})

		b.Run("multiple-rows", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				var users []User
				rows, err := pgxDB.Query(ctx, `SELECT id, name, age FROM users OFFSET $1 LIMIT 10`, i%90)
				if err != nil {
					b.Fatalf("query error: %s", err.Error())
				}
				for j := 0; j < 10; j++ {
					if !rows.Next() {
						b.Fatalf("missing user from inserted record, offset: %d", i%90)
					}
					var user User
					err = rows.Scan(&user.ID, &user.Name, &user.Age)
					if err != nil {
						b.Fatalf("error scanning rows")
					}
					users = append(users, user)
				}
				if len(users) < 10 {
					b.Fatalf("expected 10 scanned users, but got: %d", len(users))
				}

				rows.Close()
			}
		})
	})

	b.Run("gorm", func(b *testing.B) {
		gormDB, err := gorm.Open(postgres.Open(connStr), &gorm.Config{})
		if err != nil {
			b.Fatalf("error creating gorm client: %s", err)
		}

		err = recreateTable(connStr)
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
				err := gormDB.Table("users").WithContext(ctx).Offset(i % 100).Take(&user).Error
				if err != nil {
					b.Fatalf("query error: %s", err)
				}
			}
		})

		b.Run("multiple-rows", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				var users []User
				err := gormDB.Table("users").WithContext(ctx).Offset(i % 90).Limit(10).Find(&users).Error
				if err != nil {
					b.Fatalf("query error: %s", err.Error())
				}
				if len(users) < 10 {
					b.Fatalf("expected 10 scanned users, but got: %d", len(users))
				}
			}
		})
	})

	b.Run("sqlc", func(b *testing.B) {
		sqlDB, err := sql.Open(driver, connStr)
		if err != nil {
			b.Fatalf("error creating sql client: %s", err)
		}
		sqlDB.SetMaxOpenConns(1)

		sqlcDB := sqlcgen.New(sqlDB)

		err = recreateTable(connStr)
		if err != nil {
			b.Fatalf("error creating table: %s", err.Error())
		}

		err = insertUsers(connStr, 100)
		if err != nil {
			b.Fatalf("error inserting users: %s", err.Error())
		}

		b.Run("single-row", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := sqlcDB.GetUser(ctx, int32(i%100))
				if err != nil {
					b.Fatalf("query error: %s", err.Error())
				}
			}
		})

		b.Run("multiple-rows", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := sqlcDB.List10Users(ctx, int32(i%90))
				if err != nil {
					b.Fatalf("query error: %s", err.Error())
				}
			}
		})
	})

	b.Run("sqlc/prep-stmt", func(b *testing.B) {
		sqlDB, err := sql.Open(driver, connStr)
		if err != nil {
			b.Fatalf("error creating sql client: %s", err)
		}
		sqlDB.SetMaxOpenConns(1)

		sqlcDB, err := sqlcgen.Prepare(ctx, sqlDB)
		if err != nil {
			b.Fatalf("error preparing sqlc statements: %s", err)
		}

		err = recreateTable(connStr)
		if err != nil {
			b.Fatalf("error creating table: %s", err.Error())
		}

		err = insertUsers(connStr, 100)
		if err != nil {
			b.Fatalf("error inserting users: %s", err.Error())
		}

		b.Run("single-row", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := sqlcDB.GetUser(ctx, int32(i%100))
				if err != nil {
					b.Fatalf("query error: %s", err.Error())
				}
			}
		})

		b.Run("multiple-rows", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := sqlcDB.List10Users(ctx, int32(i%90))
				if err != nil {
					b.Fatalf("query error: %s", err.Error())
				}
			}
		})
	})

	b.Run("sqlboiler", func(b *testing.B) {
		sqlDB, err := sql.Open(driver, connStr)
		if err != nil {
			b.Fatalf("error creating sql client: %s", err)
		}
		sqlDB.SetMaxOpenConns(1)

		err = recreateTable(connStr)
		if err != nil {
			b.Fatalf("error creating table: %s", err.Error())
		}

		err = insertUsers(connStr, 100)
		if err != nil {
			b.Fatalf("error inserting users: %s", err.Error())
		}

		b.Run("single-row", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := sqlboilergen.Users(qm.Select("id", "name", "age"), qm.Offset(i%100), qm.Limit(1)).One(ctx, sqlDB)
				if err != nil {
					b.Fatalf("query error: %s", err.Error())
				}
			}
		})

		b.Run("multiple-rows", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := sqlboilergen.Users(qm.Select("id", "name", "age"), qm.Offset(i%90), qm.Limit(10)).One(ctx, sqlDB)
				if err != nil {
					b.Fatalf("query error: %s", err.Error())
				}
			}
		})
	})
}

//go:embed schema.sql
var createTablesSQL string

func recreateTable(connStr string) error {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return err
	}
	defer db.Close()

	db.Exec(`DROP TABLE users`)

	_, err = db.Exec(createTablesSQL)
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
