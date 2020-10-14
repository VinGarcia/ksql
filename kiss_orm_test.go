package kissorm

import (
	"context"
	"testing"
	"time"

	"github.com/ditointernet/go-assert"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
)

type User struct {
	ID        uint      `gorm:"id"`
	Name      string    `gorm:"name"`
	CreatedAt time.Time `gorm:"created_at"`
}

func TestQuery(t *testing.T) {
	err := createTable()
	if err != nil {
		t.Fatal("could not create test table!")
	}

	t.Run("should return 0 results correctly", func(t *testing.T) {
		db := connectDB(t)
		defer db.Close()

		ctx := context.Background()
		c := Client{
			db:        db,
			tableName: "users",
		}
		var users []User
		err := c.Query(ctx, &users, `SELECT * FROM users WHERE id=1;`)
		assert.Equal(t, nil, err)
		assert.Equal(t, []User{}, users)
	})

	t.Run("should return a user correctly", func(t *testing.T) {
		db := connectDB(t)
		defer db.Close()

		db.Create(&User{
			Name: "Bia",
		})

		ctx := context.Background()
		c := Client{
			db:        db,
			tableName: "users",
		}
		var users []User
		err = c.Query(ctx, &users, `SELECT * FROM users WHERE name=?;`, "Bia")

		assert.Equal(t, nil, err)
		assert.Equal(t, 1, len(users))
		assert.Equal(t, "Bia", users[0].Name)
		assert.NotEqual(t, 0, users[0].ID)
	})

	t.Run("should return multiple users correctly", func(t *testing.T) {
		db := connectDB(t)
		defer db.Close()

		db.Create(&User{
			Name: "João Garcia",
		})

		db.Create(&User{
			Name: "Bia Garcia",
		})

		ctx := context.Background()
		c := Client{
			db:        db,
			tableName: "users",
		}
		var users []User
		err = c.Query(ctx, &users, `SELECT * FROM users WHERE name like ?;`, "% Garcia")

		assert.Equal(t, nil, err)
		assert.Equal(t, 2, len(users))
		assert.Equal(t, "João Garcia", users[0].Name)
		assert.NotEqual(t, 0, users[0].ID)
		assert.Equal(t, "Bia Garcia", users[1].Name)
		assert.NotEqual(t, 0, users[1].ID)
	})
}

func TestQueryOne(t *testing.T) {
	err := createTable()
	if err != nil {
		t.Fatal("could not create test table!")
	}

	t.Run("should return EntityNotFoundErr when there are no results", func(t *testing.T) {
		db := connectDB(t)
		defer db.Close()

		ctx := context.Background()
		c := Client{
			db:        db,
			tableName: "users",
		}
		u := User{}
		err := c.QueryOne(ctx, &u, `SELECT * FROM users WHERE id=1;`)
		assert.Equal(t, EntityNotFoundErr, err)
	})

	t.Run("should return a user correctly", func(t *testing.T) {
		db := connectDB(t)
		defer db.Close()

		db.Create(&User{
			Name: "Bia",
		})

		ctx := context.Background()
		c := Client{
			db:        db,
			tableName: "users",
		}
		u := User{}
		err = c.QueryOne(ctx, &u, `SELECT * FROM users WHERE name=?;`, "Bia")

		assert.Equal(t, nil, err)
		assert.Equal(t, "Bia", u.Name)
		assert.NotEqual(t, 0, u.ID)
	})

	t.Run("should report error if input is no a pointer to struct", func(t *testing.T) {
		db := connectDB(t)
		defer db.Close()

		db.Create(&User{
			Name: "Andréa Sá",
		})

		db.Create(&User{
			Name: "Caio Sá",
		})

		ctx := context.Background()
		c := Client{
			db:        db,
			tableName: "users",
		}
		users := []User{}
		err = c.QueryOne(ctx, &users, `SELECT * FROM users WHERE name like ?;`, "% Sá")

		assert.NotEqual(t, nil, err)
	})
}

func TestInsert(t *testing.T) {
	err := createTable()
	if err != nil {
		t.Fatal("could not create test table!")
	}

	t.Run("should ignore empty lists of users", func(t *testing.T) {
		db := connectDB(t)
		defer db.Close()

		ctx := context.Background()
		c := Client{
			db:        db,
			tableName: "users",
		}

		err = c.Insert(ctx)
		assert.Equal(t, nil, err)
	})

	t.Run("should insert one user correctly", func(t *testing.T) {
		db := connectDB(t)
		defer db.Close()

		ctx := context.Background()
		c := Client{
			db:        db,
			tableName: "users",
		}

		u := User{
			Name: "Fernanda",
		}

		err := c.Insert(ctx, &u)
		assert.Equal(t, nil, err)

		result := User{}
		it := c.db.Raw("SELECT * FROM users WHERE id=?", u.ID)
		it.Scan(&result)
		assert.Equal(t, nil, it.Error)
		assert.Equal(t, u.Name, result.Name)
		assert.Equal(t, u.CreatedAt.Format(time.RFC3339), result.CreatedAt.Format(time.RFC3339))
	})
}

func TestDelete(t *testing.T) {
	err := createTable()
	if err != nil {
		t.Fatal("could not create test table!")
	}

	t.Run("should ignore empty lists of ids", func(t *testing.T) {
		db := connectDB(t)
		defer db.Close()

		ctx := context.Background()
		c := Client{
			db:        db,
			tableName: "users",
		}

		err = c.Delete(ctx)
		assert.Equal(t, nil, err)
	})

	t.Run("should delete one id correctly", func(t *testing.T) {
		db := connectDB(t)
		defer db.Close()

		ctx := context.Background()
		c := Client{
			db:        db,
			tableName: "users",
		}

		u := User{
			Name: "Fernanda",
		}

		err := c.Insert(ctx, &u)
		assert.Equal(t, nil, err)

		assert.NotEqual(t, 0, u.ID)
		result := User{}
		it := c.db.Raw("SELECT * FROM users WHERE id=?", u.ID)
		it.Scan(&result)
		assert.Equal(t, u.ID, result.ID)

		err = c.Delete(ctx, u.ID)
		assert.Equal(t, nil, err)

		result = User{}
		it = c.db.Raw("SELECT * FROM users WHERE id=?", u.ID)
		it.Scan(&result)

		assert.Equal(t, nil, it.Error)
		assert.Equal(t, uint(0), result.ID)
		assert.Equal(t, "", result.Name)
	})
}

func TestUpdate(t *testing.T) {
	err := createTable()
	if err != nil {
		t.Fatal("could not create test table!")
	}

	t.Run("should ignore empty lists of ids", func(t *testing.T) {
		db := connectDB(t)
		defer db.Close()

		ctx := context.Background()
		c := Client{
			db:        db,
			tableName: "users",
		}

		u := User{
			Name: "Thay",
		}
		err := c.Insert(ctx, &u)
		assert.Equal(t, nil, err)
		assert.NotEqual(t, 0, u.ID)

		// Empty update, should do nothing:
		err = c.Update(ctx)
		assert.Equal(t, nil, err)

		result := User{}
		it := c.db.Raw("SELECT * FROM users WHERE id=?", u.ID)
		it.Scan(&result)
		it.Close()
		assert.Equal(t, nil, err)

		assert.Equal(t, "Thay", result.Name)
	})

	t.Run("should update one user correctly", func(t *testing.T) {
		db := connectDB(t)
		defer db.Close()

		ctx := context.Background()
		c := Client{
			db:        db,
			tableName: "non_existing_table",
		}

		err = c.Update(ctx, User{
			ID:   1,
			Name: "Thayane",
		})
		assert.NotEqual(t, nil, err)
	})
}

func TestStructToMap(t *testing.T) {
	type S1 struct {
		Name string `gorm:"name_attr"`
		Age  int    `gorm:"age_attr"`
	}
	t.Run("should convert plain structs to maps", func(t *testing.T) {
		m, err := StructToMap(S1{
			Name: "my name",
			Age:  22,
		})

		assert.Equal(t, nil, err)
		assert.Equal(t, map[string]interface{}{
			"name_attr": "my name",
			"age_attr":  22,
		}, m)
	})

	t.Run("should not ignore zero value attrs, if they are not pointers", func(t *testing.T) {
		m, err := StructToMap(S1{
			Name: "",
			Age:  0,
		})

		assert.Equal(t, nil, err)
		assert.Equal(t, map[string]interface{}{
			"name_attr": "",
			"age_attr":  0,
		}, m)
	})

	type S2 struct {
		Name *string `gorm:"name"`
		Age  *int    `gorm:"age"`
	}

	t.Run("should not ignore not nil pointers", func(t *testing.T) {
		str := ""
		age := 0
		m, err := StructToMap(S2{
			Name: &str,
			Age:  &age,
		})

		assert.Equal(t, nil, err)
		assert.Equal(t, map[string]interface{}{
			"name": "",
			"age":  0,
		}, m)
	})

	t.Run("should ignore nil pointers", func(t *testing.T) {
		m, err := StructToMap(S2{
			Name: nil,
			Age:  nil,
		})

		assert.Equal(t, nil, err)
		assert.Equal(t, map[string]interface{}{}, m)
	})
}

func TestQueryChunks(t *testing.T) {
	t.Run("should query a single row correctly", func(t *testing.T) {
		err := createTable()
		if err != nil {
			t.Fatal("could not create test table!")
		}

		db := connectDB(t)
		defer db.Close()

		ctx := context.Background()
		c := Client{
			db:        db,
			tableName: "users",
		}

		_ = c.Insert(ctx, &User{Name: "User1"})

		var length int
		var u User
		var users []User
		err = c.QueryChunks(ctx, ChunkParser{
			Query:  `select * from users where name = ?;`,
			Params: []interface{}{"User1"},

			ChunkSize: 100,
			Chunk:     &users,
			ForEachChunk: func() error {
				length = len(users)
				if length > 0 {
					u = users[0]
				}
				return nil
			},
		})

		assert.Equal(t, nil, err)
		assert.Equal(t, 1, length)
		assert.NotEqual(t, 0, u.ID)
		assert.Equal(t, "User1", u.Name)
	})
}

func TestFillSliceWith(t *testing.T) {
	t.Run("should fill a list correctly", func(t *testing.T) {
		var users []User
		err := FillSliceWith(&users, []map[string]interface{}{
			{
				"name": "Jorge",
			},
			{
				"name": "Luciana",
			},
			{
				"name": "Breno",
			},
		})

		assert.Equal(t, nil, err)
		assert.Equal(t, 3, len(users))
		assert.Equal(t, "Jorge", users[0].Name)
		assert.Equal(t, "Luciana", users[1].Name)
		assert.Equal(t, "Breno", users[2].Name)
	})
}

func createTable() error {
	db, err := gorm.Open("sqlite3", "/tmp/test.db")
	if err != nil {
		return err
	}
	defer db.Close()

	db.DropTableIfExists(&User{})
	db.CreateTable(&User{})

	return nil
}

func connectDB(t *testing.T) *gorm.DB {
	db, err := gorm.Open("sqlite3", "/tmp/test.db")
	if err != nil {
		t.Fatal(err.Error())
	}
	return db
}
