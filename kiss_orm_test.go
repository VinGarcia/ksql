package kissorm

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ditointernet/go-assert"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	"github.com/vingarcia/kissorm/nullable"
)

type User struct {
	ID        uint      `gorm:"id"`
	Name      string    `gorm:"name"`
	Age       int       `gorm:"age"`
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
		assert.Equal(t, []User(nil), users)

		users = []User{}
		err = c.Query(ctx, &users, `SELECT * FROM users WHERE id=1;`)
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
		assert.NotEqual(t, uint(0), users[0].ID)
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
		assert.NotEqual(t, uint(0), users[0].ID)
		assert.Equal(t, "Bia Garcia", users[1].Name)
		assert.NotEqual(t, uint(0), users[1].ID)
	})

	t.Run("should report error if input is not a pointer to a slice of structs", func(t *testing.T) {
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
		err = c.Query(ctx, &User{}, `SELECT * FROM users WHERE name like ?;`, "% Sá")
		assert.NotEqual(t, nil, err)

		err = c.Query(ctx, []User{}, `SELECT * FROM users WHERE name like ?;`, "% Sá")
		assert.NotEqual(t, nil, err)

		var i int
		err = c.Query(ctx, &i, `SELECT * FROM users WHERE name like ?;`, "% Sá")
		assert.NotEqual(t, nil, err)

		err = c.Query(ctx, &[]int{}, `SELECT * FROM users WHERE name like ?;`, "% Sá")
		assert.NotEqual(t, nil, err)
	})
}

func TestQueryOne(t *testing.T) {
	err := createTable()
	if err != nil {
		t.Fatal("could not create test table!")
	}

	t.Run("should return RecordNotFoundErr when there are no results", func(t *testing.T) {
		db := connectDB(t)
		defer db.Close()

		ctx := context.Background()
		c := Client{
			db:        db,
			tableName: "users",
		}
		u := User{}
		err := c.QueryOne(ctx, &u, `SELECT * FROM users WHERE id=1;`)
		assert.Equal(t, ErrRecordNotFound, err)
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
		assert.NotEqual(t, uint(0), u.ID)
	})

	t.Run("should report error if input is not a pointer to struct", func(t *testing.T) {
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

		err = c.QueryOne(ctx, &[]User{}, `SELECT * FROM users WHERE name like ?;`, "% Sá")
		assert.NotEqual(t, nil, err)

		err = c.QueryOne(ctx, User{}, `SELECT * FROM users WHERE name like ?;`, "% Sá")
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

		u := User{
			Name: "Won't be deleted",
		}

		err := c.Insert(ctx, &u)
		assert.Equal(t, nil, err)
		assert.NotEqual(t, uint(0), u.ID)

		result := User{}
		it := c.db.Raw("SELECT * FROM users WHERE id=?", u.ID)
		it.Scan(&result)
		assert.Equal(t, u.ID, result.ID)

		err = c.Delete(ctx)
		assert.Equal(t, nil, err)

		result = User{}
		it = c.db.Raw("SELECT * FROM users WHERE id=?", u.ID)
		it.Scan(&result)
		assert.Equal(t, u.ID, result.ID)
	})

	t.Run("should delete one id correctly", func(t *testing.T) {
		db := connectDB(t)
		defer db.Close()

		ctx := context.Background()
		c := Client{
			db:        db,
			tableName: "users",
		}

		u1 := User{
			Name: "Fernanda",
		}

		err := c.Insert(ctx, &u1)
		assert.Equal(t, nil, err)
		assert.NotEqual(t, uint(0), u1.ID)

		result := User{}
		it := c.db.Raw("SELECT * FROM users WHERE id=?", u1.ID)
		it.Scan(&result)
		assert.Equal(t, u1.ID, result.ID)

		u2 := User{
			Name: "Won't be deleted",
		}

		err = c.Insert(ctx, &u2)
		assert.Equal(t, nil, err)
		assert.NotEqual(t, uint(0), u2.ID)

		result = User{}
		it = c.db.Raw("SELECT * FROM users WHERE id=?", u2.ID)
		it.Scan(&result)
		assert.Equal(t, u2.ID, result.ID)

		err = c.Delete(ctx, u1.ID)
		assert.Equal(t, nil, err)

		result = User{}
		it = c.db.Raw("SELECT * FROM users WHERE id=?", u1.ID)
		it.Scan(&result)

		assert.Equal(t, nil, it.Error)
		assert.Equal(t, uint(0), result.ID)
		assert.Equal(t, "", result.Name)

		result = User{}
		it = c.db.Raw("SELECT * FROM users WHERE id=?", u2.ID)
		it.Scan(&result)

		assert.Equal(t, nil, it.Error)
		assert.NotEqual(t, uint(0), result.ID)
		assert.Equal(t, "Won't be deleted", result.Name)
	})

	t.Run("should delete multiple ids correctly", func(t *testing.T) {
		db := connectDB(t)
		defer db.Close()

		ctx := context.Background()
		c := Client{
			db:        db,
			tableName: "users",
		}

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
		it := c.db.Raw("SELECT * FROM users WHERE id=?", u1.ID)
		it.Scan(&result)
		assert.Equal(t, u1.ID, result.ID)

		result = User{}
		it = c.db.Raw("SELECT * FROM users WHERE id=?", u2.ID)
		it.Scan(&result)
		assert.Equal(t, u2.ID, result.ID)

		result = User{}
		it = c.db.Raw("SELECT * FROM users WHERE id=?", u3.ID)
		it.Scan(&result)
		assert.Equal(t, u3.ID, result.ID)

		err = c.Delete(ctx, u1.ID, u2.ID)
		assert.Equal(t, nil, err)

		results := []User{}
		it = c.db.Raw("SELECT * FROM users WHERE id IN (?, ?, ?)", u1.ID, u2.ID, u3.ID)
		it.Scan(&results)

		assert.Equal(t, nil, it.Error)
		assert.Equal(t, 1, len(results))
		assert.Equal(t, "This won't be deleted", results[0].Name)
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
		assert.NotEqual(t, uint(0), u.ID)

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
			tableName: "users",
		}

		u := User{
			Name: "Letícia",
		}
		r := c.db.Table(c.tableName).Create(&u)
		assert.Equal(t, nil, r.Error)
		assert.NotEqual(t, uint(0), u.ID)

		err = c.Update(ctx, User{
			ID:   u.ID,
			Name: "Thayane",
		})
		assert.Equal(t, nil, err)

		var result User
		it := c.db.Raw("SELECT * FROM users WHERE id=?", u.ID)
		it.Scan(&result)
		assert.Equal(t, nil, it.Error)
		assert.Equal(t, "Thayane", result.Name)
	})

	t.Run("should update one user correctly", func(t *testing.T) {
		db := connectDB(t)
		defer db.Close()

		ctx := context.Background()
		c := Client{
			db:        db,
			tableName: "users",
		}

		u := User{
			Name: "Letícia",
		}
		r := c.db.Table(c.tableName).Create(&u)
		assert.Equal(t, nil, r.Error)
		assert.NotEqual(t, uint(0), u.ID)

		err = c.Update(ctx, User{
			ID:   u.ID,
			Name: "Thayane",
		})
		assert.Equal(t, nil, err)

		var result User
		it := c.db.Raw("SELECT * FROM users WHERE id=?", u.ID)
		it.Scan(&result)
		assert.Equal(t, nil, it.Error)
		assert.Equal(t, "Thayane", result.Name)
	})

	t.Run("should ignore null pointers on partial updates", func(t *testing.T) {
		db := connectDB(t)
		defer db.Close()

		ctx := context.Background()
		c := Client{
			db:        db,
			tableName: "users",
		}

		type partialUser struct {
			ID   uint   `gorm:"id"`
			Name string `gorm:"name"`
			Age  *int   `gorm:"age"`
		}
		u := partialUser{
			Name: "Letícia",
			Age:  nullable.Int(22),
		}
		r := c.db.Table(c.tableName).Create(&u)
		assert.Equal(t, nil, r.Error)
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
		it := c.db.Raw("SELECT * FROM users WHERE id=?", u.ID)
		it.Scan(&result)
		assert.Equal(t, nil, it.Error)
		assert.Equal(t, "", result.Name)
		assert.Equal(t, 22, result.Age)
	})

	t.Run("should update valid pointers on partial updates", func(t *testing.T) {
		db := connectDB(t)
		defer db.Close()

		ctx := context.Background()
		c := Client{
			db:        db,
			tableName: "users",
		}

		type partialUser struct {
			ID   uint   `gorm:"id"`
			Name string `gorm:"name"`
			Age  *int   `gorm:"age"`
		}
		u := partialUser{
			Name: "Letícia",
			Age:  nullable.Int(22),
		}
		r := c.db.Table(c.tableName).Create(&u)
		assert.Equal(t, nil, r.Error)
		assert.NotEqual(t, uint(0), u.ID)

		// Should update all fields:
		err = c.Update(ctx, partialUser{
			ID:   u.ID,
			Name: "Thay",
			Age:  nullable.Int(42),
		})
		assert.Equal(t, nil, err)

		var result User
		it := c.db.Raw("SELECT * FROM users WHERE id=?", u.ID)
		it.Scan(&result)
		assert.Equal(t, nil, it.Error)
		assert.Equal(t, "Thay", result.Name)
		assert.Equal(t, 42, result.Age)
	})

	t.Run("should report database errors correctly", func(t *testing.T) {
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
		err = c.QueryChunks(ctx, ChunkParser{
			Query:  `select * from users where name = ?;`,
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
	})

	t.Run("should query one chunk correctly", func(t *testing.T) {
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
		_ = c.Insert(ctx, &User{Name: "User2"})

		var lengths []int
		var users []User
		err = c.QueryChunks(ctx, ChunkParser{
			Query:  `select * from users where name like ? order by name asc;`,
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
		assert.NotEqual(t, uint(0), users[1].ID)
		assert.Equal(t, "User2", users[1].Name)
	})

	t.Run("should query chunks of 1 correctly", func(t *testing.T) {
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
		_ = c.Insert(ctx, &User{Name: "User2"})

		var lengths []int
		var users []User
		err = c.QueryChunks(ctx, ChunkParser{
			Query:  `select * from users where name like ? order by name asc;`,
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
		assert.NotEqual(t, uint(0), users[0].ID)
		assert.Equal(t, "User1", users[0].Name)
		assert.NotEqual(t, uint(0), users[1].ID)
		assert.Equal(t, "User2", users[1].Name)
		assert.Equal(t, []int{1, 1}, lengths)
	})

	t.Run("should load partially filled chunks correctly", func(t *testing.T) {
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
		_ = c.Insert(ctx, &User{Name: "User2"})
		_ = c.Insert(ctx, &User{Name: "User3"})

		var lengths []int
		var users []User
		err = c.QueryChunks(ctx, ChunkParser{
			Query:  `select * from users where name like ? order by name asc;`,
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

	t.Run("should abort the first iteration when the callback returns an ErrAbortIteration", func(t *testing.T) {
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
		_ = c.Insert(ctx, &User{Name: "User2"})
		_ = c.Insert(ctx, &User{Name: "User3"})

		var lengths []int
		var users []User
		err = c.QueryChunks(ctx, ChunkParser{
			Query:  `select * from users where name like ? order by name asc;`,
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
		_ = c.Insert(ctx, &User{Name: "User2"})
		_ = c.Insert(ctx, &User{Name: "User3"})

		returnVals := []error{nil, ErrAbortIteration}
		var lengths []int
		var users []User
		err = c.QueryChunks(ctx, ChunkParser{
			Query:  `select * from users where name like ? order by name asc;`,
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

func TestScanRows(t *testing.T) {
	t.Run("should scan users correctly", func(t *testing.T) {
		err := createTable()
		if err != nil {
			t.Fatal("could not create test table!")
		}

		ctx := context.TODO()
		db := connectDB(t)
		defer db.Close()
		c := Client{
			db:        db,
			tableName: "users",
		}
		_ = c.Insert(ctx, &User{Name: "User1", Age: 22})
		_ = c.Insert(ctx, &User{Name: "User2", Age: 14})
		_ = c.Insert(ctx, &User{Name: "User3", Age: 43})

		rows, err := db.DB().QueryContext(ctx, "select * from users where name='User2'")
		assert.Equal(t, nil, err)

		assert.Equal(t, true, rows.Next())

		var u User
		err = scanRows(rows, &u)
		assert.Equal(t, nil, err)

		assert.Equal(t, "User2", u.Name)
		assert.Equal(t, 14, u.Age)
	})

	t.Run("should report error for closed rows", func(t *testing.T) {
		err := createTable()
		if err != nil {
			t.Fatal("could not create test table!")
		}

		ctx := context.TODO()
		db := connectDB(t)
		defer db.Close()

		rows, err := db.DB().QueryContext(ctx, "select * from users where name='User2'")
		assert.Equal(t, nil, err)

		var u User
		err = rows.Close()
		assert.Equal(t, nil, err)
		err = scanRows(rows, &u)
		assert.NotEqual(t, nil, err)
	})

	t.Run("should report if record is not a pointer", func(t *testing.T) {
		err := createTable()
		if err != nil {
			t.Fatal("could not create test table!")
		}

		ctx := context.TODO()
		db := connectDB(t)
		defer db.Close()

		rows, err := db.DB().QueryContext(ctx, "select * from users where name='User2'")
		assert.Equal(t, nil, err)

		var u User
		err = scanRows(rows, u)
		assert.NotEqual(t, nil, err)
	})

	t.Run("should report if record is not a pointer to struct", func(t *testing.T) {
		err := createTable()
		if err != nil {
			t.Fatal("could not create test table!")
		}

		ctx := context.TODO()
		db := connectDB(t)
		defer db.Close()

		rows, err := db.DB().QueryContext(ctx, "select * from users where name='User2'")
		assert.Equal(t, nil, err)

		var u map[string]interface{}
		fmt.Println("before scan")
		err = scanRows(rows, &u)
		fmt.Println("after scan")
		assert.NotEqual(t, nil, err)
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

func shiftErrSlice(errs *[]error) error {
	err := (*errs)[0]
	*errs = (*errs)[1:]
	return err
}
