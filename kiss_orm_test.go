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

func TestFind(t *testing.T) {
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
		u := User{}
		err := c.Find(ctx, &u, `SELECT * FROM users WHERE id=1;`)
		assert.NotEqual(t, nil, err)
		assert.Equal(t, User{}, u)
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
		err = c.Find(ctx, &u, `SELECT * FROM users WHERE name=?;`, "Bia")

		assert.Equal(t, err, nil)
		assert.Equal(t, "Bia", u.Name)
		assert.NotEqual(t, 0, u.ID)
	})
}

func TestGetByID(t *testing.T) {
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
		u := User{}
		err := c.GetByID(ctx, &u, 999)
		assert.NotEqual(t, nil, err)
		assert.Equal(t, User{}, u)
	})

	t.Run("should return a user correctly", func(t *testing.T) {
		db := connectDB(t)
		defer db.Close()

		bia := &User{
			Name: "Bia",
		}
		db.Create(&bia)

		ctx := context.Background()
		c := Client{
			db:        db,
			tableName: "users",
		}
		result := User{}
		err = c.GetByID(ctx, &result, bia.ID)

		assert.Equal(t, err, nil)
		assert.Equal(t, "Bia", result.Name)
		assert.Equal(t, bia.ID, result.ID)
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
		assert.Equal(t, err, nil)
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
		assert.Equal(t, err, nil)

		result := User{}
		it := c.db.Raw("SELECT * FROM users WHERE id=?", u.ID)
		it.Scan(&result)
		assert.Equal(t, it.Error, nil)
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
		assert.Equal(t, err, nil)
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
		assert.Equal(t, err, nil)

		assert.NotEqual(t, 0, u.ID)
		result := User{}
		it := c.db.Raw("SELECT * FROM users WHERE id=?", u.ID)
		it.Scan(&result)
		assert.Equal(t, u.ID, result.ID)

		err = c.Delete(ctx, u.ID)
		assert.Equal(t, err, nil)

		result = User{}
		it = c.db.Raw("SELECT * FROM users WHERE id=?", u.ID)
		it.Scan(&result)

		assert.Equal(t, it.Error, nil)
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
		assert.Equal(t, err, nil)
		assert.NotEqual(t, 0, u.ID)

		// Empty update, should do nothing:
		err = c.Update(ctx)
		assert.Equal(t, err, nil)

		result := User{}
		err = c.GetByID(ctx, &result, u.ID)
		assert.Equal(t, err, nil)

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
		assert.NotEqual(t, err, nil)
	})
}

func TestStructToMap(t *testing.T) {
	type S1 struct {
		Name string `gorm:"name_attr"`
		Age  int    `gorm:"age_attr"`
	}
	t.Run("should convert plain structs to maps", func(t *testing.T) {
		m, err := structToMap(S1{
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
		m, err := structToMap(S1{
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
		m, err := structToMap(S2{
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
		m, err := structToMap(S2{
			Name: nil,
			Age:  nil,
		})

		assert.Equal(t, nil, err)
		assert.Equal(t, map[string]interface{}{}, m)
	})
}

func TestQuery(t *testing.T) {
	t.Run("should execute query one correctly", func(t *testing.T) {
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

		it, err := c.Query(ctx, `select * from users where name = ?;`, "User1")
		assert.Equal(t, nil, err)

		u := User{}
		_, err = c.QueryNext(ctx, it, &u)
		it.Close()

		assert.Equal(t, nil, err)
		assert.NotEqual(t, 0, u.ID)
		assert.Equal(t, "User1", u.Name)
	})

	t.Run("should execute query many correctly", func(t *testing.T) {
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

		it, err := c.Query(ctx, `select * from users where name in (?,?);`, "User1", "User3")
		assert.Equal(t, nil, err)

		// var results []User
		u := User{}
		u2 := User{}
		u3 := User{}
		done, err := c.QueryNext(ctx, it, &u)
		assert.Equal(t, false, done)
		assert.Equal(t, nil, err)

		done, err = c.QueryNext(ctx, it, &u2)
		assert.Equal(t, false, done)
		assert.Equal(t, nil, err)

		done, err = c.QueryNext(ctx, it, &u3)
		assert.Equal(t, true, done)
		assert.Equal(t, nil, err)

		assert.NotEqual(t, 0, u.ID)
		assert.Equal(t, "User1", u.Name)
		assert.NotEqual(t, 0, u2.ID)
		assert.Equal(t, "User3", u2.Name)
	})

	t.Run("should return error for an invalid iterator", func(t *testing.T) {
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

		u := User{}
		_, err = c.QueryNext(ctx, Iterator(nil), &u)

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
