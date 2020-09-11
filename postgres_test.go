package gpostgres

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/ditointernet/go-assert"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
)

type User struct {
	ID        uint
	Name      string
	CreatedAt time.Time
}

func TestGet(t *testing.T) {
	err := createTable()
	if err != nil {
		t.Fatal("could not create test table!")
	}

	t.Run("should return 0 results correctly", func(t *testing.T) {
		db := connectDB(t)
		defer db.Close()

		ctx := context.Background()
		c := Client{db: db}
		u := User{}
		err := c.Get(ctx, &u, `SELECT * FROM users WHERE id=1;`)
		assert.Equal(t, err, nil)
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
			db: db,
		}
		u := User{}
		err = c.Get(ctx, &u, `SELECT * FROM users WHERE name='Bia';`)

		assert.Equal(t, err, nil)
		assert.Equal(t, "Bia", u.Name)
		assert.NotEqual(t, 0, u.ID)
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
		db.LogMode(true)

		ctx := context.Background()
		c := Client{
			db: db,
		}

		user := User{}
		err = c.Insert(ctx, &user)
		assert.Equal(t, err, nil)
	})

	t.Run("should insert one user correctly", func(t *testing.T) {
		db := connectDB(t)
		defer db.Close()

		ctx := context.Background()
		c := Client{
			db: db,
		}

		u := User{
			Name: "Fernanda",
		}

		err := c.Insert(ctx, &u)
		assert.Equal(t, err, nil)

		result := User{}
		fmt.Println("ID:  ", u.ID)
		it := c.db.Raw("SELECT * FROM users WHERE id=?", u.ID)
		it.Scan(&result)
		assert.Equal(t, it.Error, nil)
		assert.Equal(t, u.Name, result.Name)
		assert.Equal(t, u.CreatedAt.Format(time.RFC3339), result.CreatedAt.Format(time.RFC3339))
	})
}

func TestGetMetadata(t *testing.T) {
	t.Run("it should get the struct name correctly", func(t *testing.T) {
		c := &Client{
			metadata: map[reflect.Type]*metaCache{},
		}

		m, err := c.getMetadata(User{})
		assert.Equal(t, nil, err)
		assert.Equal(t, "users", m.TableName)
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
