package gpostgres

import (
	"context"
	"fmt"
	"reflect"

	"github.com/jinzhu/gorm"
)

// Client ...
type Client struct {
	tableName string
	db        *gorm.DB
}

// NewClient ...
func NewClient(tableName string) Client {
	return Client{
		tableName: tableName,
	}
}

// Find one instance from the database, the input struct
// must be passed by reference and the query should
// return only one result.
func (c Client) Find(
	ctx context.Context,
	item interface{},
	query string,
	params ...interface{},
) error {
	it := c.db.Raw(query, params...)
	it.Scan(item)
	return it.Error
}

func (c Client) GetByID(
	ctx context.Context,
	item interface{},
	id interface{},
) error {
	it := c.db.Raw(fmt.Sprintf("select * from %s where id = ?", c.tableName), id)
	it.Scan(item)
	return it.Error
}

// Insert one or more instances on the database
//
// If the original instances have been passed by reference
// the ID is automatically updated after insertion is completed.
func (c Client) Insert(
	ctx context.Context,
	items ...interface{},
) error {
	if len(items) == 0 {
		return nil
	}

	for _, item := range items {
		r := c.db.Table(c.tableName).Create(item)
		if r.Error != nil {
			return r.Error
		}
	}

	return nil
}

// Delete deletes one or more instances from the database by id
func (c Client) Delete(
	ctx context.Context,
	ids ...interface{},
) error {
	for _, id := range ids {
		r := c.db.Table(c.tableName).Delete(id)
		if r.Error != nil {
			return r.Error
		}
	}

	return nil
}

// Update updates the given instances on the database by id.
//
// Partial updates are supported, i.e. it will ignore nil pointer attributes
func (c Client) Update(
	ctx context.Context,
	items ...interface{},
) error {
	for _, item := range items {
		m, err := structToMap(item)
		if err != nil {
			return err
		}

		r := c.db.Table(c.tableName).Updates(m)
		if r.Error != nil {
			return r.Error
		}
	}

	return nil
}

// This cache is kept as a pkg variable
// because the total number of types on a program
// should be finite. So keeping a single cache here
// works fine.
var tagNamesCache = map[reflect.Type]map[int]string{}

// structToMap converts any type to a map based on the
// tag named `gorm`, i.e. `gorm:"map_key_name"`
//
// This function is efficient in the fact that it caches
// the slower steps of the reflection required to do perform
// this task.
func structToMap(obj interface{}) (map[string]interface{}, error) {
	v := reflect.ValueOf(obj)
	t := v.Type()

	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		v = v.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("input must be a struct or struct pointer")
	}

	names, found := tagNamesCache[t]
	if !found {
		names = getTagNames(t)
		tagNamesCache[t] = names
	}

	m := map[string]interface{}{}
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		ft := field.Type()
		if ft.Kind() == reflect.Ptr {
			if field.IsNil() {
				continue
			}

			field = field.Elem()
		}

		m[names[i]] = field.Interface()
	}

	return m, nil
}

// This function collects only the names
// that will be used from the input type.
//
// This should save several calls to `Field(i).Tag.Get("foo")`
// which improves performance by a lot.
func getTagNames(t reflect.Type) map[int]string {
	resp := map[int]string{}
	for i := 0; i < t.NumField(); i++ {
		name := t.Field(i).Tag.Get("gorm")
		if name == "" {
			continue
		}
		resp[i] = name
	}

	return resp
}
