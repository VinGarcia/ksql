package kissorm

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/jinzhu/gorm"
)

// ORMProvider describes the public behavior of this ORM
type ORMProvider interface {
	Find(ctx context.Context, item interface{}, query string, params ...interface{}) error
	GetByID(ctx context.Context, item interface{}, id interface{}) error
	Insert(ctx context.Context, items ...interface{}) error
	Delete(ctx context.Context, ids ...interface{}) error
	Update(ctx context.Context, intems ...interface{}) error
	Query(ctx context.Context, query string, params ...interface{}) (Iterator, error)
	QueryNext(ctx context.Context, rawIt Iterator, item interface{}) (done bool, err error)
}

// Iterator ...
type Iterator interface {
	Close() error
}

// Client ...
type Client struct {
	tableName string
	db        *gorm.DB
}

// NewClient instantiates a new client
func NewClient(dbDriver string, connectionString string, maxOpenConns int, tableName string) (Client, error) {
	db, err := gorm.Open(dbDriver, connectionString)
	if err != nil {
		return Client{}, err
	}
	if err = db.DB().Ping(); err != nil {
		return Client{}, err
	}

	db.DB().SetMaxOpenConns(maxOpenConns)

	return Client{
		db:        db,
		tableName: tableName,
	}, nil
}

// ChangeTable creates a new client configured to query on a different table
func (c Client) ChangeTable(ctx context.Context, tableName string) ORMProvider {
	return &Client{
		db:        c.db,
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
	if it.Error != nil {
		return it.Error
	}
	it = it.Scan(item)
	return it.Error
}

// Query build an iterator for querying several
// results from the database
func (c Client) Query(
	ctx context.Context,
	query string,
	params ...interface{},
) (Iterator, error) {
	it := c.db.Raw(query, params...)
	if it.Error != nil {
		return nil, it.Error
	}

	return it.Rows()
}

// QueryNext parses the next row of a query
// and updates the item argument that must be
// passed by reference.
func (c Client) QueryNext(
	ctx context.Context,
	rawIt Iterator,
	item interface{},
) (done bool, err error) {
	rows, ok := rawIt.(*sql.Rows)
	if !ok {
		return false, fmt.Errorf("invalid iterator received on QueryNext()")
	}

	if !rows.Next() {
		rows.Close()
		return true, rows.Err()
	}

	return false, c.db.ScanRows(rows, item)
}

// GetByID recovers a single entity from the database by the ID field.
func (c Client) GetByID(
	ctx context.Context,
	item interface{},
	id interface{},
) error {
	it := c.db.Raw(fmt.Sprintf("select * from %s where id = ?", c.tableName), id)
	if it.Error != nil {
		return it.Error
	}
	it = it.Scan(item)
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

		r := c.db.Table(c.tableName).Model(item).Updates(m)
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
var tagInfoCache = map[reflect.Type]StructInfo{}

type StructInfo struct {
	Names map[int]string
	Index map[string]int
}

// structToMap converts any struct type to a map based on
// the tag named `gorm`, i.e. `gorm:"map_key_name"`
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

	info, found := tagInfoCache[t]
	if !found {
		info = getTagNames(t)
		tagInfoCache[t] = info
	}

	m := map[string]interface{}{}
	for i := 0; i < v.NumField(); i++ {
		if info.Names[i] == "id" {
			continue
		}
		field := v.Field(i)
		ft := field.Type()
		if ft.Kind() == reflect.Ptr {
			if field.IsNil() {
				continue
			}

			field = field.Elem()
		}

		m[info.Names[i]] = field.Interface()
	}

	return m, nil
}

// This function collects only the names
// that will be used from the input type.
//
// This should save several calls to `Field(i).Tag.Get("foo")`
// which improves performance by a lot.
func getTagNames(t reflect.Type) StructInfo {
	info := StructInfo{
		Names: map[int]string{},
		Index: map[string]int{},
	}
	for i := 0; i < t.NumField(); i++ {
		name := t.Field(i).Tag.Get("gorm")
		if name == "" {
			continue
		}
		info.Names[i] = name
		info.Index[name] = i
	}

	return info
}

// UpdateStructWith is meant to be used on unit tests to mock
// the response from the database.
//
// The first argument is any struct you are passing to a kissorm func,
// and the second is a map representing a database row you want
// to use to update this struct.
func UpdateStructWith(entity interface{}, db_row map[string]interface{}) error {
	v := reflect.ValueOf(entity)
	t := v.Type()

	if t.Kind() != reflect.Ptr {
		return fmt.Errorf(
			"UpdateStructWith: expected input to be a pointer to struct but got %T",
			entity,
		)
	}

	t = t.Elem()
	v = v.Elem()

	if t.Kind() != reflect.Struct {
		return fmt.Errorf(
			"UpdateStructWith: expected input to be a kind of struct but got %T",
			entity,
		)
	}

	info, found := tagInfoCache[t]
	if !found {
		info = getTagNames(t)
		tagInfoCache[t] = info
	}

	for colName, attr := range db_row {
		attrValue := reflect.ValueOf(attr)
		field := v.Field(info.Index[colName])
		fieldType := t.Field(info.Index[colName]).Type

		if !attrValue.Type().ConvertibleTo(fieldType) {
			return fmt.Errorf(
				"UpdateStructWith: cannot convert atribute %s of type %v to type %T",
				colName,
				fieldType,
				entity,
			)
		}
		field.Set(attrValue.Convert(fieldType))
	}

	return nil
}
