package kissorm

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/jinzhu/gorm"
)

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

type iterator struct {
	isClosed bool
	rows     *sql.Rows
}

// Close ...
func (i *iterator) Close() error {
	if i.isClosed {
		return nil
	}
	i.isClosed = true
	return i.rows.Close()
}

var noopCloser = iterator{isClosed: true}

// Query builds an iterator for querying several
// results from the database
func (c Client) Query(
	ctx context.Context,
	query string,
	params ...interface{},
) (Iterator, error) {
	it := c.db.Raw(query, params...)
	if it.Error != nil {
		return &noopCloser, it.Error
	}

	rows, err := it.Rows()
	if err != nil {
		return &noopCloser, err
	}

	return &iterator{
		isClosed: false,
		rows:     rows,
	}, nil
}

// QueryNext parses the next row of a query
// and updates the item argument that must be
// passed by reference.
func (c Client) QueryNext(
	ctx context.Context,
	rawIt Iterator,
	item interface{},
) (done bool, err error) {
	it, ok := rawIt.(*iterator)
	if !ok {
		return false, fmt.Errorf("invalid iterator received on QueryNext()")
	}

	if it.isClosed {
		return false, fmt.Errorf("received closed iterator")
	}

	if !it.rows.Next() {
		it.Close()
		return true, it.rows.Err()
	}

	return false, c.db.ScanRows(it.rows, item)
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
		m, err := StructToMap(item)
		if err != nil {
			return err
		}
		delete(m, "id")
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
var tagInfoCache = map[reflect.Type]structInfo{}

type structInfo struct {
	Names map[int]string
	Index map[string]int
}

// StructToMap converts any struct type to a map based on
// the tag named `gorm`, i.e. `gorm:"map_key_name"`
//
// This function is efficient in the fact that it caches
// the slower steps of the reflection required to do perform
// this task.
func StructToMap(obj interface{}) (map[string]interface{}, error) {
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
func getTagNames(t reflect.Type) structInfo {
	info := structInfo{
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

// FillStructWith is meant to be used on unit tests to mock
// the response from the database.
//
// The first argument is any struct you are passing to a kissorm func,
// and the second is a map representing a database row you want
// to use to update this struct.
func FillStructWith(entity interface{}, dbRow map[string]interface{}) error {
	v := reflect.ValueOf(entity)
	t := v.Type()

	if t.Kind() != reflect.Ptr {
		return fmt.Errorf(
			"FillStructWith: expected input to be a pointer to struct but got %T",
			entity,
		)
	}

	t = t.Elem()
	v = v.Elem()

	if t.Kind() != reflect.Struct {
		return fmt.Errorf(
			"FillStructWith: expected input kind to be a struct but got %T",
			entity,
		)
	}

	info, found := tagInfoCache[t]
	if !found {
		info = getTagNames(t)
		tagInfoCache[t] = info
	}

	for colName, attr := range dbRow {
		attrValue := reflect.ValueOf(attr)
		field := v.Field(info.Index[colName])
		fieldType := t.Field(info.Index[colName]).Type

		if !attrValue.Type().ConvertibleTo(fieldType) {
			return fmt.Errorf(
				"FillStructWith: cannot convert atribute %s of type %v to type %T",
				colName,
				fieldType,
				entity,
			)
		}
		field.Set(attrValue.Convert(fieldType))
	}

	return nil
}

// FillSliceWith is meant to be used on unit tests to mock
// the response from the database.
//
// The first argument is any slice of structs you are passing to a kissorm func,
// and the second is a slice of maps representing the database rows you want
// to use to update this struct.
func FillSliceWith(entities interface{}, dbRows []map[string]interface{}) error {
	slicePtrValue := reflect.ValueOf(entities)
	slicePtrType := slicePtrValue.Type()

	if slicePtrType.Kind() != reflect.Ptr {
		return fmt.Errorf(
			"FillListWith: expected input to be a pointer to struct but got %T",
			entities,
		)
	}

	t := slicePtrType.Elem()
	v := slicePtrValue.Elem()

	if t.Kind() != reflect.Slice {
		return fmt.Errorf(
			"FillListWith: expected input kind to be a slice but got %T",
			entities,
		)
	}

	elemType := t.Elem()
	isPtr := elemType.Kind() == reflect.Ptr

	if isPtr {
		elemType = elemType.Elem()
	}

	info, found := tagInfoCache[elemType]
	if !found {
		info = getTagNames(elemType)
		tagInfoCache[elemType] = info
	}

	if elemType.Kind() != reflect.Struct {
		return fmt.Errorf(
			"FillListWith: expected input to be a slice of structs but got %T",
			entities,
		)
	}

	for idx, row := range dbRows {
		if v.Len() <= idx {
			var elemValue reflect.Value
			elemValue = reflect.New(elemType)
			if !isPtr {
				elemValue = elemValue.Elem()
			}
			v = reflect.Append(v, elemValue)
		}

		err := FillStructWith(v.Index(idx).Addr().Interface(), row)
		if err != nil {
			return err
		}
	}

	slicePtrValue.Elem().Set(v)

	return nil
}
