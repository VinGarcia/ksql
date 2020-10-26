package kissorm

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/jinzhu/gorm"
)

// Client ...
type Client struct {
	tableName string
	db        *gorm.DB
}

// NewClient instantiates a new client
func NewClient(
	dbDriver string,
	connectionString string,
	maxOpenConns int,
	tableName string,
) (Client, error) {
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

// Query queries several rows from the database,
// the input should be a slice of structs passed
// by reference and it will be filled with all the results.
//
// Note: it is very important to make sure the query will
// return a small number of results, otherwise you risk
// of overloading the available memory.
func (c Client) Query(
	ctx context.Context,
	records interface{},
	query string,
	params ...interface{},
) error {
	t := reflect.TypeOf(records)
	if t.Kind() != reflect.Ptr {
		return fmt.Errorf("kissorm: expected to receive a pointer to slice of structs, but got: %T", records)
	}
	t = t.Elem()
	if t.Kind() != reflect.Slice {
		return fmt.Errorf("kissorm: expected to receive a pointer to slice of structs, but got: %T", records)
	}
	if t.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("kissorm: expected to receive a pointer to slice of structs, but got: %T", records)
	}

	it := c.db.Raw(query, params...)
	if it.Error != nil {
		return it.Error
	}
	it = it.Scan(records)
	return it.Error
}

// QueryOne queries one instance from the database,
// the input struct must be passed by reference
// and the query should return only one result.
//
// QueryOne returns a ErrRecordNotFound if
// the query returns no results.
func (c Client) QueryOne(
	ctx context.Context,
	record interface{},
	query string,
	params ...interface{},
) error {
	t := reflect.TypeOf(record)
	if t.Kind() != reflect.Ptr {
		return fmt.Errorf("kissorm: expected to receive a pointer to struct, but got: %T", record)
	}
	t = t.Elem()
	if t.Kind() != reflect.Struct {
		return fmt.Errorf("kissorm: expected to receive a pointer to struct, but got: %T", record)
	}

	it := c.db.Raw(query, params...)
	if it.Error != nil {
		return it.Error
	}
	it = it.Scan(record)
	if it.Error != nil && it.Error.Error() == "record not found" {
		return ErrRecordNotFound
	}
	return it.Error
}

// QueryChunks is meant to perform queries that returns
// many results and should only be used for that purpose.
//
// It ChunkParser argument will inform the query and its params,
// and the information that will be used to iterate on the results,
// namely:
// (1) The Chunk, which must be a pointer to a slice of structs where
// the results of the query will be kept on each iteration.
// (2) The ChunkSize that describes how many rows should be loaded
// on the Chunk slice before running the iteration callback.
// (3) The ForEachChunk function, which is the iteration callback
// and will be called right after the Chunk is filled with rows
// and/or after the last row is read from the database.
func (c Client) QueryChunks(
	ctx context.Context,
	parser ChunkParser,
) error {
	it := c.db.Raw(parser.Query, parser.Params...)
	if it.Error != nil {
		return it.Error
	}

	rows, err := it.Rows()
	if err != nil {
		return err
	}
	defer rows.Close()

	sliceRef, structType, isSliceOfPtrs, err := decodeAsSliceOfStructs(parser.Chunk)
	if err != nil {
		return err
	}

	slice := sliceRef.Elem()
	if slice.Len() > parser.ChunkSize {
		slice = slice.Slice(0, parser.ChunkSize)
	}

	var idx = 0
	for rows.Next() {
		if slice.Len() <= idx {
			var elemValue reflect.Value
			elemValue = reflect.New(structType)
			if !isSliceOfPtrs {
				elemValue = elemValue.Elem()
			}
			slice = reflect.Append(slice, elemValue)
		}

		err = c.db.ScanRows(rows, slice.Index(idx).Addr().Interface())
		if err != nil {
			return err
		}

		if idx < parser.ChunkSize-1 {
			idx++
			continue
		}

		idx = 0
		sliceRef.Elem().Set(slice)
		err = parser.ForEachChunk()
		if err != nil {
			if err == ErrAbortIteration {
				return nil
			}
			return err
		}
	}

	// If no rows were found or idx was reset to 0
	// on the last iteration skip this last call to ForEachChunk:
	if idx > 0 {
		sliceRef.Elem().Set(slice.Slice(0, idx))
		err = parser.ForEachChunk()
		if err != nil {
			if err == ErrAbortIteration {
				return nil
			}
			return err
		}
	}

	return nil
}

// Insert one or more instances on the database
//
// If the original instances have been passed by reference
// the ID is automatically updated after insertion is completed.
func (c Client) Insert(
	ctx context.Context,
	records ...interface{},
) error {
	if len(records) == 0 {
		return nil
	}

	for _, record := range records {
		r := c.db.Table(c.tableName).Create(record)
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
	records ...interface{},
) error {
	for _, record := range records {
		query, params, err := buildUpdateQuery(c.tableName, record, "id")
		if err != nil {
			return err
		}

		_, err = c.db.DB().ExecContext(ctx, query, params...)
		if err != nil {
			return err
		}
	}

	return nil
}

func buildUpdateQuery(
	tableName string,
	record interface{},
	idFieldNames ...string,
) (query string, args []interface{}, err error) {
	recordMap, err := StructToMap(record)
	if err != nil {
		return "", nil, err
	}
	numAttrs := len(recordMap)
	numIDs := len(idFieldNames)
	args = make([]interface{}, numAttrs+numIDs)
	whereArgs := args[numAttrs-len(idFieldNames):]

	var whereQuery []string
	for i, fieldName := range idFieldNames {
		whereArgs[i] = recordMap[fieldName]
		whereQuery = append(whereQuery, fmt.Sprintf("`%s` = ?", fieldName))
		delete(recordMap, fieldName)
	}

	keys := []string{}
	for key := range recordMap {
		keys = append(keys, key)
	}

	var setQuery []string
	for i, k := range keys {
		args[i] = recordMap[k]
		setQuery = append(setQuery, fmt.Sprintf("`%s` = ?", k))
	}

	query = fmt.Sprintf(
		"UPDATE `%s` SET %s WHERE %s",
		tableName,
		strings.Join(setQuery, ", "),
		strings.Join(whereQuery, ", "),
	)

	return query, args, nil
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
// Valid pointers are dereferenced and copied to the map,
// null pointers are ignored.
//
// This function is efficient in the fact that it caches
// the slower steps of the reflection required to perform
// this task.
func StructToMap(obj interface{}) (map[string]interface{}, error) {
	v := reflect.ValueOf(obj)
	t := v.Type()

	if t.Kind() == reflect.Ptr {
		v = v.Elem()
		t = t.Elem()
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
func FillStructWith(record interface{}, dbRow map[string]interface{}) error {
	v := reflect.ValueOf(record)
	t := v.Type()

	if t.Kind() != reflect.Ptr {
		return fmt.Errorf(
			"FillStructWith: expected input to be a pointer to struct but got %T",
			record,
		)
	}

	t = t.Elem()
	v = v.Elem()

	if t.Kind() != reflect.Struct {
		return fmt.Errorf(
			"FillStructWith: expected input kind to be a struct but got %T",
			record,
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
				record,
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
	sliceRef, structType, isSliceOfPtrs, err := decodeAsSliceOfStructs(entities)
	if err != nil {
		return err
	}

	info, found := tagInfoCache[structType]
	if !found {
		info = getTagNames(structType)
		tagInfoCache[structType] = info
	}

	slice := sliceRef.Elem()
	for idx, row := range dbRows {
		if slice.Len() <= idx {
			var elemValue reflect.Value
			elemValue = reflect.New(structType)
			if !isSliceOfPtrs {
				elemValue = elemValue.Elem()
			}
			slice = reflect.Append(slice, elemValue)
		}

		err := FillStructWith(slice.Index(idx).Addr().Interface(), row)
		if err != nil {
			return err
		}
	}

	sliceRef.Elem().Set(slice)

	return nil
}

func decodeAsSliceOfStructs(slice interface{}) (
	sliceRef reflect.Value,
	structType reflect.Type,
	isSliceOfPtrs bool,
	err error,
) {
	slicePtrValue := reflect.ValueOf(slice)
	slicePtrType := slicePtrValue.Type()

	if slicePtrType.Kind() != reflect.Ptr {
		err = fmt.Errorf(
			"FillListWith: expected input to be a pointer to struct but got %T",
			slice,
		)
		return
	}

	t := slicePtrType.Elem()

	if t.Kind() != reflect.Slice {
		err = fmt.Errorf(
			"FillListWith: expected input kind to be a slice but got %T",
			slice,
		)
		return
	}

	elemType := t.Elem()
	isPtr := elemType.Kind() == reflect.Ptr

	if isPtr {
		elemType = elemType.Elem()
	}

	if elemType.Kind() != reflect.Struct {
		err = fmt.Errorf(
			"FillListWith: expected input to be a slice of structs but got %T",
			slice,
		)
		return
	}

	return slicePtrValue, elemType, isPtr, nil
}
