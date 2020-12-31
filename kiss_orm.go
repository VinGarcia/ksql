package kissorm

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

// Client ...
type Client struct {
	driver    string
	dialect   dialect
	tableName string
	db        *sql.DB
}

// NewClient instantiates a new client
func NewClient(
	dbDriver string,
	connectionString string,
	maxOpenConns int,
	tableName string,
) (Client, error) {
	db, err := sql.Open(dbDriver, connectionString)
	if err != nil {
		return Client{}, err
	}
	if err = db.Ping(); err != nil {
		return Client{}, err
	}

	db.SetMaxOpenConns(maxOpenConns)

	dialect := getDriverDialect(dbDriver)
	if dialect == nil {
		return Client{}, fmt.Errorf("unsupported driver `%s`", dbDriver)
	}

	return Client{
		dialect:   dialect,
		driver:    dbDriver,
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
// the input should be a slice of structs (or *struct) passed
// by reference and it will be filled with all the results.
//
// Note: it is very important to make sure the query will
// return a small known number of results, otherwise you risk
// of overloading the available memory.
func (c Client) Query(
	ctx context.Context,
	records interface{},
	query string,
	params ...interface{},
) error {
	slicePtr := reflect.ValueOf(records)
	slicePtrType := slicePtr.Type()
	if slicePtrType.Kind() != reflect.Ptr {
		return fmt.Errorf("kissorm: expected to receive a pointer to slice of structs, but got: %T", records)
	}
	sliceType := slicePtrType.Elem()
	slice := slicePtr.Elem()
	structType, isSliceOfPtrs, err := decodeAsSliceOfStructs(sliceType)
	if err != nil {
		return err
	}

	if isSliceOfPtrs {
		// Truncate the slice so there is no risk
		// of overwritting records that were already saved
		// on the slice:
		slice = slice.Slice(0, 0)
	}

	rows, err := c.db.QueryContext(ctx, query, params...)
	if err != nil {
		return fmt.Errorf("error running query: %s", err.Error())
	}
	defer rows.Close()

	for idx := 0; rows.Next(); idx++ {
		// Allocate new slice elements
		// only if they are not already allocated:
		if slice.Len() <= idx {
			var elemValue reflect.Value
			elemValue = reflect.New(structType)
			if !isSliceOfPtrs {
				elemValue = elemValue.Elem()
			}
			slice = reflect.Append(slice, elemValue)
		}

		err = scanRows(rows, slice.Index(idx).Addr().Interface())
		if err != nil {
			return err
		}
	}

	if err := rows.Close(); err != nil {
		return err
	}

	if rows.Err() != nil {
		return rows.Err()
	}

	// Update the original slice passed by reference:
	slicePtr.Elem().Set(slice)

	return nil
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

	rows, err := c.db.QueryContext(ctx, query, params...)
	if err != nil {
		return err
	}
	defer rows.Close()

	if !rows.Next() {
		if rows.Err() != nil {
			return rows.Err()
		}
		return ErrRecordNotFound
	}

	err = scanRows(rows, record)
	if err != nil {
		return err
	}

	return rows.Close()
}

// QueryChunks is meant to perform queries that returns
// more results than would normally fit on memory,
// for others cases the Query and QueryOne functions are indicated.
//
// The ChunkParser argument has 4 attributes:
// (1) The Query;
// (2) The query args;
// (3) The chunk size;
// (4) A callback function called ForEachChunk, that will be called
// to process each chunk loaded from the database.
//
// Note that the signature of the ForEachChunk callback can be
// any function that receives a slice of structs or a slice of
// pointers to struct as its only argument and that reflection
// will be used to instantiate this argument and to fill it
// with the database rows.
func (c Client) QueryChunks(
	ctx context.Context,
	parser ChunkParser,
) error {
	fnValue := reflect.ValueOf(parser.ForEachChunk)
	chunkType, err := parseInputFunc(parser.ForEachChunk)
	if err != nil {
		return err
	}

	chunk := reflect.MakeSlice(chunkType, 0, parser.ChunkSize)

	structType, isSliceOfPtrs, err := decodeAsSliceOfStructs(chunkType)
	if err != nil {
		return err
	}

	rows, err := c.db.QueryContext(ctx, parser.Query, parser.Params...)
	if err != nil {
		return err
	}
	defer rows.Close()

	var idx = 0
	for rows.Next() {
		// Allocate new slice elements
		// only if they are not already allocated:
		if chunk.Len() <= idx {
			var elemValue reflect.Value
			elemValue = reflect.New(structType)
			if !isSliceOfPtrs {
				elemValue = elemValue.Elem()
			}
			chunk = reflect.Append(chunk, elemValue)
		}

		err = scanRows(rows, chunk.Index(idx).Addr().Interface())
		if err != nil {
			return err
		}

		if idx < parser.ChunkSize-1 {
			idx++
			continue
		}

		idx = 0
		err, _ = fnValue.Call([]reflect.Value{chunk})[0].Interface().(error)
		if err != nil {
			if err == ErrAbortIteration {
				return nil
			}
			return err
		}
	}

	if err := rows.Close(); err != nil {
		return err
	}

	// If Next() returned false because of an error:
	if rows.Err() != nil {
		return rows.Err()
	}

	// If no rows were found or idx was reset to 0
	// on the last iteration skip this last call to ForEachChunk:
	if idx > 0 {
		chunk = chunk.Slice(0, idx)

		err, _ = fnValue.Call([]reflect.Value{chunk})[0].Interface().(error)
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
	for _, record := range records {
		query, params, err := buildInsertQuery(c.dialect, c.tableName, record, "id")
		if err != nil {
			return err
		}

		switch c.driver {
		case "postgres":
			err = c.insertOnPostgres(ctx, record, query, params)
		case "sqlite3":
			err = c.insertWithLastInsertID(ctx, record, query, params)
		default:
			err = fmt.Errorf("unsupported driver `%s`", c.driver)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func (c Client) insertOnPostgres(
	ctx context.Context,
	record interface{},
	query string,
	params []interface{},
) error {
	query = query + " RETURNING id"

	rows, err := c.db.QueryContext(ctx, query, params...)
	if err != nil {
		return err
	}
	defer rows.Close()

	if !rows.Next() {
		err := fmt.Errorf("unexpected error retrieving the id from the database")
		if rows.Err() != nil {
			err = rows.Err()
		}

		return err
	}

	v := reflect.ValueOf(record)
	t := v.Type()
	info := getCachedTagInfo(tagInfoCache, t.Elem())

	fieldAddr := v.Elem().Field(info.Index["id"]).Addr()
	err = rows.Scan(fieldAddr.Interface())
	if err != nil {
		return err
	}

	return rows.Close()
}

func (c Client) insertWithLastInsertID(
	ctx context.Context,
	record interface{},
	query string,
	params []interface{},
) error {
	result, err := c.db.ExecContext(ctx, query, params...)
	if err != nil {
		return err
	}

	v := reflect.ValueOf(record)
	t := v.Type()
	info := getCachedTagInfo(tagInfoCache, t.Elem())

	id, err := result.LastInsertId()
	if err != nil {
		return err
	}

	fieldAddr := v.Elem().Field(info.Index["id"]).Addr()
	fieldAddr.Elem().Set(reflect.ValueOf(id).Convert(fieldAddr.Elem().Type()))
	return nil
}

// Delete deletes one or more instances from the database by id
func (c Client) Delete(
	ctx context.Context,
	ids ...interface{},
) error {
	if len(ids) == 0 {
		return nil
	}

	query := buildDeleteQuery(c.dialect, c.tableName, ids)

	_, err := c.db.ExecContext(ctx, query, ids...)

	return err
}

// Update updates the given instances on the database by id.
//
// Partial updates are supported, i.e. it will ignore nil pointer attributes
func (c Client) Update(
	ctx context.Context,
	records ...interface{},
) error {
	for _, record := range records {
		query, params, err := buildUpdateQuery(c.dialect, c.tableName, record, "id")
		if err != nil {
			return err
		}

		_, err = c.db.ExecContext(ctx, query, params...)
		if err != nil {
			return err
		}
	}

	return nil
}

func buildInsertQuery(
	dialect dialect,
	tableName string,
	record interface{},
	idFieldNames ...string,
) (query string, params []interface{}, err error) {
	recordMap, err := StructToMap(record)
	if err != nil {
		return "", nil, err
	}

	for _, fieldName := range idFieldNames {
		// Remove any ID field that was not set:
		if reflect.ValueOf(recordMap[fieldName]).IsZero() {
			delete(recordMap, fieldName)
		}
	}

	columnNames := []string{}
	for col := range recordMap {
		columnNames = append(columnNames, col)
	}

	params = make([]interface{}, len(recordMap))
	valuesQuery := make([]string, len(recordMap))
	for i, col := range columnNames {
		params[i] = recordMap[col]
		valuesQuery[i] = dialect.Placeholder(i)
	}

	// Escape all cols to be sure they will be interpreted as column names:
	escapedColumnNames := []string{}
	for _, col := range columnNames {
		escapedColumnNames = append(escapedColumnNames, dialect.Escape(col))
	}

	query = fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		dialect.Escape(tableName),
		strings.Join(escapedColumnNames, ", "),
		strings.Join(valuesQuery, ", "),
	)

	return query, params, nil
}

func buildUpdateQuery(
	dialect dialect,
	tableName string,
	record interface{},
	idFieldNames ...string,
) (query string, args []interface{}, err error) {
	recordMap, err := StructToMap(record)
	if err != nil {
		return "", nil, err
	}
	numAttrs := len(recordMap)
	args = make([]interface{}, numAttrs)
	numNonIDArgs := numAttrs - len(idFieldNames)
	whereArgs := args[numNonIDArgs:]

	whereQuery := make([]string, len(idFieldNames))
	for i, fieldName := range idFieldNames {
		whereArgs[i] = recordMap[fieldName]
		whereQuery[i] = fmt.Sprintf(
			"%s = %s",
			dialect.Escape(fieldName),
			dialect.Placeholder(i+numNonIDArgs),
		)

		delete(recordMap, fieldName)
	}

	keys := []string{}
	for key := range recordMap {
		keys = append(keys, key)
	}

	var setQuery []string
	for i, k := range keys {
		args[i] = recordMap[k]
		setQuery = append(setQuery, fmt.Sprintf(
			"%s = %s",
			dialect.Escape(k),
			dialect.Placeholder(i),
		))
	}

	query = fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s",
		dialect.Escape(tableName),
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

	info := getCachedTagInfo(tagInfoCache, t)

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

	info := getCachedTagInfo(tagInfoCache, t)

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
	sliceRef := reflect.ValueOf(entities)
	sliceType := sliceRef.Type()
	if sliceType.Kind() != reflect.Ptr {
		return fmt.Errorf(
			"FillSliceWith: expected input to be a pointer to struct but got %v",
			sliceType,
		)
	}

	structType, isSliceOfPtrs, err := decodeAsSliceOfStructs(sliceType.Elem())
	if err != nil {
		return fmt.Errorf("FillSliceWith: %s", err.Error())
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

func decodeAsSliceOfStructs(slice reflect.Type) (
	structType reflect.Type,
	isSliceOfPtrs bool,
	err error,
) {
	if slice.Kind() != reflect.Slice {
		err = fmt.Errorf(
			"expected input kind to be a slice but got %v",
			slice,
		)
		return
	}

	elemType := slice.Elem()
	isPtr := elemType.Kind() == reflect.Ptr

	if isPtr {
		elemType = elemType.Elem()
	}

	if elemType.Kind() != reflect.Struct {
		err = fmt.Errorf(
			"expected input to be a slice of structs but got %v",
			slice,
		)
		return
	}

	return elemType, isPtr, nil
}

var errType = reflect.TypeOf(new(error)).Elem()

func parseInputFunc(fn interface{}) (reflect.Type, error) {
	t := reflect.TypeOf(fn)

	if t.Kind() != reflect.Func {
		return nil, fmt.Errorf("the ForEachChunk callback must be a function")
	}
	if t.NumIn() != 1 {
		return nil, fmt.Errorf("the ForEachChunk callback must have 1 argument")
	}

	if t.NumOut() != 1 {
		return nil, fmt.Errorf("the ForEachChunk callback must have a single return value")
	}

	if t.Out(0) != errType {
		return nil, fmt.Errorf("the return value of the ForEachChunk callback must be of type error")
	}

	argsType := t.In(0)
	if argsType.Kind() != reflect.Slice {
		return nil, fmt.Errorf("the argument of the ForEachChunk callback must a slice of structs")
	}

	return argsType, nil
}

func scanRows(rows *sql.Rows, record interface{}) error {
	names, err := rows.Columns()
	if err != nil {
		return err
	}

	v := reflect.ValueOf(record)
	t := v.Type()
	if t.Kind() != reflect.Ptr {
		return fmt.Errorf("kissorm: expected to receive a pointer to struct, but got: %T", record)
	}

	v = v.Elem()
	t = t.Elem()

	if t.Kind() != reflect.Struct {
		return fmt.Errorf("kissorm: expected to receive a pointer to slice of structs, but got: %T", record)
	}

	info := getCachedTagInfo(tagInfoCache, t)

	scanArgs := []interface{}{}
	for _, name := range names {
		scanArgs = append(scanArgs, v.Field(info.Index[name]).Addr().Interface())
	}

	return rows.Scan(scanArgs...)
}

func getCachedTagInfo(tagInfoCache map[reflect.Type]structInfo, key reflect.Type) structInfo {
	info, found := tagInfoCache[key]
	if !found {
		info = getTagNames(key)
		tagInfoCache[key] = info
	}
	return info
}

func buildDeleteQuery(dialect dialect, table string, ids []interface{}) string {
	values := []string{}
	for i := range ids {
		values = append(values, dialect.Placeholder(i))
	}

	return fmt.Sprintf(
		"DELETE FROM %s WHERE id IN (%s)",
		dialect.Escape(table),
		strings.Join(values, ","),
	)
}
