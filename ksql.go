package ksql

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"unicode"

	"github.com/vingarcia/ksql/internal/modifiers"
	"github.com/vingarcia/ksql/internal/structs"
	"github.com/vingarcia/ksql/ksqlmodifiers"
	"github.com/vingarcia/ksql/sqldialect"
)

var selectQueryCache = initializeQueryCache()

func initializeQueryCache() map[string]*sync.Map {
	cache := map[string]*sync.Map{}
	for dname := range sqldialect.SupportedDialects {
		cache[dname] = &sync.Map{}
	}

	return cache
}

// DB represents the KSQL client responsible for
// interfacing with the "database/sql" package implementing
// the KSQL interface `ksql.Provider`.
type DB struct {
	dialect sqldialect.Provider
	db      DBAdapter
}

// DBAdapter is minimalistic interface to decouple our implementation
// from database/sql, i.e. if any struct implements the functions below
// with the exact same semantic as the sql package it will work with KSQL.
//
// To create a new client using this adapter use `ksql.NewWithAdapter()`
type DBAdapter interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (Rows, error)
}

// TxBeginner needs to be implemented by the DBAdapter in order to make it possible
// to use the `ksql.Transaction()` function.
type TxBeginner interface {
	BeginTx(ctx context.Context) (Tx, error)
}

// Result stores information about the result of an Exec query
type Result interface {
	LastInsertId() (int64, error)
	RowsAffected() (int64, error)
}

// Rows represents the results from a call to Query()
type Rows interface {
	Scan(...interface{}) error
	Close() error
	Next() bool
	Err() error
	Columns() ([]string, error)
}

// ScanArgError is a type of error that is expected to be returned
// from the Scan() method of the Rows interface.
//
// It should be returned when there is an error scanning one of the input
// values.
//
// This is necessary in order to allow KSQL to produce a better and more
// readable error message when this type of error occur.
type ScanArgError struct {
	ColumnIndex int
	Err         error
}

// Error implements the error interface.
func (s ScanArgError) Error() string {
	return fmt.Sprintf(
		"error scanning input attribute with index %d: %s",
		s.ColumnIndex, s.Err,
	)
}

func (s ScanArgError) ErrorWithStructNames(structName string, colName string) error {
	return fmt.Errorf(
		"error scanning %s.%s: %w",
		structName, colName, s.Err,
	)
}

// Tx represents a transaction and is expected to be returned by the DBAdapter.BeginTx function
type Tx interface {
	DBAdapter

	Rollback(ctx context.Context) error
	Commit(ctx context.Context) error
}

// Config describes the optional arguments accepted
// by the `ksql.New()` function.
type Config struct {
	// MaxOpenCons defaults to 1 if not set
	MaxOpenConns int

	// Used by some adapters (such as kpgx) where nil disables TLS
	TLSConfig *tls.Config
}

// SetDefaultValues should be called by all adapters
// to set the default config values if unset.
func (c *Config) SetDefaultValues() {
	if c.MaxOpenConns == 0 {
		c.MaxOpenConns = 1
	}
}

// NewWithAdapter allows the user to insert a custom implementation
// of the DBAdapter interface
func NewWithAdapter(
	adapter DBAdapter,
	dialect sqldialect.Provider,
) (DB, error) {
	if dialect == nil {
		return DB{}, fmt.Errorf("expected a valid sqldialect.Provider as argument but got `nil`")
	}

	return DB{
		dialect: dialect,
		db:      adapter,
	}, nil
}

// Query queries several rows from the database,
// the input should be a slice of structs (or *struct) passed
// by reference and it will be filled with all the results.
//
// Note: it is very important to make sure the query will
// return a small known number of results, otherwise you risk
// of overloading the available memory.
func (c DB) Query(
	ctx context.Context,
	records interface{},
	query string,
	params ...interface{},
) (err error) {
	slicePtr := reflect.ValueOf(records)
	slicePtrType := slicePtr.Type()
	if slicePtrType.Kind() != reflect.Ptr {
		return fmt.Errorf("KSQL: expected to receive a pointer to slice of structs, but got: %T", records)
	}
	sliceType := slicePtrType.Elem()
	slice := slicePtr.Elem()
	structType, isSliceOfPtrs, err := structs.DecodeAsSliceOfStructs(sliceType)
	if err != nil {
		return err
	}

	if isSliceOfPtrs {
		// Truncate the slice so there is no risk
		// of overwritting records that were already saved
		// on the slice:
		slice = slice.Slice(0, 0)
	}

	info, err := structs.GetTagInfo(structType)
	if err != nil {
		return err
	}

	firstToken := strings.ToUpper(getFirstToken(query))
	if info.IsNestedStruct && firstToken == "SELECT" {
		// This error check is necessary, since if we can't build the select part of the query this feature won't work.
		return fmt.Errorf("can't generate SELECT query for nested struct: when using this feature omit the SELECT part of the query")
	}

	if firstToken == "FROM" {
		selectPrefix, err := buildSelectQuery(c.dialect, structType, info, selectQueryCache[c.dialect.DriverName()])
		if err != nil {
			return err
		}
		query = selectPrefix + query
	}

	defer ctxLog(ctx, query, params, &err)

	rows, err := c.db.QueryContext(ctx, query, params...)
	if err != nil {
		return fmt.Errorf("error running query: %w", err)
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

		elemPtr := slice.Index(idx).Addr()
		if isSliceOfPtrs {
			// This is necessary since scanRows expects a *record not a **record
			elemPtr = elemPtr.Elem()
		}

		err = scanRows(ctx, c.dialect, rows, elemPtr.Interface())
		if err != nil {
			return err
		}
	}

	if rows.Err() != nil {
		return fmt.Errorf("KSQL: unexpected error when parsing query result: %w", rows.Err())
	}

	if err := rows.Close(); err != nil {
		return fmt.Errorf("KSQL: unexpected error when closing query result rows: %w", err)
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
func (c DB) QueryOne(
	ctx context.Context,
	record interface{},
	query string,
	params ...interface{},
) (err error) {
	v := reflect.ValueOf(record)
	t := v.Type()
	if t.Kind() != reflect.Ptr {
		return fmt.Errorf("KSQL: expected to receive a pointer to struct, but got: %T", record)
	}

	if v.IsNil() {
		return fmt.Errorf("KSQL: expected a valid pointer to struct as argument but received a nil pointer: %v", record)
	}

	tStruct := t.Elem()
	if tStruct.Kind() != reflect.Struct {
		return fmt.Errorf("KSQL: expected to receive a pointer to struct, but got: %T", record)
	}

	info, err := structs.GetTagInfo(tStruct)
	if err != nil {
		return err
	}

	firstToken := strings.ToUpper(getFirstToken(query))
	if info.IsNestedStruct && firstToken == "SELECT" {
		// This error check is necessary, since if we can't build the select part of the query this feature won't work.
		return fmt.Errorf("can't generate SELECT query for nested struct: when using this feature omit the SELECT part of the query")
	}

	if firstToken == "FROM" {
		selectPrefix, err := buildSelectQuery(c.dialect, tStruct, info, selectQueryCache[c.dialect.DriverName()])
		if err != nil {
			return err
		}
		query = selectPrefix + query
	}

	defer ctxLog(ctx, query, params, &err)

	rows, err := c.db.QueryContext(ctx, query, params...)
	if err != nil {
		return fmt.Errorf("error running query: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return ErrRecordNotFound
	}

	err = scanRowsFromType(ctx, c.dialect, rows, record, t, v)
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
func (c DB) QueryChunks(
	ctx context.Context,
	parser ChunkParser,
) (err error) {
	fnValue := reflect.ValueOf(parser.ForEachChunk)
	chunkType, err := structs.ParseInputFunc(parser.ForEachChunk)
	if err != nil {
		return err
	}

	chunk := reflect.MakeSlice(chunkType, 0, parser.ChunkSize)

	structType, isSliceOfPtrs, err := structs.DecodeAsSliceOfStructs(chunkType)
	if err != nil {
		return err
	}

	info, err := structs.GetTagInfo(structType)
	if err != nil {
		return err
	}

	firstToken := strings.ToUpper(getFirstToken(parser.Query))
	if info.IsNestedStruct && firstToken == "SELECT" {
		// This error check is necessary, since if we can't build the select part of the query this feature won't work.
		return fmt.Errorf("can't generate SELECT query for nested struct: when using this feature omit the SELECT part of the query")
	}

	if firstToken == "FROM" {
		selectPrefix, err := buildSelectQuery(c.dialect, structType, info, selectQueryCache[c.dialect.DriverName()])
		if err != nil {
			return err
		}
		parser.Query = selectPrefix + parser.Query
	}

	defer ctxLog(ctx, parser.Query, parser.Params, &err)

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

		err = scanRows(ctx, c.dialect, rows, chunk.Index(idx).Addr().Interface())
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
func (c DB) Insert(
	ctx context.Context,
	table Table,
	record interface{},
) (err error) {
	v := reflect.ValueOf(record)
	t := v.Type()
	if err = assertStructPtr(t); err != nil {
		return fmt.Errorf(
			"KSQL: expected record to be a pointer to struct, but got: %T",
			record,
		)
	}

	if v.IsNil() {
		return fmt.Errorf("KSQL: expected a valid pointer to struct as argument but received a nil pointer: %v", record)
	}

	if err := table.validate(); err != nil {
		return fmt.Errorf("can't insert in ksql.Table: %w", err)
	}

	info, err := structs.GetTagInfo(t.Elem())
	if err != nil {
		return err
	}

	query, params, scanValues, err := buildInsertQuery(ctx, c.dialect, table, t, v, info, record)
	if err != nil {
		return err
	}

	defer ctxLog(ctx, query, params, &err)

	switch table.insertMethodFor(c.dialect) {
	case sqldialect.InsertWithReturning, sqldialect.InsertWithOutput:
		err = c.insertReturningIDs(ctx, query, params, scanValues, table.idColumns)
	case sqldialect.InsertWithLastInsertID:
		err = c.insertWithLastInsertID(ctx, t, v, info, record, query, params, table.idColumns[0])
	case sqldialect.InsertWithNoIDRetrieval:
		err = c.insertWithNoIDRetrieval(ctx, query, params)
	default:
		// Unsupported drivers should be detected on the New() function,
		// So we don't expect the code to ever get into this default case.
		err = fmt.Errorf("code error: unsupported driver `%s`", c.dialect.DriverName())
	}

	return err
}

func (c DB) insertReturningIDs(
	ctx context.Context,
	query string,
	params []interface{},
	scanValues []interface{},
	idNames []string,
) error {
	rows, err := c.db.QueryContext(ctx, query, params...)
	if err != nil {
		return err
	}
	defer rows.Close()

	if !rows.Next() {
		err := fmt.Errorf("unexpected error when retrieving the id columns from the database")
		if rows.Err() != nil {
			err = rows.Err()
		}

		return err
	}

	err = rows.Scan(scanValues...)
	if err != nil {
		return err
	}

	return rows.Close()
}

func (c DB) insertWithLastInsertID(
	ctx context.Context,
	t reflect.Type,
	v reflect.Value,
	info structs.StructInfo,
	record interface{},
	query string,
	params []interface{},
	idName string,
) error {
	result, err := c.db.ExecContext(ctx, query, params...)
	if err != nil {
		return fmt.Errorf("error running insert query: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("error fetching LastInsertId: %w", err)
	}

	vID := reflect.ValueOf(id)

	fieldValue := v.Elem().Field(info.ByName(idName).Index)
	fieldType := fieldValue.Type()

	baseFieldKind := fieldType.Kind()
	leafFieldKind := baseFieldKind
	if baseFieldKind == reflect.Pointer {
		leafFieldKind = fieldType.Elem().Kind()
	}

	switch leafFieldKind {
	case reflect.Int, reflect.Int64, reflect.Uint, reflect.Uint64:
		if baseFieldKind == reflect.Pointer {
			// If fieldValue is nil allocate memory for it:
			if fieldValue.IsNil() {
				fieldValue.Set(reflect.New(fieldType.Elem()))
			}

			fieldValue.Elem().Set(vID.Convert(fieldType.Elem()))
			return nil
		}

		fieldValue.Set(vID.Convert(fieldType))
		return nil

	case reflect.String:
		// In case the record's ID is a string,
		// we cannot retrieve it, so we just return:
		return nil

	default:
		return fmt.Errorf(
			"error scanning field `%s` cannot assign last insert id of type int64 into field of type %v",
			idName,
			fieldType,
		)
	}
}

func (c DB) insertWithNoIDRetrieval(
	ctx context.Context,
	query string,
	params []interface{},
) error {
	_, err := c.db.ExecContext(ctx, query, params...)
	return err
}

func assertStructPtr(t reflect.Type) error {
	if t.Kind() != reflect.Ptr {
		return fmt.Errorf("expected a Kind of Ptr but got: %s", t)
	}
	if t.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("expected a Kind of Ptr to Struct but got: %s", t)
	}
	return nil
}

// Delete deletes one record from the database using the ID or IDs
// defined on the `ksql.Table` passed as second argument.
//
// For tables with a single ID column you can pass the record
// to be deleted as a struct, as a map or just pass the ID itself.
//
// For tables with composite keys you must pass the record
// as a struct or a map so that KSQL can read all the composite keys
// from it.
//
// The examples below should work for both types of tables:
//
//	err := c.Delete(ctx, UsersTable, user)
//
//	err := c.Delete(ctx, UserPostsTable, map[string]interface{}{
//	    "user_id": user.ID,
//	    "post_id": post.ID,
//	})
//
// The example below is shorter but will only work for tables with a single primary key:
//
//	err := c.Delete(ctx, UsersTable, user.ID)
func (c DB) Delete(
	ctx context.Context,
	table Table,
	idOrRecord interface{},
) (err error) {
	if err := table.validate(); err != nil {
		return fmt.Errorf("can't delete from ksql.Table: %w", err)
	}

	idMap, err := normalizeIDsAsMap(table.idColumns, idOrRecord)
	if err != nil {
		return err
	}

	var query string
	var params []interface{}
	query, params = buildDeleteQuery(c.dialect, table, idMap)

	defer ctxLog(ctx, query, params, &err)

	result, err := c.db.ExecContext(ctx, query, params...)
	if err != nil {
		return err
	}

	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("unable to check if the record was succesfully deleted: %w", err)
	}

	if n == 0 {
		return ErrRecordNotFound
	}

	return err
}

func normalizeIDsAsMap(idNames []string, idOrMap interface{}) (idMap map[string]interface{}, err error) {
	if len(idNames) == 0 {
		return nil, fmt.Errorf("internal ksql error: missing idNames")
	}

	t := reflect.TypeOf(idOrMap)
	if t.Kind() == reflect.Ptr {
		v := reflect.ValueOf(idOrMap)
		if v.IsNil() {
			return nil, fmt.Errorf("KSQL: expected a valid pointer to struct as argument but received a nil pointer: %v", idOrMap)
		}
		t = t.Elem()
	}

	switch t.Kind() {
	case reflect.Struct:
		idMap, err = structs.StructToMap(idOrMap)
		if err != nil {
			return nil, fmt.Errorf("could not get ID(s) from input record: %w", err)
		}
	case reflect.Map:
		var ok bool
		idMap, ok = idOrMap.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("expected map[string]interface{} but got %T", idOrMap)
		}
	default:
		idMap = map[string]interface{}{
			idNames[0]: idOrMap,
		}
	}

	return idMap, validateIfAllIdsArePresent(idNames, idMap)
}

// Patch applies a partial update (explained below) to the given instance on the database by id.
//
// Partial updates will ignore any nil pointer attributes from the struct, updating only
// the non nil pointers and non pointer attributes.
func (c DB) Patch(
	ctx context.Context,
	table Table,
	record interface{},
) (err error) {
	v := reflect.ValueOf(record)
	t := v.Type()
	tStruct := t
	if t.Kind() == reflect.Ptr {
		if v.IsNil() {
			return fmt.Errorf("KSQL: expected a valid pointer to struct as argument but received a nil pointer: %v", record)
		}
		tStruct = t.Elem()
	}
	info, err := structs.GetTagInfo(tStruct)
	if err != nil {
		return err
	}

	recordMap, err := structs.StructToMap(record)
	if err != nil {
		return err
	}

	query, params, err := buildUpdateQuery(ctx, c.dialect, table.name, info, recordMap, table.idColumns...)
	if err != nil {
		return err
	}

	defer ctxLog(ctx, query, params, &err)

	result, err := c.db.ExecContext(ctx, query, params...)
	if err != nil {
		return err
	}

	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf(
			"unexpected error: unable to fetch how many rows were affected by the update: %w",
			err,
		)
	}
	if n < 1 {
		return ErrRecordNotFound
	}

	return nil
}

func buildInsertQuery(
	ctx context.Context,
	dialect sqldialect.Provider,
	table Table,
	t reflect.Type,
	v reflect.Value,
	info structs.StructInfo,
	record interface{},
) (query string, params []interface{}, scanValues []interface{}, err error) {
	recordMap, err := structs.StructToMap(record)
	if err != nil {
		return "", nil, nil, err
	}

	for _, fieldName := range table.idColumns {
		field, found := recordMap[fieldName]
		if !found {
			continue
		}

		hasValueModifier := info.ByName(fieldName).Modifier.Value != nil

		// Remove any ID field that was not set:
		if reflect.ValueOf(field).IsZero() && !hasValueModifier {
			delete(recordMap, fieldName)
		}
	}

	columnNames := []string{}
	for col := range recordMap {
		if info.ByName(col).Modifier.SkipOnInsert {
			continue
		}

		columnNames = append(columnNames, col)
	}

	params = make([]interface{}, len(columnNames))
	valuesQuery := make([]string, len(columnNames))
	for i, col := range columnNames {
		recordValue := recordMap[col]
		params[i] = recordValue

		valueFn := info.ByName(col).Modifier.Value
		if valueFn != nil {
			params[i] = modifiers.AttrValueWrapper{
				Ctx:     ctx,
				Attr:    recordValue,
				ValueFn: valueFn,
				OpInfo: ksqlmodifiers.OpInfo{
					DriverName: dialect.DriverName(),
					Method:     "Insert",
				},
			}
		}

		valuesQuery[i] = dialect.Placeholder(i)
	}

	// Escape all cols to be sure they will be interpreted as column names:
	escapedColumnNames := []string{}
	for _, col := range columnNames {
		escapedColumnNames = append(escapedColumnNames, dialect.Escape(col))
	}

	var returningQuery, outputQuery string
	switch dialect.InsertMethod() {
	case sqldialect.InsertWithReturning:
		escapedIDNames := []string{}
		for _, id := range table.idColumns {
			escapedIDNames = append(escapedIDNames, dialect.Escape(id))
		}
		returningQuery = " RETURNING " + strings.Join(escapedIDNames, ", ")

		for _, id := range table.idColumns {
			scanValues = append(
				scanValues,
				v.Elem().Field(info.ByName(id).Index).Addr().Interface(),
			)
		}
	case sqldialect.InsertWithOutput:
		escapedIDNames := []string{}
		for _, id := range table.idColumns {
			escapedIDNames = append(escapedIDNames, "INSERTED."+dialect.Escape(id))
		}
		outputQuery = " OUTPUT " + strings.Join(escapedIDNames, ", ")

		for _, id := range table.idColumns {
			scanValues = append(
				scanValues,
				v.Elem().Field(info.ByName(id).Index).Addr().Interface(),
			)
		}
	}

	if len(columnNames) == 0 && dialect.DriverName() != "mysql" {
		query = fmt.Sprintf(
			"INSERT INTO %s%s DEFAULT VALUES%s",
			table.name,
			outputQuery,
			returningQuery,
		)
		return query, params, scanValues, nil
	}

	// Note that the outputQuery and the returningQuery depend
	// on the selected driver, thus, they might be empty strings.
	query = fmt.Sprintf(
		"INSERT INTO %s (%s)%s VALUES (%s)%s",
		table.name,
		strings.Join(escapedColumnNames, ", "),
		outputQuery,
		strings.Join(valuesQuery, ", "),
		returningQuery,
	)

	return query, params, scanValues, nil
}

func buildUpdateQuery(
	ctx context.Context,
	dialect sqldialect.Provider,
	tableName string,
	info structs.StructInfo,
	recordMap map[string]interface{},
	idFieldNames ...string,
) (query string, args []interface{}, err error) {
	for key := range recordMap {
		if info.ByName(key).Modifier.SkipOnUpdate {
			delete(recordMap, key)
		}
	}

	numAttrs := len(recordMap)
	args = make([]interface{}, numAttrs)

	err = validateIfAllIdsArePresent(idFieldNames, recordMap)
	if err != nil {
		return "", nil, err
	}

	numNonIDArgs := numAttrs - len(idFieldNames)
	whereArgs := args[numNonIDArgs:]
	if numNonIDArgs == 0 {
		return "", nil, ErrNoValuesToUpdate
	}

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
		recordValue := recordMap[k]

		valueFn := info.ByName(k).Modifier.Value
		if valueFn != nil {
			recordValue = modifiers.AttrValueWrapper{
				Ctx:     ctx,
				Attr:    recordValue,
				ValueFn: valueFn,
				OpInfo: ksqlmodifiers.OpInfo{
					DriverName: dialect.DriverName(),
					Method:     "Update",
				},
			}
		}
		args[i] = recordValue
		setQuery = append(setQuery, fmt.Sprintf(
			"%s = %s",
			dialect.Escape(k),
			dialect.Placeholder(i),
		))
	}

	query = fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s",
		tableName,
		strings.Join(setQuery, ", "),
		strings.Join(whereQuery, " AND "),
	)

	return query, args, nil
}

func validateIfAllIdsArePresent(idNames []string, idMap map[string]interface{}) error {
	for _, idName := range idNames {
		id, found := idMap[idName]
		if !found {
			return fmt.Errorf("missing required id field `%s` on input record: %w", idName, ErrRecordMissingIDs)
		}

		if id == nil || reflect.ValueOf(id).IsZero() {
			return fmt.Errorf("invalid value '%v' received for id column: '%s': %w", id, idName, ErrRecordMissingIDs)
		}
	}

	return nil
}

// Exec just runs an SQL command on the database returning no rows.
func (c DB) Exec(ctx context.Context, query string, params ...interface{}) (_ Result, err error) {
	defer ctxLog(ctx, query, params, &err)

	return c.db.ExecContext(ctx, query, params...)
}

// Transaction encapsulates several queries into a single transaction.
// All these queries should be made inside the input callback `fn`
// and they should use the input ksql.Provider.
//
// If the callback returns any errors the transaction will be rolled back,
// otherwise the transaction will me committed.
//
// If it happens that a second transaction is started inside a transaction
// callback the same transaction will be reused with no errors.
func (c DB) Transaction(ctx context.Context, fn func(Provider) error) error {
	switch txBeginner := c.db.(type) {
	case Tx:
		return fn(c)
	case TxBeginner:
		tx, err := txBeginner.BeginTx(ctx)
		if err != nil {
			return fmt.Errorf("KSQL: error starting transaction: %w", err)
		}
		defer func() {
			if r := recover(); r != nil {
				rollbackErr := tx.Rollback(ctx)
				if rollbackErr != nil {
					r = fmt.Errorf(
						"KSQL: unable to rollback after panic with value: %v, rollback error: %w",
						r, rollbackErr,
					)
				}
				panic(r)
			}
		}()

		dbCopy := c
		dbCopy.db = tx

		err = fn(dbCopy)
		if err != nil {
			rollbackErr := tx.Rollback(ctx)
			if rollbackErr != nil {
				err = fmt.Errorf(
					"KSQL: unable to rollback after error: %s, rollback error: %w",
					err, rollbackErr,
				)
			}
			return err
		}

		return tx.Commit(ctx)

	default:
		return fmt.Errorf("KSQL: can't start transaction: The DBAdapter doesn't implement the TxBeginner interface")
	}
}

// Close implements the io.Closer interface
func (c DB) Close() error {
	closer, ok := c.db.(io.Closer)
	if ok {
		return closer.Close()
	}
	return nil
}

type nopScanner struct{}

var nopScannerValue = reflect.ValueOf(&nopScanner{}).Interface()

func (nopScanner) Scan(value interface{}) error {
	return nil
}

func scanRows(ctx context.Context, dialect sqldialect.Provider, rows Rows, record interface{}) error {
	v := reflect.ValueOf(record)
	t := v.Type()
	return scanRowsFromType(ctx, dialect, rows, record, t, v)
}

func scanRowsFromType(
	ctx context.Context,
	dialect sqldialect.Provider,
	rows Rows,
	record interface{},
	t reflect.Type,
	v reflect.Value,
) error {
	if t.Kind() != reflect.Ptr {
		return fmt.Errorf("KSQL: expected record to be a pointer to struct, but got: %T", record)
	}

	v = v.Elem()
	t = t.Elem()

	if t.Kind() != reflect.Struct {
		return fmt.Errorf("KSQL: expected record to be a pointer to struct, but got: %T", record)
	}

	info, err := structs.GetTagInfo(t)
	if err != nil {
		return err
	}

	var attrNames []string
	var scanArgs []interface{}
	var nestedPtrGroups []nestedPtrGroup
	if info.IsNestedStruct {
		// This version is positional meaning that it expect the arguments
		// to follow an specific order. It's ok because we don't allow the
		// user to type the "SELECT" part of the query for nested structs.
		attrNames, scanArgs, nestedPtrGroups, err = getScanArgsForNestedStructs(ctx, dialect, rows, t, v, info)
		if err != nil {
			return err
		}
	} else {
		colNames, err := rows.Columns()
		if err != nil {
			return fmt.Errorf("KSQL: unable to read columns from returned rows: %w", err)
		}
		// Since this version uses the names of the columns it works
		// with any order of attributes/columns.
		attrNames, scanArgs = getScanArgsFromNames(ctx, dialect, colNames, v, info)
	}

	err = rows.Scan(scanArgs...)
	if err != nil {
		if scanErr, ok := err.(ScanArgError); ok {
			return fmt.Errorf(
				"KSQL: scan error: %w",
				scanErr.ErrorWithStructNames(t.Name(), attrNames[scanErr.ColumnIndex]),
			)
		}
		return fmt.Errorf("KSQL: scan error: %w", err)
	}

	if len(nestedPtrGroups) > 0 {
		if err := materializeNestedPtrGroups(v, nestedPtrGroups); err != nil {
			return err
		}
	}
	return nil
}

// nestedPtrField carries the temporaries needed to materialize a single field
// of an optional joined struct (`*Struct`) after rows.Scan.
type nestedPtrField struct {
	// index is the position of this field within the nested struct.
	index int

	// isPtr is true when the nested field is itself a pointer (`*T`).
	isPtr bool

	// temp holds the value scanned from the database:
	//   - without modifier: a `**base`, so temp.Elem() is the `*base` slot the
	//     driver sets to nil on NULL;
	//   - with modifier: a `*T` written by the modifier, whose nullness is read
	//     from wrapper.WasNull instead.
	temp reflect.Value

	// wrapper is non-nil when the field uses a modifier; WasNull then tells
	// whether the column was NULL.
	wrapper *modifiers.AttrScanWrapper

	// name is the "Outer.Field" attribute name, used for error messages.
	name string
}

func (f nestedPtrField) wasNull() bool {
	if f.wrapper != nil {
		return f.wrapper.WasNull
	}
	return f.temp.Elem().IsNil()
}

// nestedPtrGroup collects everything needed to materialize one optional joined
// struct field after rows.Scan.
type nestedPtrGroup struct {
	// fieldIndex is the index of the `*Struct` field on the outer record.
	fieldIndex int

	// elemType is the struct type pointed to by the field (the deref of `*Struct`).
	elemType reflect.Type

	fields []nestedPtrField
}

func getScanArgsForNestedStructs(
	ctx context.Context,
	dialect sqldialect.Provider,
	rows Rows,
	t reflect.Type,
	v reflect.Value,
	info structs.StructInfo,
) (attrNames []string, scanArgs []interface{}, nestedPtrGroups []nestedPtrGroup, _ error) {
	for _, field := range info.Fields {
		fieldType := t.Field(field.Index).Type
		isOptional := fieldType.Kind() == reflect.Ptr
		nestedStructType := fieldType
		if isOptional {
			nestedStructType = fieldType.Elem()
		}

		nestedStructInfo, err := structs.GetTagInfo(nestedStructType)
		if err != nil {
			return nil, nil, nil, err
		}

		outerAttrName := info.ByIndex(field.Index).AttrName

		if !isOptional {
			// Value struct: unchanged path, scan straight into the fields.
			nestedStructValue := v.Field(field.Index)
			for _, nestedField := range nestedStructInfo.Fields {
				valueScanner := nestedStructValue.Field(nestedField.Index).Addr().Interface()
				if nestedField.Modifier.Scan != nil {
					valueScanner = &modifiers.AttrScanWrapper{
						Ctx:     ctx,
						AttrPtr: valueScanner,
						ScanFn:  nestedField.Modifier.Scan,
						OpInfo: ksqlmodifiers.OpInfo{
							DriverName: dialect.DriverName(),
							// We will not differentiate between Query, QueryOne and QueryChunks
							// if we did this could lead users to make very strange modifiers
							Method: "Query",
						},
					}
				}

				scanArgs = append(scanArgs, valueScanner)
				attrNames = append(attrNames, outerAttrName+"."+nestedField.AttrName)
			}
			continue
		}

		// Optional joined struct (`*Struct`): the outer pointer is nil, so we
		// scan into temporaries and materialize the struct after rows.Scan.
		group := nestedPtrGroup{
			fieldIndex: field.Index,
			elemType:   nestedStructType,
		}
		for _, nestedField := range nestedStructInfo.Fields {
			nestedFieldType := nestedStructType.Field(nestedField.Index).Type
			ptrField := nestedPtrField{
				index: nestedField.Index,
				isPtr: nestedFieldType.Kind() == reflect.Ptr,
				name:  outerAttrName + "." + nestedField.AttrName,
			}

			if nestedField.Modifier.Scan != nil {
				// The modifier writes into a real T; nullness is tracked apart.
				ptrField.temp = reflect.New(nestedFieldType)
				ptrField.wrapper = &modifiers.AttrScanWrapper{
					Ctx:       ctx,
					AttrPtr:   ptrField.temp.Interface(),
					ScanFn:    nestedField.Modifier.Scan,
					TrackNull: true,
					OpInfo: ksqlmodifiers.OpInfo{
						DriverName: dialect.DriverName(),
						Method:     "Query",
					},
				}
				scanArgs = append(scanArgs, ptrField.wrapper)
			} else {
				// Design B2: use a `**base` temporary so the driver sets the
				// inner `*base` to nil on NULL. A field that is already a pointer
				// reuses its own indirection so we never pass triple indirection
				// (`***U`) to the driver, which many drivers cannot handle.
				tempType := nestedFieldType
				if !ptrField.isPtr {
					tempType = reflect.PtrTo(nestedFieldType)
				}
				ptrField.temp = reflect.New(tempType)
				scanArgs = append(scanArgs, ptrField.temp.Interface())
			}

			group.fields = append(group.fields, ptrField)
			attrNames = append(attrNames, ptrField.name)
		}
		nestedPtrGroups = append(nestedPtrGroups, group)
	}

	return attrNames, scanArgs, nestedPtrGroups, nil
}

// materializeNestedPtrGroups fills each optional joined struct field after
// rows.Scan: all columns NULL ⇒ nil (reset explicitly, since Query/QueryChunks
// reuse slice elements); some column non-NULL ⇒ allocate and copy; a NULL in a
// non-nullable field of an otherwise present struct ⇒ error naming the field.
func materializeNestedPtrGroups(v reflect.Value, groups []nestedPtrGroup) error {
	for _, group := range groups {
		anyNonNull := false
		for _, f := range group.fields {
			if !f.wasNull() {
				anyNonNull = true
				break
			}
		}

		fieldVal := v.Field(group.fieldIndex)
		if !anyNonNull {
			fieldVal.Set(reflect.Zero(fieldVal.Type()))
			continue
		}

		newStruct := reflect.New(group.elemType)
		elem := newStruct.Elem()
		for _, f := range group.fields {
			dst := elem.Field(f.index)
			switch {
			case f.wrapper != nil:
				// The modifier owns its own NULL semantics; on NULL we leave the
				// destination at its zero value.
				if !f.wrapper.WasNull {
					dst.Set(f.temp.Elem())
				}
			case f.isPtr:
				dst.Set(f.temp.Elem()) // *base, possibly nil for a nullable field
			default:
				if f.temp.Elem().IsNil() {
					return fmt.Errorf(
						"KSQL: unexpected null value for non-nullable field '%s' of an optional joined struct",
						f.name,
					)
				}
				dst.Set(f.temp.Elem().Elem())
			}
		}
		fieldVal.Set(newStruct)
	}

	return nil
}

func getScanArgsFromNames(
	ctx context.Context,
	dialect sqldialect.Provider,
	names []string,
	v reflect.Value,
	info structs.StructInfo,
) (attrNames []string, scanArgs []interface{}) {
	for _, name := range names {
		fieldInfo := info.ByName(name)

		valueScanner := nopScannerValue
		if fieldInfo.Valid {
			valueScanner = v.Field(fieldInfo.Index).Addr().Interface()
			if fieldInfo.Modifier.Scan != nil {
				valueScanner = &modifiers.AttrScanWrapper{
					Ctx:     ctx,
					AttrPtr: valueScanner,
					ScanFn:  fieldInfo.Modifier.Scan,
					OpInfo: ksqlmodifiers.OpInfo{
						DriverName: dialect.DriverName(),
						// We will not differentiate between Query, QueryOne and QueryChunks
						// if we did this could lead users to make very strange modifiers
						Method: "Query",
					},
				}
			}
		}

		scanArgs = append(scanArgs, valueScanner)
		attrNames = append(attrNames, fieldInfo.AttrName)
	}

	return attrNames, scanArgs
}

func buildDeleteQuery(
	dialect sqldialect.Provider,
	table Table,
	idMap map[string]interface{},
) (query string, params []interface{}) {
	whereQuery := []string{}
	for i, idName := range table.idColumns {
		whereQuery = append(whereQuery, fmt.Sprintf(
			"%s = %s", dialect.Escape(idName), dialect.Placeholder(i),
		))
		params = append(params, idMap[idName])
	}

	return fmt.Sprintf(
		"DELETE FROM %s WHERE %s",
		table.name,
		strings.Join(whereQuery, " AND "),
	), params
}

// We implemented this function instead of using
// a regex or strings.Fields because we wanted
// to preserve the performance of the package.
func getFirstToken(s string) string {
	s = strings.TrimLeftFunc(s, unicode.IsSpace)

	var token strings.Builder
	for _, c := range s {
		if unicode.IsSpace(c) {
			break
		}
		token.WriteRune(c)
	}
	return token.String()
}

func buildSelectQuery(
	dialect sqldialect.Provider,
	structType reflect.Type,
	info structs.StructInfo,
	selectQueryCache *sync.Map,
) (query string, err error) {
	if data, found := selectQueryCache.Load(structType); found {
		if selectQuery, ok := data.(string); !ok {
			return "", fmt.Errorf("invalid cache entry, expected type string, found %T", data)
		} else {
			return selectQuery, nil
		}
	}

	if info.IsNestedStruct {
		query, err = buildSelectQueryForNestedStructs(dialect, structType, info)
		if err != nil {
			return "", err
		}
	} else {
		query = buildSelectQueryForPlainStructs(dialect, structType, info)
	}

	selectQueryCache.Store(structType, query)
	return query, nil
}

func buildSelectQueryForPlainStructs(
	dialect sqldialect.Provider,
	structType reflect.Type,
	info structs.StructInfo,
) string {
	var fields []string
	for _, field := range info.Fields {
		fields = append(fields, dialect.Escape(field.ColumnName))
	}

	return "SELECT " + strings.Join(fields, ", ") + " "
}

func buildSelectQueryForNestedStructs(
	dialect sqldialect.Provider,
	structType reflect.Type,
	info structs.StructInfo,
) (string, error) {
	var fields []string
	for _, fieldInfo := range info.Fields {
		nestedStructName := fieldInfo.ColumnName
		nestedStructType := structType.Field(fieldInfo.Index).Type
		// An optional joined struct (`*Struct`) selects the exact same
		// columns as its value counterpart, so we just deref the type here.
		if nestedStructType.Kind() == reflect.Ptr {
			nestedStructType = nestedStructType.Elem()
		}
		if nestedStructType.Kind() != reflect.Struct {
			return "", fmt.Errorf(
				"expected nested struct with `tablename:\"%s\"` to be a kind of Struct, but got %v",
				nestedStructName, nestedStructType,
			)
		}

		structInfo, err := structs.GetTagInfo(nestedStructType)
		if err != nil {
			return "", err
		}

		for _, nestedFieldInfo := range structInfo.Fields {
			fields = append(
				fields,
				dialect.Escape(nestedStructName)+"."+dialect.Escape(nestedFieldInfo.ColumnName),
			)
		}
	}

	return "SELECT " + strings.Join(fields, ", ") + " ", nil
}

// QueryFromBuilder is a helper for making it easier to use a QueryBuilder,
// it builds a query using the builder param and forwards the results to Query()
//
// The input struct should be a reference to a slice of structs or struct pointers
// just like for the Query function.
//
// Note: it is very important to make sure the query will
// return a small known number of results, otherwise you risk
// of overloading the available memory.
func (c DB) QueryFromBuilder(ctx context.Context, records interface{}, builder QueryBuilder) (err error) {
	query, params, err := builder.BuildQuery(c.dialect)
	if err != nil {
		return err
	}

	return c.Query(ctx, records, query, params...)
}

// ExecFromBuilder is a helper for making it easier to use a QueryBuilder,
// it builds a query using the builder param and forwards the results to Exec()
func (c DB) ExecFromBuilder(ctx context.Context, builder QueryBuilder) (_ Result, err error) {
	query, params, err := builder.BuildQuery(c.dialect)
	if err != nil {
		return nil, err
	}

	return c.Exec(ctx, query, params...)
}
