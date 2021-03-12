package ksql

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/pkg/errors"
	"github.com/vingarcia/ksql/structs"
)

// DB represents the ksql client responsible for
// interfacing with the "database/sql" package implementing
// the KissSQL interface `SQLProvider`.
type DB struct {
	driver    string
	dialect   dialect
	tableName string
	db        sqlProvider

	// Most dbs have a single primary key,
	// But in future ksql should work with compound keys as well
	idCols []string

	insertMethod insertMethod
}

type sqlProvider interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

type insertMethod int

const (
	insertWithReturning insertMethod = iota
	insertWithLastInsertID
	insertWithNoIDRetrieval
)

// Config describes the optional arguments accepted
// by the ksql.New() function.
type Config struct {
	// MaxOpenCons defaults to 1 if not set
	MaxOpenConns int

	// TableName must be set in order to use the Insert, Delete and Update helper
	// functions. If you only intend to make queries or to use the Exec function
	// it is safe to leave this field unset.
	TableName string

	// IDColumns defaults to []string{"id"} if unset
	IDColumns []string
}

// New instantiates a new KissSQL client
func New(
	dbDriver string,
	connectionString string,
	config Config,
) (DB, error) {
	db, err := sql.Open(dbDriver, connectionString)
	if err != nil {
		return DB{}, err
	}
	if err = db.Ping(); err != nil {
		return DB{}, err
	}

	if config.MaxOpenConns == 0 {
		config.MaxOpenConns = 1
	}

	db.SetMaxOpenConns(config.MaxOpenConns)

	dialect := getDriverDialect(dbDriver)
	if dialect == nil {
		return DB{}, fmt.Errorf("unsupported driver `%s`", dbDriver)
	}

	if len(config.IDColumns) == 0 {
		config.IDColumns = []string{"id"}
	}

	var insertMethod insertMethod
	switch dbDriver {
	case "sqlite3":
		insertMethod = insertWithLastInsertID
		if len(config.IDColumns) > 1 {
			insertMethod = insertWithNoIDRetrieval
		}
	case "postgres":
		insertMethod = insertWithReturning
	default:
		return DB{}, fmt.Errorf("unsupported driver `%s`", dbDriver)
	}

	return DB{
		dialect:   dialect,
		driver:    dbDriver,
		db:        db,
		tableName: config.TableName,

		idCols:       config.IDColumns,
		insertMethod: insertMethod,
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
) error {
	slicePtr := reflect.ValueOf(records)
	slicePtrType := slicePtr.Type()
	if slicePtrType.Kind() != reflect.Ptr {
		return fmt.Errorf("ksql: expected to receive a pointer to slice of structs, but got: %T", records)
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

		elemPtr := slice.Index(idx).Addr()
		if isSliceOfPtrs {
			// This is necessary since scanRows expects a *record not a **record
			elemPtr = elemPtr.Elem()
		}

		err = scanRows(rows, elemPtr.Interface())
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
func (c DB) QueryOne(
	ctx context.Context,
	record interface{},
	query string,
	params ...interface{},
) error {
	t := reflect.TypeOf(record)
	if t.Kind() != reflect.Ptr {
		return fmt.Errorf("ksql: expected to receive a pointer to struct, but got: %T", record)
	}
	t = t.Elem()
	if t.Kind() != reflect.Struct {
		return fmt.Errorf("ksql: expected to receive a pointer to struct, but got: %T", record)
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
func (c DB) QueryChunks(
	ctx context.Context,
	parser ChunkParser,
) error {
	fnValue := reflect.ValueOf(parser.ForEachChunk)
	chunkType, err := parseInputFunc(parser.ForEachChunk)
	if err != nil {
		return err
	}

	chunk := reflect.MakeSlice(chunkType, 0, parser.ChunkSize)

	structType, isSliceOfPtrs, err := structs.DecodeAsSliceOfStructs(chunkType)
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
func (c DB) Insert(
	ctx context.Context,
	records ...interface{},
) error {
	if c.tableName == "" {
		return fmt.Errorf("the optional TableName argument was not provided to New(), can't use the Insert method")
	}

	for _, record := range records {
		query, params, err := buildInsertQuery(c.dialect, c.tableName, record, c.idCols...)
		if err != nil {
			return err
		}

		switch c.insertMethod {
		case insertWithReturning:
			err = c.insertWithReturningID(ctx, record, query, params, c.idCols)
		case insertWithLastInsertID:
			err = c.insertWithLastInsertID(ctx, record, query, params, c.idCols[0])
		case insertWithNoIDRetrieval:
			err = c.insertWithNoIDRetrieval(ctx, record, query, params)
		default:
			// Unsupported drivers should be detected on the New() function,
			// So we don't expect the code to ever get into this default case.
			err = fmt.Errorf("code error: unsupported driver `%s`", c.driver)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func (c DB) insertWithReturningID(
	ctx context.Context,
	record interface{},
	query string,
	params []interface{},
	idNames []string,
) error {
	escapedIDNames := []string{}
	for _, id := range idNames {
		escapedIDNames = append(escapedIDNames, c.dialect.Escape(id))
	}
	query += " RETURNING " + strings.Join(idNames, ", ")

	rows, err := c.db.QueryContext(ctx, query, params...)
	if err != nil {
		return err
	}
	defer rows.Close()

	if !rows.Next() {
		err := fmt.Errorf("unexpected error retrieving the id columns from the database")
		if rows.Err() != nil {
			err = rows.Err()
		}

		return err
	}

	v := reflect.ValueOf(record)
	t := v.Type()
	if err = assertStructPtr(t); err != nil {
		return errors.Wrap(err, "can't write id field")
	}
	info := structs.GetTagInfo(t.Elem())

	var scanFields []interface{}
	for _, id := range idNames {
		scanFields = append(
			scanFields,
			v.Elem().Field(info.ByName(id).Index).Addr().Interface(),
		)
	}
	err = rows.Scan(scanFields...)
	if err != nil {
		return err
	}

	return rows.Close()
}

func (c DB) insertWithLastInsertID(
	ctx context.Context,
	record interface{},
	query string,
	params []interface{},
	idName string,
) error {
	result, err := c.db.ExecContext(ctx, query, params...)
	if err != nil {
		return err
	}

	v := reflect.ValueOf(record)
	t := v.Type()
	if err = assertStructPtr(t); err != nil {
		return errors.Wrap(err, "can't write to `"+idName+"` field")
	}

	info := structs.GetTagInfo(t.Elem())

	id, err := result.LastInsertId()
	if err != nil {
		return err
	}

	vID := reflect.ValueOf(id)
	tID := vID.Type()

	fieldAddr := v.Elem().Field(info.ByName(idName).Index).Addr()
	fieldType := fieldAddr.Type().Elem()

	if !tID.ConvertibleTo(fieldType) {
		return fmt.Errorf(
			"Can't convert last insert id of type int64 into field `%s` of type %s",
			idName,
			fieldType,
		)
	}

	fieldAddr.Elem().Set(vID.Convert(fieldType))
	return nil
}

func (c DB) insertWithNoIDRetrieval(
	ctx context.Context,
	record interface{},
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

// Delete deletes one or more instances from the database by id
func (c DB) Delete(
	ctx context.Context,
	ids ...interface{},
) error {
	if c.tableName == "" {
		return fmt.Errorf("the optional TableName argument was not provided to New(), can't use the Delete method")
	}

	if len(ids) == 0 {
		return nil
	}

	idMaps, err := normalizeIDsAsMaps(c.idCols, ids)
	if err != nil {
		return err
	}

	var query string
	var params []interface{}
	if len(c.idCols) == 1 {
		query, params = buildSingleKeyDeleteQuery(c.dialect, c.tableName, c.idCols[0], idMaps)
	} else {
		query, params = buildCompositeKeyDeleteQuery(c.dialect, c.tableName, c.idCols, idMaps)
	}

	_, err = c.db.ExecContext(ctx, query, params...)

	return err
}

func normalizeIDsAsMaps(idNames []string, ids []interface{}) ([]map[string]interface{}, error) {
	if len(idNames) == 0 {
		return nil, fmt.Errorf("internal ksql error: missing idNames")
	}

	idMaps := []map[string]interface{}{}
	for i := range ids {
		t := reflect.TypeOf(ids[i])
		switch t.Kind() {
		case reflect.Struct:
			m, err := structs.StructToMap(ids[i])
			if err != nil {
				return nil, errors.Wrapf(err, "could not get ID(s) from record on idx %d", i)
			}
			idMaps = append(idMaps, m)
		case reflect.Map:
			m, ok := ids[i].(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("expected map[string]interface{} but got %T", ids[i])
			}
			idMaps = append(idMaps, m)
		default:
			idMaps = append(idMaps, map[string]interface{}{
				idNames[0]: ids[i],
			})
		}
	}

	for i, m := range idMaps {
		for _, id := range idNames {
			if _, found := m[id]; !found {
				return nil, fmt.Errorf("missing required id field `%s` on record with idx %d", id, i)
			}
		}
	}

	return idMaps, nil
}

// Update updates the given instances on the database by id.
//
// Partial updates are supported, i.e. it will ignore nil pointer attributes
func (c DB) Update(
	ctx context.Context,
	records ...interface{},
) error {
	if c.tableName == "" {
		return fmt.Errorf("the optional TableName argument was not provided to New(), can't use the Update method")
	}

	for _, record := range records {
		query, params, err := buildUpdateQuery(c.dialect, c.tableName, record, c.idCols...)
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
	recordMap, err := structs.StructToMap(record)
	if err != nil {
		return "", nil, err
	}

	t := reflect.TypeOf(record)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	info := structs.GetTagInfo(t)

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
		recordValue := recordMap[col]
		params[i] = recordValue
		if info.ByName(col).SerializeAsJSON {
			params[i] = jsonSerializable{Attr: recordValue}
		}

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
	recordMap, err := structs.StructToMap(record)
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

	t := reflect.TypeOf(record)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	info := structs.GetTagInfo(t)

	var setQuery []string
	for i, k := range keys {
		recordValue := recordMap[k]
		if info.ByName(k).SerializeAsJSON {
			recordValue = jsonSerializable{Attr: recordValue}
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
		dialect.Escape(tableName),
		strings.Join(setQuery, ", "),
		strings.Join(whereQuery, ", "),
	)

	return query, args, nil
}

// Exec just runs an SQL command on the database returning no rows.
func (c DB) Exec(ctx context.Context, query string, params ...interface{}) error {
	_, err := c.db.ExecContext(ctx, query, params...)
	return err
}

// Transaction just runs an SQL command on the database returning no rows.
func (c DB) Transaction(ctx context.Context, fn func(SQLProvider) error) error {
	switch db := c.db.(type) {
	case *sql.Tx:
		return fn(c)
	case *sql.DB:
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer func() {
			if r := recover(); r != nil {
				rollbackErr := tx.Rollback()
				if rollbackErr != nil {
					r = errors.Wrap(rollbackErr,
						fmt.Sprintf("unable to rollback after panic with value: %v", r),
					)
				}
				panic(r)
			}
		}()

		ormCopy := c
		ormCopy.db = tx

		err = fn(ormCopy)
		if err != nil {
			rollbackErr := tx.Rollback()
			if rollbackErr != nil {
				err = errors.Wrap(rollbackErr,
					fmt.Sprintf("unable to rollback after error: %s", err.Error()),
				)
			}
			return err
		}

		return tx.Commit()

	default:
		return fmt.Errorf("unexpected error on ksql: db attribute has an invalid type")
	}
}

var errType = reflect.TypeOf(new(error)).Elem()

func parseInputFunc(fn interface{}) (reflect.Type, error) {
	if fn == nil {
		return nil, fmt.Errorf("the ForEachChunk attribute is required and cannot be nil")
	}

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

type nopScanner struct{}

var nopScannerValue = reflect.ValueOf(&nopScanner{}).Interface()

func (nopScanner) Scan(value interface{}) error {
	return nil
}

func scanRows(rows *sql.Rows, record interface{}) error {
	names, err := rows.Columns()
	if err != nil {
		return err
	}

	v := reflect.ValueOf(record)
	t := v.Type()
	if t.Kind() != reflect.Ptr {
		return fmt.Errorf("ksql: expected record to be a pointer to struct, but got: %T", record)
	}

	v = v.Elem()
	t = t.Elem()

	if t.Kind() != reflect.Struct {
		return fmt.Errorf("ksql: expected record to be a pointer to struct, but got: %T", record)
	}

	info := structs.GetTagInfo(t)

	scanArgs := []interface{}{}
	for _, name := range names {
		fieldInfo := info.ByName(name)

		valueScanner := nopScannerValue
		if fieldInfo.Valid {
			valueScanner = v.Field(fieldInfo.Index).Addr().Interface()
			if fieldInfo.SerializeAsJSON {
				valueScanner = &jsonSerializable{Attr: valueScanner}
			}
		}

		scanArgs = append(scanArgs, valueScanner)
	}

	return rows.Scan(scanArgs...)
}

func buildSingleKeyDeleteQuery(
	dialect dialect,
	table string,
	idName string,
	idMaps []map[string]interface{},
) (query string, params []interface{}) {
	values := []string{}
	for i, m := range idMaps {
		values = append(values, dialect.Placeholder(i))
		params = append(params, m[idName])
	}

	return fmt.Sprintf(
		"DELETE FROM %s WHERE %s IN (%s)",
		dialect.Escape(table),
		dialect.Escape(idName),
		strings.Join(values, ","),
	), params
}

func buildCompositeKeyDeleteQuery(
	dialect dialect,
	table string,
	idNames []string,
	idMaps []map[string]interface{},
) (query string, params []interface{}) {
	escapedNames := []string{}
	for _, name := range idNames {
		escapedNames = append(escapedNames, dialect.Escape(name))
	}

	values := []string{}
	for _, m := range idMaps {
		tuple := []string{}
		for _, name := range idNames {
			params = append(params, m[name])
			tuple = append(tuple, dialect.Placeholder(len(values)))
		}
		values = append(values, "("+strings.Join(tuple, ",")+")")
	}

	return fmt.Sprintf(
		"DELETE FROM %s WHERE (%s) IN (VALUES %s)",
		dialect.Escape(table),
		strings.Join(escapedNames, ","),
		strings.Join(values, ","),
	), params
}
