package ksql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/vingarcia/ksql/sqldialect"
)

// ErrRecordNotFound informs that a given query failed because the record was not found.
// This error can be returned by the following methods: Update(), QueryOne() and Delete().
var ErrRecordNotFound error = fmt.Errorf("ksql: the query returned no results: %w", sql.ErrNoRows)

// ErrNoValuesToUpdate informs the error of trying to make an update that would not change any attributes.
//
// This could happen if all the non-ID attributes of the struct are being ignored or if they don't exist.
//
// Since ID attributes are ignored by the Patch() method updating a struct that only have IDs will result in this error.
// And this error will also occur if the struct does have some non-ID attributes but they are all being ignored.
//
// The reasons that can cause an attribute to be ignored in the Patch() function are:
// (1) If it is a nil pointer, Patch() will just ignore it.
// (2) If the attribute is using a modifier that contains the SkipUpdates flag.
var ErrNoValuesToUpdate error = fmt.Errorf("ksql: the input struct contains no values to update")

// ErrRecordMissingIDs is returned by the Update or Delete functions if an input record does
// not have all of the IDs described on the input table.
var ErrRecordMissingIDs error = fmt.Errorf("ksql: missing required ID fields")

// ErrAbortIteration should be used inside the QueryChunks function to inform QueryChunks it should stop querying,
// close the connection and return with no errors.
var ErrAbortIteration error = fmt.Errorf("ksql: abort iteration, should only be used inside QueryChunks function")

// Provider describes the ksql public behavior.
//
// The Insert, Update, Delete and QueryOne functions return ksql.ErrRecordNotFound
// if no record was found or no rows were changed during the operation.
type Provider interface {
	Insert(ctx context.Context, table Table, record interface{}) error
	Patch(ctx context.Context, table Table, record interface{}) error
	Delete(ctx context.Context, table Table, idOrRecord interface{}) error

	// Deprecated: use the Patch() method instead.
	Update(ctx context.Context, table Table, record interface{}) error

	Query(ctx context.Context, records interface{}, query string, params ...interface{}) error
	QueryOne(ctx context.Context, record interface{}, query string, params ...interface{}) error
	QueryChunks(ctx context.Context, parser ChunkParser) error

	Exec(ctx context.Context, query string, params ...interface{}) (Result, error)
	Transaction(ctx context.Context, fn func(Provider) error) error
}

// Table describes the required information for inserting, updating and
// deleting entities from the database by ID using the 3 helper functions
// created for that purpose.
type Table struct {
	// this name must be set in order to use the Insert, Delete and Update helper
	// functions. If you only intend to make queries or to use the Exec function
	// it is safe to leave this field unset.
	name string

	// IDColumns defaults to []string{"id"} if unset
	idColumns []string
}

// NewTable returns a Table instance that stores
// the tablename and the names of columns used as ID,
// if no column name is passed it defaults to using
// the `"id"` column.
//
// This Table is required only for using the helper methods:
//
// - Insert
// - Update
// - Delete
//
// Passing multiple ID columns will be interpreted
// as a single composite key, if you want
// to use the helper functions with different
// keys you'll need to create multiple Table instances
// for the same database table, each with a different
// set of ID columns, but this is usually not necessary.
func NewTable(tableName string, ids ...string) Table {
	if len(ids) == 0 {
		ids = []string{"id"}
	}

	return Table{
		name:      tableName,
		idColumns: ids,
	}
}

func (t Table) validate() error {
	if t.name == "" {
		return fmt.Errorf("table name cannot be an empty string")
	}

	for _, fieldName := range t.idColumns {
		if fieldName == "" {
			return fmt.Errorf("ID columns cannot be empty strings")
		}
	}

	return nil
}

func (t Table) insertMethodFor(dialect sqldialect.Provider) sqldialect.InsertMethod {
	if len(t.idColumns) == 1 {
		return dialect.InsertMethod()
	}

	insertMethod := dialect.InsertMethod()
	if insertMethod == sqldialect.InsertWithLastInsertID {
		return sqldialect.InsertWithNoIDRetrieval
	}

	return insertMethod
}

// ChunkParser stores the arguments of the QueryChunks function
type ChunkParser struct {
	// The Query and Params are used together to build a query with
	// protection from injection, just like when using the Find function.
	Query  string
	Params []interface{}

	ChunkSize int

	// This attribute must be a function with the following signature:
	//
	// `func(chunk []<Record>) error`.
	//
	// Where the actual Record type should be of a struct
	// representing the rows you are expecting to receive.
	ForEachChunk interface{}
}
