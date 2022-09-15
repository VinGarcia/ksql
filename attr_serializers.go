package ksql

import (
	"context"
	"database/sql/driver"
	"fmt"
)

// Here we keep all the registered serializers
var serializers = map[string]AttrSerializer{
	"json": jsonSerializer{},
}

// RegisterAttrSerializer allow users to add custom serializers on startup
// it is recommended to do this inside an init() function.
func RegisterAttrSerializer(key string, serializer AttrSerializer) {
	_, found := serializers[key]
	if found {
		panic(fmt.Errorf("KSQL: cannot register serializer '%s' name is already in use", key))
	}

	serializers[key] = serializer
}

// AttrSerializer describes the two operations required to serialize and deserialize an object from the database.
type AttrSerializer interface {
	AttrScan(ctx context.Context, opInfo OpInfo, attrPtr interface{}, dbValue interface{}) error
	AttrValue(ctx context.Context, opInfo OpInfo, inputValue interface{}) (outputValue interface{}, _ error)
}

// OpInfo contains information that might be used by a serializer to determine how it should behave.
type OpInfo struct {
	// A string version of the name of one of
	// the methods of the `ksql.Provider` interface, e.g. `Insert` or `Query`
	Method string

	// The string representing the current underlying database, e.g.:
	// "postgres", "sqlite3", "mysql" or "sqlserver".
	DriverName string
}

// attrSerializer is the wrapper that allow us to intercept the Scan and Value processes
// so we can run the serializers instead of allowing the database driver to use
// its default behavior.
//
// For that this struct implements both the `sql.Scanner` and `sql.Valuer` interfaces.
type attrSerializer struct {
	ctx context.Context

	// When Scanning this value should be a pointer to the attribute
	// and when "Valuing" it should just be the actual value
	attr interface{}

	serializerName string
	opInfo         OpInfo
}

// Scan implements the sql.Scanner interface
func (a attrSerializer) Scan(dbValue interface{}) error {
	return serializers[a.serializerName].AttrScan(a.ctx, a.opInfo, a.attr, dbValue)
}

// Value implements the sql.Valuer interface
func (a attrSerializer) Value() (driver.Value, error) {
	return serializers[a.serializerName].AttrValue(a.ctx, a.opInfo, a.attr)
}
