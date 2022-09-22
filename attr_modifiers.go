package ksql

import (
	"context"
	"database/sql/driver"
	"fmt"
)

// Here we keep all the registered modifier
var modifiers = map[string]AttrModifier{
	"json": jsonModifier{},
}

// RegisterAttrModifier allow users to add custom modifiers on startup
// it is recommended to do this inside an init() function.
func RegisterAttrModifier(key string, modifier AttrModifier) {
	_, found := modifiers[key]
	if found {
		panic(fmt.Errorf("KSQL: cannot register modifier '%s' name is already in use", key))
	}

	modifiers[key] = modifier
}

// AttrModifier describes the two operations required to serialize and deserialize an object from the database.
type AttrModifier interface {
	AttrScan(ctx context.Context, opInfo OpInfo, attrPtr interface{}, dbValue interface{}) error
	AttrValue(ctx context.Context, opInfo OpInfo, inputValue interface{}) (outputValue interface{}, _ error)
}

// OpInfo contains information that might be used by a modifier to determine how it should behave.
type OpInfo struct {
	// A string version of the name of one of
	// the methods of the `ksql.Provider` interface, e.g. `Insert` or `Query`
	Method string

	// The string representing the current underlying database, e.g.:
	// "postgres", "sqlite3", "mysql" or "sqlserver".
	DriverName string
}

// attrModifier is the wrapper that allow us to intercept the Scan and Value processes
// so we can run the modifiers instead of allowing the database driver to use
// its default behavior.
//
// For that this struct implements both the `sql.Scanner` and `sql.Valuer` interfaces.
type attrModifier struct {
	ctx context.Context

	// When Scanning this value should be a pointer to the attribute
	// and when "Valuing" it should just be the actual value
	attr interface{}

	modifierName string
	opInfo       OpInfo
}

// Scan implements the sql.Scanner interface
func (a attrModifier) Scan(dbValue interface{}) error {
	return modifiers[a.modifierName].AttrScan(a.ctx, a.opInfo, a.attr, dbValue)
}

// Value implements the sql.Valuer interface
func (a attrModifier) Value() (driver.Value, error) {
	return modifiers[a.modifierName].AttrValue(a.ctx, a.opInfo, a.attr)
}
