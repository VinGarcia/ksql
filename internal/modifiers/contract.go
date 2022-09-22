package modifiers

import "context"

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
