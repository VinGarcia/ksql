package modifiers

import (
	"context"
)

// AttrModifier informs KSQL how to use this modifier
type AttrModifier struct {
	// The following attributes will tell KSQL to
	// leave this attribute out of insertions, updates,
	// and queries respectively.
	SkipOnInsert bool
	SkipOnUpdate bool

	// Implement these functions if you want to override the default Scan/Value behavior
	// for the target attribute.
	Scan  AttrScanner
	Value AttrValuer
}

// AttrScanner describes the operation of deserializing an object received from the database.
type AttrScanner func(ctx context.Context, opInfo OpInfo, attrPtr interface{}, dbValue interface{}) error

// AttrValuer describes the operation of serializing an object when saving it to the database.
type AttrValuer func(ctx context.Context, opInfo OpInfo, inputValue interface{}) (outputValue interface{}, _ error)

// OpInfo contains information that might be used by a modifier to determine how it should behave.
type OpInfo struct {
	// A string version of the name of one of
	// the methods of the `ksql.Provider` interface, e.g. `Insert` or `Query`
	Method string

	// The string representing the current underlying database, e.g.:
	// "postgres", "sqlite3", "mysql" or "sqlserver".
	DriverName string
}
