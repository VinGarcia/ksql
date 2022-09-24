package modifiers

import (
	"context"
	"database/sql/driver"
)

// AttrWrapper is the wrapper that allow us to intercept the Scan and Value processes
// so we can run the modifiers instead of allowing the database driver to use
// its default behavior.
//
// For that this struct implements both the `sql.Scanner` and `sql.Valuer` interfaces.
type AttrWrapper struct {
	Ctx context.Context

	// When Scanning this value should be a pointer to the attribute
	// and when "Valuing" it should just be the actual value
	Attr interface{}

	Modifier AttrModifier
	OpInfo   OpInfo
}

// Scan implements the sql.Scanner interface
func (a AttrWrapper) Scan(dbValue interface{}) error {
	return a.Modifier.AttrScan(a.Ctx, a.OpInfo, a.Attr, dbValue)
}

// Value implements the sql.Valuer interface
func (a AttrWrapper) Value() (driver.Value, error) {
	return a.Modifier.AttrValue(a.Ctx, a.OpInfo, a.Attr)
}
