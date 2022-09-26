package modifiers

import (
	"context"
	"database/sql/driver"
)

// AttrScanWrapper is the wrapper that allow us to intercept the Scan process
// so we can run the modifiers instead of allowing the database driver to use
// its default behavior.
//
// For that this struct implements the `sql.Scanner` interface
type AttrScanWrapper struct {
	Ctx context.Context

	AttrPtr interface{}

	ScanFn AttrScanner
	OpInfo OpInfo
}

// Scan implements the sql.Scanner interface
func (a AttrScanWrapper) Scan(dbValue interface{}) error {
	return a.ScanFn(a.Ctx, a.OpInfo, a.AttrPtr, dbValue)
}

// AttrValueWrapper is the wrapper that allow us to intercept the "Valuing" process
// so we can run the modifiers instead of allowing the database driver to use
// its default behavior.
//
// For that this struct implements the `sql.Valuer` interface
type AttrValueWrapper struct {
	Ctx context.Context

	Attr interface{}

	ValueFn AttrValuer
	OpInfo  OpInfo
}

// Value implements the sql.Valuer interface
func (a AttrValueWrapper) Value() (driver.Value, error) {
	return a.ValueFn(a.Ctx, a.OpInfo, a.Attr)
}
