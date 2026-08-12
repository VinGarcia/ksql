package modifiers

import (
	"context"
	"database/sql/driver"

	"github.com/vingarcia/ksql/ksqlmodifiers"
)

// AttrScanWrapper is the wrapper that allow us to intercept the Scan process
// so we can run the modifiers instead of allowing the database driver to use
// its default behavior.
//
// For that this struct implements the `sql.Scanner` interface
type AttrScanWrapper struct {
	Ctx context.Context

	AttrPtr interface{}

	ScanFn ksqlmodifiers.AttrScanner
	OpInfo ksqlmodifiers.OpInfo

	// TrackNull, when set, makes the wrapper record on WasNull whether the
	// value received from the database was NULL. It is used by the optional
	// joined struct feature to decide, after Scan, whether a nested pointer
	// struct with modifiers should be materialized or left nil.
	TrackNull bool

	// WasNull is written during Scan when TrackNull is set: it is true if
	// the received dbValue was nil (a SQL NULL).
	WasNull bool
}

// Scan implements the sql.Scanner interface
func (a *AttrScanWrapper) Scan(dbValue interface{}) error {
	if a.TrackNull {
		a.WasNull = dbValue == nil
	}
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

	ValueFn ksqlmodifiers.AttrValuer
	OpInfo  ksqlmodifiers.OpInfo
}

// Value implements the sql.Valuer interface
func (a AttrValueWrapper) Value() (driver.Value, error) {
	return a.ValueFn(a.Ctx, a.OpInfo, a.Attr)
}
