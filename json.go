package ksql

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
)

// This type was created to make it easier to adapt
// input attributes to be convertible to and from JSON
// before sending or receiving it from the database.
type jsonSerializer struct{}

// Scan Implements the Scanner interface in order to load
// this field from the JSON stored in the database
func (j jsonSerializer) AttrScan(ctx context.Context, opInfo OpInfo, attrPtr interface{}, dbValue interface{}) error {
	if dbValue == nil {
		v := reflect.ValueOf(attrPtr)
		// Set the struct to its 0 value just like json.Unmarshal
		// does for nil attributes:
		v.Elem().Set(reflect.Zero(reflect.TypeOf(attrPtr).Elem()))
		return nil
	}

	// Required since sqlite3 returns strings not bytes
	if v, ok := dbValue.(string); ok {
		dbValue = []byte(v)
	}

	rawJSON, ok := dbValue.([]byte)
	if !ok {
		return fmt.Errorf("unexpected type received to Scan: %T", dbValue)
	}
	return json.Unmarshal(rawJSON, attrPtr)
}

// Value Implements the Valuer interface in order to save
// this field as JSON on the database.
func (j jsonSerializer) AttrValue(ctx context.Context, opInfo OpInfo, inputValue interface{}) (outputValue interface{}, _ error) {
	b, err := json.Marshal(inputValue)
	if opInfo.DriverName == "sqlserver" {
		return string(b), err
	}
	return b, err
}
