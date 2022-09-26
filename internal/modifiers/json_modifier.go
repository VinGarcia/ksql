package modifiers

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
)

// This modifier serializes objects as JSON when
// sending it to the database and decodes
// them when receiving.
var jsonModifier = AttrModifier{
	Scan: func(ctx context.Context, opInfo OpInfo, attrPtr interface{}, dbValue interface{}) error {
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
	},

	Value: func(ctx context.Context, opInfo OpInfo, inputValue interface{}) (outputValue interface{}, _ error) {
		b, err := json.Marshal(inputValue)
		if opInfo.DriverName == "sqlserver" {
			return string(b), err
		}
		return b, err
	},
}
