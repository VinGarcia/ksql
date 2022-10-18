package modifiers

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/vingarcia/ksql/kmodifiers"
)

// This modifier serializes objects as JSON when
// sending it to the database and decodes
// them when receiving.
var jsonModifier = kmodifiers.AttrModifier{
	Scan: func(ctx context.Context, opInfo kmodifiers.OpInfo, attrPtr interface{}, dbValue interface{}) error {
		if dbValue == nil {
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

	Value: func(ctx context.Context, opInfo kmodifiers.OpInfo, inputValue interface{}) (outputValue interface{}, _ error) {
		b, err := json.Marshal(inputValue)
		// SQL server uses the NVARCHAR type to store JSON and
		// it expects to receive strings not []byte, thus:
		if opInfo.DriverName == "sqlserver" {
			return string(b), err
		}
		return b, err
	},
}
