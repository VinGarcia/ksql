package kisssql

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"reflect"
)

// This type was created to make it easier to adapt
// input attributes to be convertible to and from JSON
// before sending or receiving it from the database.
type jsonSerializable struct {
	Attr interface{}
}

// Scan Implements the Scanner interface in order to load
// this field from the JSON stored in the database
func (j *jsonSerializable) Scan(value interface{}) error {
	if value == nil {
		v := reflect.ValueOf(j.Attr)
		// Set the struct to its 0 value just like json.Unmarshal
		// does for nil attributes:
		v.Elem().Set(reflect.Zero(reflect.TypeOf(j.Attr).Elem()))
		return nil
	}

	// Required since sqlite3 returns strings not bytes
	if v, ok := value.(string); ok {
		value = []byte(v)
	}

	rawJSON, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("unexpected type received to Scan: %T", value)
	}
	return json.Unmarshal(rawJSON, j.Attr)
}

// Value Implements the Valuer interface in order to save
// this field as JSON on the database.
func (j jsonSerializable) Value() (driver.Value, error) {
	return json.Marshal(j.Attr)
}
