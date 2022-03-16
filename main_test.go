package ksql

import (
	"encoding/json"
	"fmt"
)

func toJSON(i interface{}) []byte {
	rawJSON, err := json.Marshal(i)
	if err != nil {
		panic(fmt.Sprintf("error marshalling %v during test", i))
	}
	return rawJSON
}
