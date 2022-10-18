package tt

import (
	"encoding/json"
	"testing"
)

func ToJSON(t *testing.T, obj interface{}) []byte {
	rawJSON, err := json.Marshal(obj)
	AssertNoErr(t, err)

	return rawJSON
}
