package modifiers

import (
	"fmt"
	"sync"

	"github.com/vingarcia/ksql/ksqlmodifiers"
)

// Here we keep all the registered modifiers
var modifiers sync.Map

func init() {
	// Here we expose the registration function in a public package,
	// so users can use it:
	ksqlmodifiers.RegisterAttrModifier = RegisterAttrModifier

	// These are the builtin modifiers:

	// This one is useful for serializing/deserializing structs:
	modifiers.Store("json", jsonModifier)
	modifiers.Store("json/nullable", jsonNullableModifier)

	// This next two are useful for the UpdatedAt and Created fields respectively:
	// They only work on time.Time attributes and will set the attribute to time.Now().
	modifiers.Store("timeNowUTC", timeNowUTCModifier)
	modifiers.Store("timeNowUTC/skipUpdates", timeNowUTCSkipUpdatesModifier)

	// These are mostly example modifiers and they are also used
	// to test the feature of skipping updates, inserts and queries.
	modifiers.Store("skipUpdates", skipUpdatesModifier)
	modifiers.Store("skipInserts", skipInsertsModifier)
	modifiers.Store("nullable", nullableModifier)
}

// RegisterAttrModifier allow users to add custom modifiers on startup
// it is recommended to do this inside an init() function.
func RegisterAttrModifier(key string, modifier ksqlmodifiers.AttrModifier) {
	_, found := modifiers.Load(key)
	if found {
		panic(fmt.Errorf("KSQL: cannot register modifier '%s' name is already in use", key))
	}

	modifiers.Store(key, modifier)
}

// LoadGlobalModifier is used internally by KSQL to load
// modifiers during runtime.
func LoadGlobalModifier(key string) (ksqlmodifiers.AttrModifier, error) {
	rawModifier, _ := modifiers.Load(key)
	modifier, ok := rawModifier.(ksqlmodifiers.AttrModifier)
	if !ok {
		return ksqlmodifiers.AttrModifier{}, fmt.Errorf("no modifier found with name '%s'", key)
	}

	return modifier, nil
}
