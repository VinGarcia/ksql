package modifiers

import (
	"fmt"
	"sync"
)

// Here we keep all the registered modifiers
var modifiers sync.Map

func init() {
	// These are the builtin modifiers:

	// This one is useful for serializing/desserializing structs:
	modifiers.Store("json", jsonModifier)

	// This next two are useful for the UpdatedAt and Created fields respectively:
	// They only work on time.Time attributes and will set the attribute to time.Now().
	modifiers.Store("timeNowUTC", timeNowUTCModifier)
	modifiers.Store("timeNowUTC/skipUpdates", timeNowUTCSkipUpdatesModifier)

	// These are mostly example modifiers and they are also used
	// to test the feature of skipping updates, inserts and queries.
	modifiers.Store("skipUpdates", skipUpdatesModifier)
	modifiers.Store("skipInserts", skipInsertsModifier)
}

// RegisterAttrModifier allow users to add custom modifiers on startup
// it is recommended to do this inside an init() function.
func RegisterAttrModifier(key string, modifier AttrModifier) {
	_, found := modifiers.Load(key)
	if found {
		panic(fmt.Errorf("KSQL: cannot register modifier '%s' name is already in use", key))
	}

	modifiers.Store(key, modifier)
}

func LoadGlobalModifier(key string) (AttrModifier, error) {
	rawModifier, _ := modifiers.Load(key)
	modifier, ok := rawModifier.(AttrModifier)
	if !ok {
		return AttrModifier{}, fmt.Errorf("no modifier found with name '%s'", key)
	}

	return modifier, nil
}
