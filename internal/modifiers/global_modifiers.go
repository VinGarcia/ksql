package modifiers

import (
	"fmt"
	"sync"
)

// Here we keep all the registered modifiers
var modifiers sync.Map

func init() {
	// These are the builtin modifiers
	modifiers.Store("json", jsonModifier{})
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
		return nil, fmt.Errorf("no modifier found with name '%s'", key)
	}

	return modifier, nil
}
