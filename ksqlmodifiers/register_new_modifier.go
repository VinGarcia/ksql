package ksqlmodifiers

// RegisterAttrModifier allow users to add custom modifiers on startup
// it is recommended to do this inside an init() function.
var RegisterAttrModifier func(key string, modifier AttrModifier)

// This method is set at startup by the `internal/modifiers` package.
// It was done that way in order to keep most of the implementation private
// while also avoiding cyclic dependencies.
