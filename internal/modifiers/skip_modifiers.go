package modifiers

import "github.com/vingarcia/ksql/ksqlmodifiers"

var skipInsertsModifier = ksqlmodifiers.AttrModifier{
	SkipOnInsert: true,
}

var skipUpdatesModifier = ksqlmodifiers.AttrModifier{
	SkipOnUpdate: true,
}
