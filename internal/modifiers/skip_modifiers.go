package modifiers

import "github.com/vingarcia/ksql/kmodifiers"

var skipInsertsModifier = kmodifiers.AttrModifier{
	SkipOnInsert: true,
}

var skipUpdatesModifier = kmodifiers.AttrModifier{
	SkipOnUpdate: true,
}
