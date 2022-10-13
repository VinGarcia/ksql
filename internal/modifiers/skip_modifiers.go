package modifiers

var skipInsertsModifier = AttrModifier{
	SkipOnInsert: true,
}

var skipUpdatesModifier = AttrModifier{
	SkipOnUpdate: true,
}

var skipQueriesModifier = AttrModifier{
	SkipOnQuery: true,
}
