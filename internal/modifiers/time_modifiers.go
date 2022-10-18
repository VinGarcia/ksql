package modifiers

import (
	"context"
	"time"

	"github.com/vingarcia/ksql/ksqlmodifiers"
)

// This one is useful for updatedAt timestamps
var timeNowUTCModifier = ksqlmodifiers.AttrModifier{
	Value: func(ctx context.Context, opInfo ksqlmodifiers.OpInfo, inputValue interface{}) (outputValue interface{}, _ error) {
		return time.Now().UTC(), nil
	},
}

// This one is useful for createdAt timestamps
var timeNowUTCSkipUpdatesModifier = ksqlmodifiers.AttrModifier{
	SkipOnUpdate: true,

	Value: func(ctx context.Context, opInfo ksqlmodifiers.OpInfo, inputValue interface{}) (outputValue interface{}, _ error) {
		return time.Now().UTC(), nil
	},
}
