package modifiers

import (
	"context"
	"time"

	"github.com/vingarcia/ksql/kmodifiers"
)

// This one is useful for updatedAt timestamps
var timeNowUTCModifier = kmodifiers.AttrModifier{
	Value: func(ctx context.Context, opInfo kmodifiers.OpInfo, inputValue interface{}) (outputValue interface{}, _ error) {
		return time.Now().UTC(), nil
	},
}

// This one is useful for createdAt timestamps
var timeNowUTCSkipUpdatesModifier = kmodifiers.AttrModifier{
	SkipOnUpdate: true,

	Value: func(ctx context.Context, opInfo kmodifiers.OpInfo, inputValue interface{}) (outputValue interface{}, _ error) {
		return time.Now().UTC(), nil
	},
}
