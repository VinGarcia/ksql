package modifiers

import (
	"context"
	"time"
)

// This one is useful for updatedAt timestamps
var timeNowUTCModifier = AttrModifier{
	Value: func(ctx context.Context, opInfo OpInfo, inputValue interface{}) (outputValue interface{}, _ error) {
		return time.Now().UTC(), nil
	},
}

// This one is useful for createdAt timestamps
var timeNowUTCSkipUpdatesModifier = AttrModifier{
	SkipOnUpdate: true,

	Value: func(ctx context.Context, opInfo OpInfo, inputValue interface{}) (outputValue interface{}, _ error) {
		return time.Now().UTC(), nil
	},
}
