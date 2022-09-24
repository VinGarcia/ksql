package modifiers

import "context"

// AttrModifierMock mocks the modifiers.AttrModifier interface
type AttrModifierMock struct {
	AttrScanFn func(
		ctx context.Context,
		opInfo OpInfo,
		attrPtr interface{},
		dbValue interface{},
	) error

	AttrValueFn func(
		ctx context.Context,
		opInfo OpInfo,
		inputValue interface{},
	) (outputValue interface{}, _ error)
}

// AttrScan mocks the AttrScan method
func (a AttrModifierMock) AttrScan(
	ctx context.Context,
	opInfo OpInfo,
	attrPtr interface{},
	dbValue interface{},
) error {
	return a.AttrScanFn(ctx, opInfo, attrPtr, dbValue)
}

// AttrValue mocks the AttrValue method
func (a AttrModifierMock) AttrValue(
	ctx context.Context,
	opInfo OpInfo,
	inputValue interface{},
) (outputValue interface{}, _ error) {
	return a.AttrValueFn(ctx, opInfo, inputValue)
}
