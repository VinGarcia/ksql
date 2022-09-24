package modifiers

import (
	"context"
	"errors"
	"testing"

	tt "github.com/vingarcia/ksql/internal/testtools"
)

func TestAttrWrapper(t *testing.T) {
	ctx := context.Background()

	var scanArgs map[string]interface{}
	var valueArgs map[string]interface{}
	wrapper := AttrWrapper{
		Ctx:  ctx,
		Attr: "fakeAttr",
		Modifier: AttrModifierMock{
			AttrScanFn: func(ctx context.Context, opInfo OpInfo, attrPtr interface{}, dbValue interface{}) error {
				scanArgs = map[string]interface{}{
					"opInfo":  opInfo,
					"attrPtr": attrPtr,
					"dbValue": dbValue,
				}
				return errors.New("fakeScanErrMsg")
			},
			AttrValueFn: func(ctx context.Context, opInfo OpInfo, inputValue interface{}) (outputValue interface{}, _ error) {
				valueArgs = map[string]interface{}{
					"opInfo":     opInfo,
					"inputValue": inputValue,
				}
				return "fakeOutputValue", errors.New("fakeValueErrMsg")
			},
		},
		OpInfo: OpInfo{
			Method:     "fakeMethod",
			DriverName: "fakeDriverName",
		},
	}

	err := wrapper.Scan("fakeDbValue")
	tt.AssertErrContains(t, err, "fakeScanErrMsg")
	tt.AssertEqual(t, scanArgs, map[string]interface{}{
		"opInfo": OpInfo{
			Method:     "fakeMethod",
			DriverName: "fakeDriverName",
		},
		"attrPtr": "fakeAttr",
		"dbValue": "fakeDbValue",
	})

	value, err := wrapper.Value()
	tt.AssertErrContains(t, err, "fakeValueErrMsg")
	tt.AssertEqual(t, valueArgs, map[string]interface{}{
		"opInfo": OpInfo{
			Method:     "fakeMethod",
			DriverName: "fakeDriverName",
		},
		"inputValue": "fakeAttr",
	})
	tt.AssertEqual(t, value, "fakeOutputValue")
}
