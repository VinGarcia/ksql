package modifiers

import (
	"context"
	"testing"

	tt "github.com/vingarcia/ksql/internal/testtools"
)

func TestAttrScan(t *testing.T) {
	ctx := context.Background()

	type FakeAttr struct {
		Foo string `json:"foo"`
	}

	tests := []struct {
		desc               string
		dbInput            interface{}
		expectedValue      interface{}
		expectErrToContain []string
	}{
		{
			desc:          "should set struct to zero value if input is nil",
			dbInput:       nil,
			expectedValue: FakeAttr{},
		},
		{
			desc:    "should work when input is a byte slice",
			dbInput: []byte(`{"foo":"bar"}`),
			expectedValue: FakeAttr{
				Foo: "bar",
			},
		},
		{
			desc:    "should work when input is a string",
			dbInput: `{"foo":"bar"}`,
			expectedValue: FakeAttr{
				Foo: "bar",
			},
		},
		{
			desc:               "should report error if input type is unsupported",
			dbInput:            10,
			expectErrToContain: []string{"unexpected type", "int"},
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			fakeAttr := FakeAttr{
				Foo: "notZeroValue",
			}
			err := jsonModifier.Scan(ctx, OpInfo{}, &fakeAttr, test.dbInput)
			if test.expectErrToContain != nil {
				tt.AssertErrContains(t, err, test.expectErrToContain...)
				t.Skip()
			}

			tt.AssertNoErr(t, err)
			tt.AssertEqual(t, fakeAttr, test.expectedValue)
		})
	}
}

func TestAttrValue(t *testing.T) {
	ctx := context.Background()

	type FakeAttr struct {
		Foo string `json:"foo"`
	}

	tests := []struct {
		desc        string
		dbInput     interface{}
		opInfoInput OpInfo
		attrValue   interface{}

		expectedOutput     interface{}
		expectErrToContain []string
	}{
		{
			desc:    "should return a byte array when the driver is not sqlserver",
			dbInput: []byte(`{"foo":"bar"}`),
			opInfoInput: OpInfo{
				DriverName: "notSQLServer",
			},
			attrValue: FakeAttr{
				Foo: "bar",
			},
			expectedOutput: tt.ToJSON(t, map[string]interface{}{
				"foo": "bar",
			}),
		},
		{
			desc:    "should return a string when the driver is sqlserver",
			dbInput: []byte(`{"foo":"bar"}`),
			opInfoInput: OpInfo{
				DriverName: "sqlserver",
			},
			attrValue: FakeAttr{
				Foo: "bar",
			},
			expectedOutput: string(tt.ToJSON(t, map[string]interface{}{
				"foo": "bar",
			})),
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			output, err := jsonModifier.Value(ctx, test.opInfoInput, test.attrValue)
			if test.expectErrToContain != nil {
				tt.AssertErrContains(t, err, test.expectErrToContain...)
				t.Skip()
			}

			tt.AssertNoErr(t, err)
			tt.AssertEqual(t, output, test.expectedOutput)
		})
	}
}
