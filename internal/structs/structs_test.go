package structs

import (
	"reflect"
	"testing"

	tt "github.com/vingarcia/ksql/internal/testtools"
)

func TestGetTagInfo(t *testing.T) {
	tests := []struct {
		desc                string
		obj                 interface{}
		expectedInfo        StructInfo
		expecteErrToContain []string
	}{
		{
			desc: "should correctly parse all the fields tagged `ksql` on a struct",
			obj: struct {
				ShowStr string `ksql:"show_str"`
				HideStr string
				ShowInt int `ksql:"show_int"`
				HideInt int
			}{},
			expectedInfo: StructInfo{
				IsNestedStruct: false,
				byIndex: map[int]*FieldInfo{
					0: &FieldInfo{
						AttrName:   "ShowStr",
						ColumnName: "show_str",
						Index:      0,
						Valid:      true,
					},
					2: &FieldInfo{
						AttrName:   "ShowInt",
						ColumnName: "show_int",
						Index:      2,
						Valid:      true,
					},
				},
				byName: map[string]*FieldInfo{
					"show_str": &FieldInfo{
						AttrName:   "ShowStr",
						ColumnName: "show_str",
						Index:      0,
						Valid:      true,
					},
					"show_int": &FieldInfo{
						AttrName:   "ShowInt",
						ColumnName: "show_int",
						Index:      2,
						Valid:      true,
					},
				},
				numFields: 4,
			},
		},
		{
			desc: "should correctly ignore private fields if they are not tagged",
			obj: struct {
				ShowStr string `ksql:"show_str"`
				hideStr string
				ShowInt int `ksql:"show_int"`
				hideInt int
			}{},
			expectedInfo: StructInfo{
				IsNestedStruct: false,
				byIndex: map[int]*FieldInfo{
					0: &FieldInfo{
						AttrName:   "ShowStr",
						ColumnName: "show_str",
						Index:      0,
						Valid:      true,
					},
					2: &FieldInfo{
						AttrName:   "ShowInt",
						ColumnName: "show_int",
						Index:      2,
						Valid:      true,
					},
				},
				byName: map[string]*FieldInfo{
					"show_str": &FieldInfo{
						AttrName:   "ShowStr",
						ColumnName: "show_str",
						Index:      0,
						Valid:      true,
					},
					"show_int": &FieldInfo{
						AttrName:   "ShowInt",
						ColumnName: "show_int",
						Index:      2,
						Valid:      true,
					},
				},
				numFields: 4,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			info, err := GetTagInfo(reflect.TypeOf(test.obj))
			if test.expecteErrToContain != nil {
				tt.AssertErrContains(t, err, test.expecteErrToContain...)
				t.Skip()
			}
			tt.AssertNoErr(t, err)

			tt.AssertEqual(t, info, test.expectedInfo)
			tt.AssertEqual(t, info.NumFields(), test.expectedInfo.numFields)
		})
	}
}
