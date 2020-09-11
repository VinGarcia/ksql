package slices

import "reflect"

type ToInterfaceSlicer interface {
	ToInterfaceSlice() []interface{}
}

func ToInterfaceSlice(slice interface{}) (resp []interface{}) {
	if iSlicer, ok := slice.(ToInterfaceSlicer); ok {
		return iSlicer.ToInterfaceSlice()
	}

	v := reflect.ValueOf(slice)
	t := v.Type()
	if t.Kind() != reflect.Slice {
		panic("ToInterfaceSlice function only works with a slice as argument")
	}

	if t.Kind() != reflect.Slice {
		panic("type must be a slice!")
	}

	for i := 0; i < v.Len(); i++ {
		resp = append(resp, v.Index(i).Interface())
	}

	return resp
}
