package slices

import "reflect"

// ToInterfaceSlicer describes objects that
// can be converted to a list of interfaces
type ToInterfaceSlicer interface {
	ToInterfaceSlice() []interface{}
}

// ToInterfaceSlice converts any slice into a slice of empty interfaces.
//
// If the input argument is not a slice it panics.
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
