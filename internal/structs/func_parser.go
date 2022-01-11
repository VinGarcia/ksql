package structs

import (
	"fmt"
	"reflect"
)

var errType = reflect.TypeOf(new(error)).Elem()

// ParseInputFunc is used exclusively for parsing
// the ForEachChunk function used on the QueryChunks method.
func ParseInputFunc(fn interface{}) (reflect.Type, error) {
	if fn == nil {
		return nil, fmt.Errorf("the ForEachChunk attribute is required and cannot be nil")
	}

	t := reflect.TypeOf(fn)

	if t.Kind() != reflect.Func {
		return nil, fmt.Errorf("the ForEachChunk callback must be a function")
	}
	if t.NumIn() != 1 {
		return nil, fmt.Errorf("the ForEachChunk callback must have 1 argument")
	}

	if t.NumOut() != 1 {
		return nil, fmt.Errorf("the ForEachChunk callback must have a single return value")
	}

	if t.Out(0) != errType {
		return nil, fmt.Errorf("the return value of the ForEachChunk callback must be of type error")
	}

	argsType := t.In(0)
	if argsType.Kind() != reflect.Slice {
		return nil, fmt.Errorf("the argument of the ForEachChunk callback must a slice of structs")
	}

	if argsType.Elem().Kind() != reflect.Struct {
		return nil, fmt.Errorf("the argument of the ForEachChunk callback must a slice of structs")
	}

	return argsType, nil
}
