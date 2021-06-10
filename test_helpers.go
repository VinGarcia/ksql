package ksql

import (
	"reflect"

	"github.com/vingarcia/ksql/structs"
)

// CallFunctionWithRows was created for helping test the QueryChunks method
func CallFunctionWithRows(fn interface{}, rows []map[string]interface{}) error {
	fnValue := reflect.ValueOf(fn)
	chunkType, err := parseInputFunc(fn)
	if err != nil {
		return err
	}

	chunk := reflect.MakeSlice(chunkType, 0, len(rows))

	// Create a pointer to a slice (required by FillSliceWith)
	chunkPtr := reflect.New(chunkType)
	chunkPtr.Elem().Set(chunk)

	err = structs.FillSliceWith(chunkPtr.Interface(), rows)
	if err != nil {
		return err
	}

	err, _ = fnValue.Call([]reflect.Value{chunkPtr.Elem()})[0].Interface().(error)

	return err
}
