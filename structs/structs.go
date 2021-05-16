package structs

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/pkg/errors"
)

type structInfo struct {
	byIndex map[int]*fieldInfo
	byName  map[string]*fieldInfo
}

type fieldInfo struct {
	Name            string
	Index           int
	Valid           bool
	SerializeAsJSON bool
}

func (s structInfo) ByIndex(idx int) *fieldInfo {
	field, found := s.byIndex[idx]
	if !found {
		return &fieldInfo{}
	}
	return field
}

func (s structInfo) ByName(name string) *fieldInfo {
	field, found := s.byName[name]
	if !found {
		return &fieldInfo{}
	}
	return field
}

func (s structInfo) Add(field fieldInfo) {
	field.Valid = true
	s.byIndex[field.Index] = &field
	s.byName[field.Name] = &field
}

func (s structInfo) Fields() map[int]*fieldInfo {
	return s.byIndex
}

// This cache is kept as a pkg variable
// because the total number of types on a program
// should be finite. So keeping a single cache here
// works fine.
var tagInfoCache = map[reflect.Type]structInfo{}

// GetTagInfo efficiently returns the type information
// using a global private cache
//
// In the future we might move this cache inside
// a struct, but for now this accessor is the one
// we are using
func GetTagInfo(key reflect.Type) structInfo {
	return getCachedTagInfo(tagInfoCache, key)
}

func getCachedTagInfo(tagInfoCache map[reflect.Type]structInfo, key reflect.Type) structInfo {
	info, found := tagInfoCache[key]
	if !found {
		info = getTagNames(key)
		tagInfoCache[key] = info
	}
	return info
}

// StructToMap converts any struct type to a map based on
// the tag named `ksql`, i.e. `ksql:"map_key_name"`
//
// Valid pointers are dereferenced and copied to the map,
// null pointers are ignored.
//
// This function is efficient in the fact that it caches
// the slower steps of the reflection required to perform
// this task.
func StructToMap(obj interface{}) (map[string]interface{}, error) {
	v := reflect.ValueOf(obj)
	t := v.Type()

	if t.Kind() == reflect.Ptr {
		v = v.Elem()
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("input must be a struct or struct pointer")
	}

	info := getCachedTagInfo(tagInfoCache, t)

	m := map[string]interface{}{}
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		ft := field.Type()
		if ft.Kind() == reflect.Ptr {
			if field.IsNil() {
				continue
			}

			field = field.Elem()
		}

		m[info.ByIndex(i).Name] = field.Interface()
	}

	return m, nil
}

// FillStructWith is meant to be used on unit tests to mock
// the response from the database.
//
// The first argument is any struct you are passing to a ksql func,
// and the second is a map representing a database row you want
// to use to update this struct.
func FillStructWith(record interface{}, dbRow map[string]interface{}) error {
	v := reflect.ValueOf(record)
	t := v.Type()

	if t.Kind() != reflect.Ptr {
		return fmt.Errorf(
			"FillStructWith: expected input to be a pointer to struct but got %T",
			record,
		)
	}

	t = t.Elem()
	v = v.Elem()

	if t.Kind() != reflect.Struct {
		return fmt.Errorf(
			"FillStructWith: expected input kind to be a struct but got %T",
			record,
		)
	}

	info := getCachedTagInfo(tagInfoCache, t)
	for colName, rawSrc := range dbRow {
		fieldInfo := info.ByName(colName)
		if !fieldInfo.Valid {
			// Ignore columns not tagged with `ksql:"..."`
			continue
		}

		src := NewPtrConverter(rawSrc)
		dest := v.Field(fieldInfo.Index)
		destType := t.Field(fieldInfo.Index).Type

		destValue, err := src.Convert(destType)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("FillStructWith: error on field `%s`", colName))
		}

		dest.Set(destValue)
	}

	return nil
}

// PtrConverter was created to make it easier
// to handle conversion between ptr and non ptr types, e.g.:
//
// - *type to *type
// - type to *type
// - *type to type
// - type to type
type PtrConverter struct {
	BaseType  reflect.Type
	BaseValue reflect.Value
	ElemType  reflect.Type
	ElemValue reflect.Value
}

// NewPtrConverter instantiates a PtrConverter from
// an empty interface.
//
// The input argument can be of any type, but
// if it is a pointer then its Elem() will be
// used as source value for the PtrConverter.Convert()
// method.
func NewPtrConverter(v interface{}) PtrConverter {
	if v == nil {
		// This is necessary so that reflect.ValueOf
		// returns a valid reflect.Value
		v = (*interface{})(nil)
	}

	baseValue := reflect.ValueOf(v)
	baseType := reflect.TypeOf(v)

	elemType := baseType
	elemValue := baseValue
	if baseType.Kind() == reflect.Ptr {
		elemType = elemType.Elem()
		elemValue = elemValue.Elem()
	}
	return PtrConverter{
		BaseType:  baseType,
		BaseValue: baseValue,
		ElemType:  elemType,
		ElemValue: elemValue,
	}
}

// Convert attempts to convert the ElemValue to the destType received
// as argument and then returns the converted reflect.Value or an error
func (p PtrConverter) Convert(destType reflect.Type) (reflect.Value, error) {
	destElemType := destType
	if destType.Kind() == reflect.Ptr {
		destElemType = destType.Elem()
	}

	// Return 0 valued destType instance:
	if p.BaseType.Kind() == reflect.Ptr && p.BaseValue.IsNil() {
		// Note that if destType is a ptr it will return a nil ptr.
		return reflect.New(destType).Elem(), nil
	}

	if !p.ElemType.ConvertibleTo(destElemType) {
		return reflect.Value{}, fmt.Errorf(
			"cannot convert from type %v to type %v", p.BaseType, destType,
		)
	}

	destValue := p.ElemValue.Convert(destElemType)

	// Get the address of destValue if necessary:
	if destType.Kind() == reflect.Ptr {
		if !destValue.CanAddr() {
			tmp := reflect.New(destElemType)
			tmp.Elem().Set(destValue)
			destValue = tmp
		} else {
			destValue = destValue.Addr()
		}
	}

	return destValue, nil
}

// FillSliceWith is meant to be used on unit tests to mock
// the response from the database.
//
// The first argument is any slice of structs you are passing to a ksql func,
// and the second is a slice of maps representing the database rows you want
// to use to update this struct.
func FillSliceWith(entities interface{}, dbRows []map[string]interface{}) error {
	sliceRef := reflect.ValueOf(entities)
	sliceType := sliceRef.Type()
	if sliceType.Kind() != reflect.Ptr {
		return fmt.Errorf(
			"FillSliceWith: expected input to be a pointer to struct but got %v",
			sliceType,
		)
	}

	structType, isSliceOfPtrs, err := DecodeAsSliceOfStructs(sliceType.Elem())
	if err != nil {
		return errors.Wrap(err, "FillSliceWith")
	}

	slice := sliceRef.Elem()
	for idx, row := range dbRows {
		if slice.Len() <= idx {
			var elemValue reflect.Value
			elemValue = reflect.New(structType)
			if !isSliceOfPtrs {
				elemValue = elemValue.Elem()
			}
			slice = reflect.Append(slice, elemValue)
		}

		err := FillStructWith(slice.Index(idx).Addr().Interface(), row)
		if err != nil {
			return errors.Wrap(err, "FillSliceWith")
		}
	}

	sliceRef.Elem().Set(slice)

	return nil
}

// This function collects only the names
// that will be used from the input type.
//
// This should save several calls to `Field(i).Tag.Get("foo")`
// which improves performance by a lot.
func getTagNames(t reflect.Type) structInfo {
	info := structInfo{
		byIndex: map[int]*fieldInfo{},
		byName:  map[string]*fieldInfo{},
	}
	for i := 0; i < t.NumField(); i++ {
		name := t.Field(i).Tag.Get("ksql")
		if name == "" {
			continue
		}

		tags := strings.Split(name, ",")
		serializeAsJSON := false
		if len(tags) > 1 {
			name = tags[0]
			serializeAsJSON = tags[1] == "json"
		}

		info.Add(fieldInfo{
			Name:            name,
			Index:           i,
			SerializeAsJSON: serializeAsJSON,
		})
	}

	return info
}

// DecodeAsSliceOfStructs makes several checks
// while decoding an input type and returns
// useful information so that it is easier
// to manipulate the original slice later.
func DecodeAsSliceOfStructs(slice reflect.Type) (
	structType reflect.Type,
	isSliceOfPtrs bool,
	err error,
) {
	if slice.Kind() != reflect.Slice {
		err = fmt.Errorf(
			"expected input kind to be a slice but got %v",
			slice,
		)
		return
	}

	elemType := slice.Elem()
	isPtr := elemType.Kind() == reflect.Ptr

	if isPtr {
		elemType = elemType.Elem()
	}

	if elemType.Kind() != reflect.Struct {
		err = fmt.Errorf(
			"expected input to be a slice of structs but got %v",
			slice,
		)
		return
	}

	return elemType, isPtr, nil
}
