package structs

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
)

// StructInfo stores metainformation of the struct
// parser in order to help the ksql library to work
// efectively and efficiently with reflection.
type StructInfo struct {
	IsNestedStruct bool
	byIndex        map[int]*FieldInfo
	byName         map[string]*FieldInfo
}

// FieldInfo contains reflection and tags
// information regarding a specific field
// of a struct.
type FieldInfo struct {
	Name           string
	Index          int
	Valid          bool
	SerializerName string
}

// ByIndex returns either the *FieldInfo of a valid
// empty struct with Valid set to false
func (s StructInfo) ByIndex(idx int) *FieldInfo {
	field, found := s.byIndex[idx]
	if !found {
		return &FieldInfo{}
	}
	return field
}

// ByName returns either the *FieldInfo of a valid
// empty struct with Valid set to false
func (s StructInfo) ByName(name string) *FieldInfo {
	field, found := s.byName[name]
	if !found {
		return &FieldInfo{}
	}
	return field
}

func (s StructInfo) add(field FieldInfo) {
	field.Valid = true
	s.byIndex[field.Index] = &field
	s.byName[field.Name] = &field

	// Make sure to save a lowercased version because
	// some databases will set these keys to lowercase.
	if _, found := s.byName[strings.ToLower(field.Name)]; !found {
		s.byName[strings.ToLower(field.Name)] = &field
	}
}

// NumFields ...
func (s StructInfo) NumFields() int {
	return len(s.byIndex)
}

// This cache is kept as a pkg variable
// because the total number of types on a program
// should be finite. So keeping a single cache here
// works fine.
var tagInfoCache = &sync.Map{}

// GetTagInfo efficiently returns the type information
// using a global private cache
//
// In the future we might move this cache inside
// a struct, but for now this accessor is the one
// we are using
func GetTagInfo(key reflect.Type) (StructInfo, error) {
	return getCachedTagInfo(tagInfoCache, key)
}

func getCachedTagInfo(tagInfoCache *sync.Map, key reflect.Type) (StructInfo, error) {
	if data, found := tagInfoCache.Load(key); found {
		info, ok := data.(StructInfo)
		if !ok {
			return StructInfo{}, fmt.Errorf("invalid cache entry, expected type StructInfo, found %T", data)
		}
		return info, nil
	}

	info, err := getTagNames(key)
	if err != nil {
		return StructInfo{}, err
	}

	tagInfoCache.Store(key, info)
	return info, nil
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

	info, err := getCachedTagInfo(tagInfoCache, t)
	if err != nil {
		return nil, err
	}

	m := map[string]interface{}{}
	for i := 0; i < v.NumField(); i++ {
		fieldInfo := info.ByIndex(i)
		if !fieldInfo.Valid {
			continue
		}

		field := v.Field(i)
		ft := field.Type()
		if ft.Kind() == reflect.Ptr {
			if field.IsNil() {
				continue
			}

			field = field.Elem()
		}

		m[fieldInfo.Name] = field.Interface()
	}

	return m, nil
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

// This function collects only the names
// that will be used from the input type.
//
// This should save several calls to `Field(i).Tag.Get("foo")`
// which improves performance by a lot.
func getTagNames(t reflect.Type) (StructInfo, error) {
	info := StructInfo{
		byIndex: map[int]*FieldInfo{},
		byName:  map[string]*FieldInfo{},
	}
	for i := 0; i < t.NumField(); i++ {
		// If this field is private:
		if t.Field(i).PkgPath != "" {
			return StructInfo{}, fmt.Errorf("all fields using the ksql tags must be exported, but %v is unexported", t)
		}

		name := t.Field(i).Tag.Get("ksql")
		if name == "" {
			continue
		}

		tags := strings.Split(name, ",")
		var serializerName string
		if len(tags) > 1 {
			name = tags[0]
			serializerName = tags[1]
		}

		if _, found := info.byName[name]; found {
			return StructInfo{}, fmt.Errorf(
				"struct contains multiple attributes with the same ksql tag name: '%s'",
				name,
			)
		}

		info.add(FieldInfo{
			Name:           name,
			Index:          i,
			SerializerName: serializerName,
		})
	}

	// If there were `ksql` tags present, then we are finished:
	if len(info.byIndex) > 0 {
		return info, nil
	}

	// If there are no `ksql` tags in the struct, lets assume
	// it is a struct tagged with `tablename` for allowing JOINs
	for i := 0; i < t.NumField(); i++ {
		name := t.Field(i).Tag.Get("tablename")
		if name == "" {
			continue
		}

		info.add(FieldInfo{
			Name:  name,
			Index: i,
		})
	}

	if len(info.byIndex) == 0 {
		return StructInfo{}, fmt.Errorf("the struct must contain at least one attribute with the ksql tag")
	}

	info.IsNestedStruct = true

	return info, nil
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
