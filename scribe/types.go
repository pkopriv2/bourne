package scribe

import (
	"encoding/gob"

	"github.com/pkg/errors"
)

// Build a small type system over the standard go system.  The intent is to
// severely limit the number of encodable values so that we can maintain
// compatibility across various encoding schemes.
type Value interface {

	// Assigns the value to the value pointed to by ptr.   Values are able
	// to make decisions as to what ptr values it supports.
	AssignTo(ptr interface{}) error

	// Dumps the value to a raw go type
	Dump() interface{}
}

// A boolean value
type Bool bool

func (b Bool) AssignTo(raw interface{}) error {
	switch ptr := raw.(type) {
	default:
		return NewIncompatibleTypeError(b, ptr)
	case *bool:
		*ptr = bool(b)
	}

	return nil
}

func (b Bool) Dump() interface{} {
	return bool(b)
}

// a string value
type String string

func (s String) AssignTo(raw interface{}) error {
	switch ptr := raw.(type) {
	default:
		return NewIncompatibleTypeError(s, ptr)
	case *string:
		*ptr = string(s)
	}

	return nil
}

func (s String) Dump() interface{} {
	return string(s)
}

// an array value (composed of other values)
type Array []Value

func (a Array) AssignTo(ptr interface{}) error {
	switch ptr := ptr.(type) {
	default:
		return NewUnsupportedTypeError(ptr)
	case *[]Object:
		arr := make([]Object, len(a))
		for i := 0; i < len(a); i++ {
			a[i].AssignTo(&arr[i])
		}
		*ptr = arr
	case *[]string:
		arr := make([]string, len(a))
		for i := 0; i < len(a); i++ {
			a[i].AssignTo(&arr[i])
		}
		*ptr = arr
	case *[]bool:
		arr := make([]bool, len(a))
		for i := 0; i < len(a); i++ {
			a[i].AssignTo(&arr[i])
		}
		*ptr = arr
	}

	return nil
}

func (a Array) Dump() interface{} {
	arr := make([]interface{}, len(a))
	gob.Register(arr) // grr.... what does this screw up later?
	for i := 0; i < len(a); i++ {
		arr[i] = a[i].Dump()
	}
	return arr
}

func parseArray(arr []interface{}) (Array, error) {
	ret := make([]Value, 0, len(arr))
	for _, cur := range arr {
		val, err := parseValue(cur)
		if err != nil {
			return nil, err
		}

		ret = append(ret, val)
	}

	return Array(ret), nil
}

// a generic field indexed value
type Object map[string]Value

func (o Object) AssignTo(ptr interface{}) error {
	switch ptr := ptr.(type) {
	default:
		return NewUnsupportedTypeError(ptr)
	case *Object:
		*ptr = o
	}

	return nil
}

func (o Object) Read(field string, ptr interface{}) error {
	value, ok := o[field]
	if !ok {
		return &MissingFieldError{field}
	}

	if err := value.AssignTo(ptr); err != nil {
		return errors.Wrapf(err, "Error while reading field [%v]", field)
	}

	return nil
}

func (o Object) ReadOptional(field string, ptr interface{}) (bool, error) {
	value, ok := o[field]
	if !ok {
		return false, nil
	}

	if err := value.AssignTo(ptr); err != nil {
		return false, errors.Wrapf(err, "Error while reading field [%v]", field)
	}

	return true, nil
}

func (o Object) Write(w Writer) {
	for k, v := range o {
		w.Write(k, v)
	}
}

func (o Object) Copy() Object {
	ret := make(map[string]Value)
	for k, v := range o {
		ret[k] = v
	}

	return Object(ret)
}

func (o Object) Dump() interface{} {
	ret := make(map[string]interface{})
	gob.Register(ret) // grr.... what does this screw up later?
	for k, v := range o {
		ret[k] = v.Dump()
	}

	return ret
}

func newEmptyObject() Object {
	return Object(make(map[string]Value))
}

func parseObject(data map[string]interface{}) (Object, error) {
	obj := make(map[string]Value)
	for k, v := range data {
		val, err := parseValue(v)
		if err != nil {
			return nil, err
		}

		obj[k] = val
	}

	return Object(obj), nil
}

// parses the dumped format of value
func parseValue(data interface{}) (Value, error) {
	switch val := data.(type) {
	default:
		return nil, NewUnsupportedTypeError(val)
	case bool:
		return Bool(val), nil
	case string:
		return String(val), nil
	case []interface{}:
		return parseArray(val)
	case map[string]interface{}:
		return parseObject(val)
	}
}
