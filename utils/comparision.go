package utils

import (
	"reflect"
	"strings"
)

func Compare(a, b interface{}) int {

	switch a.(type) {
	case bool:
		if a.(bool) {
			return 1
		} else {
			return -1
		}
	case int:
		return a.(int) - b.(int)
	case int32:
		return (int)(a.(int32) - b.(int32))
	case int64:
		return (int)(a.(int64) - b.(int64))
	case string:
		return strings.Compare(a.(string), b.(string))
	case float64:
		sub := a.(float64) - b.(float64)
		if sub > 0.0 {
			return 1
		} else if sub < 0.0 {
			return -1
		}
		return 0
	case float32:
		sub := a.(float32) - b.(float32)
		if sub > 0.0 {
			return 1
		} else if sub < 0.0 {
			return -1
		}
		return 0
	}
	panic("Unsupported Index value type:" + reflect.TypeOf(a).String())
}
