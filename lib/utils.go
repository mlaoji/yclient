package lib

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func PreParams(params map[string]string, obj interface{}) (data map[string]string, err error) { // {{{
	data = make(map[string]string)
	if nil != params {
		data = params
	}

	objVal := reflect.ValueOf(obj)

	if objVal.Kind() == reflect.Ptr {
		objVal = objVal.Elem()
	}

	switch objVal.Kind() {
	case reflect.Map:
		keys := objVal.MapKeys()
		for _, k := range keys {
			data[parseToString(k)] = parseToString(objVal.MapIndex(k).Interface())
		}
	case reflect.Struct:
		t := objVal.Type()
		for i := 0; i < t.NumField(); i++ {
			tag := t.Field(i).Tag.Get("yc")
			if tag == "-" || tag == "nil" {
				continue
			}

			if tag == "" {
				tag = strings.ToLower(t.Field(i).Name)
			}

			data[tag] = parseToString(objVal.Field(i).Interface())
		}
	default:
		err = errors.New("params data type is not support")
	}

	return
} // }}}

func parseToString(arg interface{}) string { // {{{
	switch val := arg.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	default:
		switch reflect.TypeOf(arg).Kind() {
		case reflect.Slice:
			s := []string{}
			argVal := reflect.ValueOf(arg)
			l := argVal.Len()
			for i := 0; i < l; i++ {
				s = append(s, parseToString(argVal.Index(i).Interface()))
			}
			return strings.Join(s, ",")
		case reflect.Map:
			s := []string{}
			argVal := reflect.ValueOf(arg)
			keys := argVal.MapKeys()
			for _, k := range keys {
				s = append(s, parseToString(argVal.MapIndex(k).Interface()))
			}
			return strings.Join(s, ",")
		default:
			return fmt.Sprint(arg)
		}
	}
} // }}}

func GetGuid(params interface{}) string { // {{{
	if jsonStr, err := json.MarshalIndent(params, "", ""); nil == err {
		str := strings.Replace(string(jsonStr), "\n", "", -1)

		rand.Seed(time.Now().UnixNano())
		r := rand.Intn(100000)

		h := md5.New()
		h.Write([]byte(str + strconv.Itoa(r)))

		return hex.EncodeToString(h.Sum(nil))
	}

	return ""
} // }}}

//obj 必须为指针 单条记录指向 struct , 多条记录指向map[xx]struct, map[xx]*struct, []struct 或 []*struct
func ParseResult(obj interface{}, res interface{}) error { // {{{
	sliceValue := reflect.Indirect(reflect.ValueOf(obj))

	switch sliceValue.Kind() {
	case reflect.Slice: // {{{
		data, ok := res.([]map[string]interface{})
		if !ok {
			return errors.New("response data is not match with the struct")
		}

		sliceElementType := sliceValue.Type().Elem()

		isptr := false
		if sliceElementType.Kind() == reflect.Ptr {
			isptr = true
		}

		for _, v := range data {
			if isptr {
				newValue := reflect.New(sliceElementType.Elem())
				if err := fillin(newValue.Interface(), v); nil != err {
					return err
				}
				sliceValue.Set(reflect.Append(sliceValue, reflect.ValueOf(newValue.Interface())))
			} else {
				newValue := reflect.New(sliceElementType)
				if err := fillin(newValue.Interface(), v); nil != err {
					return err
				}
				sliceValue.Set(reflect.Append(sliceValue, reflect.Indirect(reflect.ValueOf(newValue.Interface()))))
			}
		} // }}}
	case reflect.Map: // {{{
		sliceElementType := sliceValue.Type().Elem()

		isptr := false
		if sliceElementType.Kind() == reflect.Ptr {
			isptr = true
		}

		dataValue := reflect.Indirect(reflect.ValueOf(res))
		if dataValue.Kind() != reflect.Map {
			return errors.New("response data is not match with the struct")
		}

		keys := dataValue.MapKeys()
		for _, k := range keys {
			if isptr {
				newValue := reflect.New(sliceElementType.Elem())
				if err := fillin(newValue.Interface(), dataValue.MapIndex(k).Interface().(map[string]interface{})); nil != err {
					return err
				}
				sliceValue.SetMapIndex(k, reflect.ValueOf(newValue.Interface()))
			} else {
				newValue := reflect.New(sliceElementType)
				if err := fillin(newValue.Interface(), dataValue.MapIndex(k).Interface().(map[string]interface{})); nil != err {
					return err
				}
				sliceValue.SetMapIndex(k, reflect.Indirect(reflect.ValueOf(newValue.Interface())))
			}
		}
		// }}}
	case reflect.Struct:
		data, ok := res.(map[string]interface{})
		if !ok {
			return errors.New("response data is not match with the struct")
		}

		return fillin(obj, data)
	default:
		return errors.New("needs a pointer to a struct or map of struct or slice of struct")
	}

	return nil
} // }}}

//map to struct
func fillin(obj interface{}, data map[string]interface{}) error { // {{{
	dataStruct := reflect.Indirect(reflect.ValueOf(obj))
	typ := dataStruct.Type()

	numField := typ.NumField()
	for i := 0; i < numField; i++ {
		typField := typ.Field(i)
		valField := dataStruct.Field(i)

		if !valField.CanSet() {
			continue
		}

		tag := typField.Tag.Get("yc")
		if tag == "-" || tag == "nil" {
			continue
		}

		if tag == "" {
			tag = strings.ToLower(typField.Name)
		}

		value, ok := data[tag]
		if !ok {
			continue
		}

		kind := typField.Type.Kind()

		switch kind {
		case reflect.Bool:
			b, ok := value.(bool)
			if !ok {
				var err error
				b, err = strconv.ParseBool(parseToString(value))
				if err != nil {
					return err
				}
			}
			valField.SetBool(b)
		case reflect.String:
			valField.SetString(parseToString(value))
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			var i64 int64
			i, ok := value.(int)
			if !ok {
				var err error
				i64, err = strconv.ParseInt(parseToString(value), 0, 64)
				if err != nil {
					return err
				}
			} else {
				i64 = int64(i)
			}
			valField.SetInt(i64)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			u, ok := value.(uint64)
			if !ok {
				var err error
				u, err = strconv.ParseUint(parseToString(value), 0, 64)
				if err != nil {
					return err
				}
			}
			valField.SetUint(u)
		case reflect.Float32, reflect.Float64:
			f, ok := value.(float64)
			if !ok {
				var err error
				f, err = strconv.ParseFloat(parseToString(value), 64)
				if err != nil {
					return err
				}
			}
			valField.SetFloat(f)
		default:
			panic(fmt.Sprintf("unsupported type: %s", kind.String()))
		}
	}

	return nil
} // }}}
