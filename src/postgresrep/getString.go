package postgresrep

import "strconv"
import "reflect"

func GetStringValue(v interface{}) (value string) {

	var str string

	if(v == nil){
		str = ""
	}else if reflect.TypeOf(v).Kind() == reflect.Float64 {
		var floatId float64 = v.(float64)
		str = strconv.FormatFloat(floatId, 'f', 2, 32)

	} else if reflect.TypeOf(v).Kind() == reflect.Int {
		var intId int = v.(int)
		str = strconv.Itoa(intId)

	} else if reflect.TypeOf(v).Kind() == reflect.String {
		str = v.(string)

	} else if reflect.TypeOf(v).Kind() == reflect.Bool {
		var boolVal bool = v.(bool)
		str = strconv.FormatBool(boolVal)

	} else if reflect.TypeOf(v).Kind() == reflect.Slice {
		var stringSlice string
		s := reflect.ValueOf(v)
		for i := 0; i < s.Len(); i++ {

			if reflect.ValueOf(s.Index(i).Interface()).Kind() == reflect.Float64 {
				var floatId float64 = s.Index(i).Interface().(float64)
				var tempInt int64 = int64(floatId)
				stringSlice = strconv.FormatInt(tempInt, 10)
			} else if reflect.ValueOf(s.Index(i).Interface()).Kind() == reflect.Int {
				var intId int = s.Index(i).Interface().(int)
				stringSlice = strconv.Itoa(intId)
			} else if reflect.ValueOf(s.Index(i).Interface()).Kind() == reflect.String {
				stringSlice = s.Index(i).Interface().(string)
			} else if reflect.ValueOf(s.Index(i).Interface()).Kind() == reflect.Int64 {
				var intId int64 = s.Index(i).Interface().(int64)
				stringSlice = strconv.FormatInt(intId, 10)
			}

			if i == 0 {
				str = str + stringSlice
			} else {
				str = str + "," + stringSlice
			}
		}
	}

	return str
}