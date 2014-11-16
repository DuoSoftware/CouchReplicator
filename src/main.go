package main

import "postgresrep"
import "fmt"
//import "reflect"
//import "encoding/json"
//import "strconv"

func main() {

//	b := []byte(`{"Name":"Nuwan","Age":28,"Parents":[123,123]}`)
//	var f interface{}
//	json.Unmarshal(b, &f)
//	m := f.(map[string]interface{})
//	if m["Parents"] == nil {
//		fmt.Println("no values")
//	} else {
//		var s1 string
//		var str string 
//		s := reflect.ValueOf(m["Parents"])
//		for i := 0; i < s.Len(); i++ {
//			fmt.Println(reflect.ValueOf(s.Index(i).Interface()).Kind())
//			if reflect.ValueOf(s.Index(i).Interface()).Kind() == reflect.Float64 {
//				var floatId float64 = s.Index(i).Interface().(float64)
//				var profileIdInt int64 = int64(floatId)
//				str = strconv.FormatInt(profileIdInt, 10)
//			} else if reflect.ValueOf(s.Index(i).Interface()).Kind() == reflect.Int {
//				var intId int = s.Index(i).Interface().(int)
//				str = strconv.Itoa(intId)
//			} else if reflect.ValueOf(s.Index(i).Interface()).Kind() == reflect.String {
//				str = s.Index(i).Interface().(string)
//			} else if reflect.ValueOf(s.Index(i).Interface()).Kind() == reflect.Int64 {
//				var intId int64 = s.Index(i).Interface().(int64)
//				str = strconv.FormatInt(intId, 10)
//			}
//
//			if i == 0 {
//				s1 = s1 + str
//			} else {
//				s1 = s1 + "," + str
//			}
//		}
//
//		fmt.Println(s1)
//	}

		var DB, User, Password, Host, CouchHost, CouchPool, CouchBucketInsert, CouchBucketUpdate, CouchViewInsert, CouchViewUpdate, XMLPath, Option string
	
		fmt.Print("Postgres database name : ")
		fmt.Scanf("%s\n", &DB)
		fmt.Println()
	
		fmt.Print("Postgres user name : ")
		fmt.Scanf("%s\n", &User)
		fmt.Println()
	
		fmt.Print("Postgres password : ")
		fmt.Scanf("%s\n", &Password)
		fmt.Println()
	
		fmt.Print("Postgres database host : ")
		fmt.Scanf("%s\n", &Host)
		fmt.Println()
	
		fmt.Print("Couch host name : ")
		fmt.Scanf("%s\n", &CouchHost)
		fmt.Println()
	
		fmt.Print("Couch pool name : ")
		fmt.Scanf("%s\n", &CouchPool)
		fmt.Println()
	
		fmt.Print("Couch bucket name for insert : ")
		fmt.Scanf("%s\n", &CouchBucketInsert)
		fmt.Println()
	
		fmt.Print("Couch bucket name for update : ")
		fmt.Scanf("%s\n", &CouchBucketUpdate)
		fmt.Println()
	
		fmt.Print("Couch view name for insert: ")
		fmt.Scanf("%s\n", &CouchViewInsert)
		fmt.Println()
	
		fmt.Print("Couch view name for update: ")
		fmt.Scanf("%s\n", &CouchViewUpdate)
		fmt.Println()
	
		fmt.Print("XML Path: ")
		fmt.Scanf("%s\n", &XMLPath)
		fmt.Println()
	
		fmt.Println("1 : InitialMigration")
		fmt.Println("2 : Updates")
		fmt.Scanf("%s\n", &Option)
	
		if(Option == "1"){
			postgresrep.InitialMigrationC2PG(DB, User, Password, Host, CouchHost, CouchPool, CouchBucketInsert, CouchViewInsert, XMLPath)
		}

}
