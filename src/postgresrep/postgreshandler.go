package postgresrep

import "database/sql"
import "fmt"
import _ "pq"
import "couchbase"
import "strings"
import "strconv"
import "reflect"
import "os"

func InitialMigrationC2PG(dbname, user, password, host, couchHost, couchPool, couchBucket, couchViewName, xmlPath string) {

	file, _ := os.Create("loginsert.txt")
	//getting table mappings
	var tables = GetXMLData(xmlPath, file)

	fmt.Println("Connecting to the couch")
	file.WriteString("Connecting to the couch" + "\n")
	client, err := couchbase.Connect("http://" + couchHost + ":8091/")
	if err != nil {
		fmt.Println("couch connection error : " + err.Error() + "\n")
		file.WriteString(err.Error() + "\n")
	}

	file.WriteString("Getting couch pool " + "\n")
	pool, err := client.GetPool(couchPool)
	if err != nil {
		fmt.Println("couch connection error : " + err.Error() + "\n")
		file.WriteString(err.Error() + "\n")
	}

	file.WriteString("Getting couch bucket " + "\n")
	bucket, err := pool.GetBucket(couchBucket)
	if err != nil {
		fmt.Println("couch get bucket error : " + err.Error() + "\n")
		file.WriteString(err.Error() + "\n")
	}

	file.WriteString("Getting couch view " + "\n")
	skipCount := 0
	res, err := bucket.View(couchViewName, couchViewName, map[string]interface{}{
		"stale": false,
		"limit": 1,
		"skip":  0,
	})

	if err != nil {
		fmt.Println("couch bucket view : " + err.Error() + "\n")
		file.WriteString(err.Error() + "\n")
	}

	totalCouchRows := res.TotalRows
	fmt.Println("Number of rows : " + strconv.Itoa(totalCouchRows))
	file.WriteString("Number of rows : " + strconv.Itoa(totalCouchRows) + "\n")

	var f interface{}

	file.WriteString("Connecting to postgres" + "\n")
	db, err := sql.Open("postgres", "postgres://"+user+":"+password+"@"+host+"/"+dbname+"?sslmode=disable")

	if err != nil {
		fmt.Println("Postgres connectivity error : " + err.Error() + "\n")
		file.WriteString(err.Error() + "\n")
	}

	fmt.Println("Connected to postgres database " + dbname + " with user " + user)

	for skipCount < totalCouchRows {

		file.WriteString("Getting couch data" + "\n")
		res, err := bucket.View(couchViewName, couchViewName, map[string]interface{}{
			"stale": false,
			"limit": 5000,
			"skip":  skipCount,
		})

		if err != nil {
			fmt.Println("Couch bucket view error : " + err.Error() + "\n")
			file.WriteString(err.Error() + "\n")
		} else {
			file.WriteString("Got couch data" + "\n")
		}

		for i := 0; i < len(res.Rows); i++ {

			fmt.Println("Getting value for key :" + res.Rows[i].ID)
			file.WriteString("Getting value for key :" + res.Rows[i].ID + "\n")

			err := bucket.Get(res.Rows[i].ID, &f)
			if (err != nil) || (f == nil) {
				fmt.Println("Object not found for key :" + res.Rows[i].ID)
			} else {

				fmt.Println("Got value for key :" + res.Rows[i].ID)

				for _, table := range tables.Tables {
					//file.WriteString("Iterating table table.CouchName " + "\n")
					if strings.Contains(res.Rows[i].ID, table.CouchName) {

						m := f.(map[string]interface{})

						//file.WriteString("Iterating table couch " + table.CouchName + "\n")
						var insertQuery = table.PGInsert
						//file.WriteString("Iterating change columns " + "\n")
						for _, prop := range table.PGChange {
							//file.WriteString("Iterating change coumn " + prop.ColumnName + "\n")
							var propValue = m[prop.ColumnName]
							var accountInt int64
							if reflect.TypeOf(propValue).Kind() == reflect.Float64 {
								accountInt = int64(m[prop.ColumnName].(float64))
								stringVal := strconv.FormatInt(accountInt, 10)
								insertQuery = strings.Replace(insertQuery, prop.ColumnName+"_", stringVal, 100)
							}

						}

						//file.WriteString("Iterating change columns finished " + "\n")

						insertQuery = strings.Replace(insertQuery, "quote_", "\"", 100)

						for k, v := range m {

							if v != nil {

								if reflect.TypeOf(v).Kind() == reflect.Float64 {
									var floatId float64 = v.(float64)
									var profileIdInt int64 = int64(floatId)
									str := strconv.FormatInt(profileIdInt, 10)
									insertQuery = strings.Replace(insertQuery, k, str, 100)
									//file.WriteString(k + " -- " + reflect.TypeOf(v).Kind().String() + "\n")
								} else if reflect.TypeOf(v).Kind() == reflect.Int {
									var intId int = v.(int)
									str := strconv.Itoa(intId)
									insertQuery = strings.Replace(insertQuery, k, str, 100)
									//file.WriteString(k + " -- " + reflect.TypeOf(v).Kind().String() + "\n")
								} else if reflect.TypeOf(v).Kind() == reflect.String {
									str := v.(string)
									insertQuery = strings.Replace(insertQuery, k, str, 100)
									//file.WriteString(k + " -- " + reflect.TypeOf(v).Kind().String() + "\n")
								} else if reflect.TypeOf(v).Kind() == reflect.Bool {
									var boolVal bool = v.(bool)
									str := strconv.FormatBool(boolVal)
									insertQuery = strings.Replace(insertQuery, k, str, 100)
									//file.WriteString(k + " -- " + reflect.TypeOf(v).Kind().String() + "\n")
								} else if reflect.TypeOf(v).Kind() == reflect.Slice {
									if v != nil {
										//file.WriteString(k + " -- " + reflect.TypeOf(v).Kind().String() + "\n")
										var stringVal string
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
												stringVal = stringVal + stringSlice
											} else {
												stringVal = stringVal + "," + stringSlice
											}
										}

										insertQuery = strings.Replace(insertQuery, k, stringVal, 100)

									} else {
										insertQuery = strings.Replace(insertQuery, k, "", 100)
									}
								} else {
									//file.WriteString(k + " -- " + reflect.TypeOf(v).Kind().String() + "\n")
								}
							} else {
								insertQuery = strings.Replace(insertQuery, k, "", 100)
							}
						}

						//file.WriteString(insertQuery)
						result, err := db.Exec(insertQuery)
						fmt.Println(insertQuery)
						if err != nil {
							fmt.Println("Postgres insertion error : " + err.Error())
							file.WriteString(err.Error() + "\n")
						} else {
							status, ok := result.RowsAffected()
							if ok == nil {
								fmt.Println("Migrate status of key " + res.Rows[0].ID + " is " + strconv.FormatInt(status, 10))
								file.WriteString("Migrate status of key " + res.Rows[0].ID + " is " + strconv.FormatInt(status, 10))
							} else {
								fmt.Println("Migrate status of key " + res.Rows[0].ID + " is " + ok.Error())
								file.WriteString("Migrate status of key " + res.Rows[0].ID + " is " + ok.Error())
							}
						}

						fmt.Println("processed " + strconv.Itoa(i) + " out of " + strconv.Itoa(res.TotalRows))
					} else {
						fmt.Println("Skipped key " + res.Rows[i].ID + "processed " + strconv.Itoa(i) + " out of " + strconv.Itoa(res.TotalRows))
					}
				}
			}
		}

		fmt.Println(strconv.Itoa(len(res.Rows)) + " data processed")
		file.WriteString(strconv.Itoa(len(res.Rows)) + " data processed" + "\n")
		skipCount += len(res.Rows)
		file.Close()
	}
	
	db.Close()
}

func UpdateC2PG(dbname, user, password, host, couchHost, couchPool, couchBucket, couchGetBucket, couchViewName, xmlPath string) (err error) {

	file, _ := os.Create("logupdate.txt")
	//getting table mappings
	var tables = GetXMLData(xmlPath, file)

	fmt.Println("Connecting to the couch")
	file.WriteString("Connecting to the couch" + "\n")
	client, err := couchbase.Connect("http://" + couchHost + ":8091/")
	if err != nil {
		fmt.Println("couch connection error : " + err.Error() + "\n")
		file.WriteString(err.Error() + "\n")
	}

	file.WriteString("Getting couch pool " + "\n")
	pool, err := client.GetPool(couchPool)
	if err != nil {
		fmt.Println("couch connection error : " + err.Error() + "\n")
		file.WriteString(err.Error() + "\n")
	}

	file.WriteString("Getting couch bucket " + "\n")
	bucket, err := pool.GetBucket(couchBucket)
	if err != nil {
		fmt.Println("couch get bucket error : " + err.Error() + "\n")
		file.WriteString(err.Error() + "\n")
	}

	file.WriteString("Getting couch bucket 2" + "\n")
	bucketGet, err := pool.GetBucket(couchGetBucket)
	if err != nil {
		fmt.Println("couch get bucket 2 error : " + err.Error() + "\n")
		file.WriteString(err.Error() + "\n")
	}

	file.WriteString("Getting couch view " + "\n")
	skipCount := 0
	res, err := bucket.View(couchViewName, couchViewName, map[string]interface{}{
		"stale": false,
		"limit": 1,
		"skip":  0,
	})

	if err != nil {
		fmt.Println("couch bucket view : " + err.Error() + "\n")
		file.WriteString(err.Error() + "\n")
	}

	totalCouchRows := res.TotalRows
	fmt.Println("Number of rows : " + strconv.Itoa(totalCouchRows))
	file.WriteString("Number of rows : " + strconv.Itoa(totalCouchRows) + "\n")

	var f interface{}

	file.WriteString("Connecting to postgres" + "\n")
	db, err := sql.Open("postgres", "postgres://"+user+":"+password+"@"+host+"/"+dbname+"?sslmode=disable")

	if err != nil {
		fmt.Println("Postgres connectivity error : " + err.Error() + "\n")
		file.WriteString(err.Error() + "\n")
	}

	fmt.Println("Connected to postgres database " + dbname + " with user " + user)

	for skipCount < totalCouchRows {

		file.WriteString("Getting couch data skip " +strconv.Itoa(skipCount)+ "\n")
		res, err := bucket.View(couchViewName, couchViewName, map[string]interface{}{
			"stale": false,
			"limit": 5000,
			"skip":  skipCount,
		})

		if err != nil {
			fmt.Println("Couch bucket view error : " + err.Error() + "\n")
			file.WriteString(err.Error() + "\n")
		} else {
			file.WriteString("Got couch data" + strconv.Itoa(len(res.Rows)))
		}		
		
		for i := 0; i < len(res.Rows); i++ {
			
			updateId := reflect.ValueOf(res.Rows[0].Value).Interface().(map[string]interface{})["ID"].(string)
			updateType := reflect.ValueOf(res.Rows[0].Value).Interface().(map[string]interface{})["Type"].(string)
			bucketGet.Get(updateId, &f)

			if updateType == "Insert" {

				fmt.Println("Getting value for key :" + updateId)
				file.WriteString("Getting value for key :" + updateId + "\n")

				err := bucketGet.Get(updateId, &f)
				if (err != nil) || (f == nil) {
					fmt.Println("Object not found for key :" + updateId)
				} else {

					m := f.(map[string]interface{})

					fmt.Println("Got value for key :" + updateId)

					for _, table := range tables.Tables {
						//file.WriteString("Iterating table table.CouchName " + "\n")
						if strings.Contains(updateId, table.CouchName) {

							//file.WriteString("Iterating table couch " + table.CouchName + "\n")
							var insertQuery = table.PGInsert
							//file.WriteString("Iterating change columns " + "\n")
							for _, prop := range table.PGChange {
								//file.WriteString("Iterating change coumn " + prop.ColumnName + "\n")
								var propValue = m[prop.ColumnName]
								var accountInt int64
								if reflect.TypeOf(propValue).Kind() == reflect.Float64 {
									accountInt = int64(m[prop.ColumnName].(float64))
									stringVal := strconv.FormatInt(accountInt, 10)
									insertQuery = strings.Replace(insertQuery, prop.ColumnName+"_", stringVal, 100)
								}

							}

							//file.WriteString("Iterating change columns finished " + "\n")

							insertQuery = strings.Replace(insertQuery, "quote_", "\"", 100)

							for k, v := range m {

								if v != nil {

									if reflect.TypeOf(v).Kind() == reflect.Float64 {
										var floatId float64 = v.(float64)
										var profileIdInt int64 = int64(floatId)
										str := strconv.FormatInt(profileIdInt, 10)
										insertQuery = strings.Replace(insertQuery, k, str, 100)
										//file.WriteString(k + " -- " + reflect.TypeOf(v).Kind().String() + "\n")
									} else if reflect.TypeOf(v).Kind() == reflect.Int {
										var intId int = v.(int)
										str := strconv.Itoa(intId)
										insertQuery = strings.Replace(insertQuery, k, str, 100)
										//file.WriteString(k + " -- " + reflect.TypeOf(v).Kind().String() + "\n")
									} else if reflect.TypeOf(v).Kind() == reflect.String {
										str := v.(string)
										insertQuery = strings.Replace(insertQuery, k, str, 100)
										//file.WriteString(k + " -- " + reflect.TypeOf(v).Kind().String() + "\n")
									} else if reflect.TypeOf(v).Kind() == reflect.Bool {
										var boolVal bool = v.(bool)
										str := strconv.FormatBool(boolVal)
										insertQuery = strings.Replace(insertQuery, k, str, 100)
										//file.WriteString(k + " -- " + reflect.TypeOf(v).Kind().String() + "\n")
									} else if reflect.TypeOf(v).Kind() == reflect.Slice {
										if v != nil {
											//file.WriteString(k + " -- " + reflect.TypeOf(v).Kind().String() + "\n")
											var stringVal string
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
													stringVal = stringVal + stringSlice
												} else {
													stringVal = stringVal + "," + stringSlice
												}
											}

											insertQuery = strings.Replace(insertQuery, k, stringVal, 100)

										} else {
											insertQuery = strings.Replace(insertQuery, k, "", 100)
										}
									} else {
										//file.WriteString(k + " -- " + reflect.TypeOf(v).Kind().String() + "\n")
									}
								} else {
									insertQuery = strings.Replace(insertQuery, k, "", 100)
								}
							}

							//file.WriteString(insertQuery)
							result, err := db.Exec(insertQuery)
							fmt.Println(insertQuery)
							if err != nil {
								fmt.Println("Postgres insertion error : " + err.Error())
								file.WriteString(err.Error() + "\n")
							} else {
								status, ok := result.RowsAffected()
								if ok == nil {
									fmt.Println("Migrate status of key " + res.Rows[0].ID + " is " + strconv.FormatInt(status, 10))
									file.WriteString("Migrate status of key " + res.Rows[0].ID + " is " + strconv.FormatInt(status, 10))
								} else {
									fmt.Println("Migrate status of key " + res.Rows[0].ID + " is " + ok.Error())
									file.WriteString("Migrate status of key " + res.Rows[0].ID + " is " + ok.Error())
								}
							}

							fmt.Println("processed " + strconv.Itoa(i) + " out of " + strconv.Itoa(res.TotalRows))
						} else {
							fmt.Println("Skipped key " + res.Rows[i].ID + "processed " + strconv.Itoa(i) + " out of " + strconv.Itoa(res.TotalRows))
						}
					}
				}

			} else {

				for _, table := range tables.Tables {
					//file.WriteString("Iterating tables " + "\n")
					if strings.Contains(updateId, table.CouchName) {

						//file.WriteString("Iterating table couch " + table.CouchName + "\n")
						var updateQuery = table.PGUpdate
						m := f.(map[string]interface{})
						//file.WriteString("Iterating change coumns " + "\n")
						for _, prop := range table.PGChange {
							//file.WriteString("Iterating change coumn " + prop.ColumnName + "\n")
							var propValue = m[prop.ColumnName]
							var accountInt int64
							if reflect.TypeOf(propValue).Kind() == reflect.Float64 {
								accountInt = int64(m[prop.ColumnName].(float64))
								stringVal := strconv.FormatInt(accountInt, 10)
								updateQuery = strings.Replace(updateQuery, prop.ColumnName+"_", stringVal, 100)
							}

						}

						updateQuery = strings.Replace(updateQuery, "quote_", "\"", 100)

						for k, v := range m {
							if v != nil {
								if reflect.TypeOf(v).Kind() == reflect.Float64 {
									var floatId float64 = v.(float64)
									var profileIdInt int64 = int64(floatId)
									str := strconv.FormatInt(profileIdInt, 10)
									updateQuery = strings.Replace(updateQuery, k, str, 100)
								} else if reflect.TypeOf(v).Kind() == reflect.Int {
									var intId int = v.(int)
									str := strconv.Itoa(intId)
									updateQuery = strings.Replace(updateQuery, k, str, 100)
								} else if reflect.TypeOf(v).Kind() == reflect.String {
									str := v.(string)
									updateQuery = strings.Replace(updateQuery, k, str, 100)
								}
							}
						}

						//file.WriteString(updateQuery)
						result, err := db.Exec(updateQuery)
						fmt.Println(updateQuery)
						if err != nil {
							fmt.Println("Postgres update error : " + err.Error())
							file.WriteString(err.Error() + "\n")
						} else {
							status, ok := result.RowsAffected()
							if ok == nil {
									fmt.Println("Migrate status of key " + updateId + " is " + strconv.FormatInt(status, 10))
									file.WriteString("Migrate status of key " + updateId + " is " + strconv.FormatInt(status, 10))
								} else {
									fmt.Println("Migrate status of key " + updateId + " is " + ok.Error())
									file.WriteString("Migrate status of key " + updateId + " is " + ok.Error())
								}
						}
					}

				}

			}
		}

		fmt.Println(strconv.Itoa(len(res.Rows)) + " data update")
		file.WriteString(strconv.Itoa(len(res.Rows)) + " data update" + "\n")
		skipCount += len(res.Rows)
	}

	file.Close()
	db.Close()
	return
}
