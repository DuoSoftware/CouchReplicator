package postgresrep

import "fmt"
import _ "pq"
import "couchbase"
import "strings"
import "strconv"
import "reflect"
import "os"
import "database/sql"

func InitialMigrationC2PG(dbname, user, password, host, couchHost, couchPool, couchBucket, couchViewName, xmlPath, enableDelete string) {

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
			if (err != nil) || (f == nil) || (reflect.ValueOf(f).Kind() != reflect.Map) {
				fmt.Println("Object not found or mismacthed with structures for key :" + res.Rows[i].ID)
			} else {

				fmt.Println("Got value for key :" + res.Rows[i].ID + " and type is " + reflect.TypeOf(f).Kind().String())

				for _, table := range tables.Tables {
					//file.WriteString("Iterating table table.CouchName " + "\n")
					fmt.Println("Iterating table " + table.CouchName + "\n")
					if strings.Contains(res.Rows[i].ID, table.CouchName) {
						m := f.(map[string]interface{})
						//fmt.Println(m)
						var insertQuery = table.PGInsert
						//fmt.Println("Iterating change columns " + "\n")
						for _, prop := range table.PGChange {
							//file.WriteString("Iterating change column " + prop.ColumnName + "\n")
							var propValue = m[prop.ColumnName]
							var accountInt int64
							if m[prop.ColumnName] != nil {
								if reflect.TypeOf(propValue).Kind() == reflect.Float64 {
									accountInt = int64(m[prop.ColumnName].(float64))
									stringVal := strconv.FormatInt(accountInt, 10)
									insertQuery = strings.Replace(insertQuery, "@"+prop.ColumnName+"_", stringVal, 100)
								}
							}
						}

						fmt.Println("Iterating nested columns " + "\n")
						for _, nested := range table.NestedColumn {
							var strArray = []string{}
							//file.WriteString("Iterating nested column " + nested.ColumnName +"\n")
							strArray = strings.Split(nested.ColumnName, ".")
							var nestedValue interface{}
							if len(strArray) == 3 {
								index, _ := strconv.Atoi(strArray[1])
								if reflect.ValueOf(m[strArray[0]]).Len() > 0 {
									nestedValue = m[strArray[0]].([]interface{})[index].(map[string]interface{})[strArray[2]]
									if nested.Fixed == 1 {
										if nestedValue != nil {
											if reflect.TypeOf(nestedValue).Kind() == reflect.Float64 {
												accountInt := int64(nestedValue.(float64))
												stringVal := strconv.FormatInt(accountInt, 10)
												//file.WriteString(nested.ColumnName+" has fixed")
												insertQuery = strings.Replace(insertQuery, "@"+nested.ColumnName, stringVal, -1)
											}
										} else {
											file.WriteString(nested.ColumnName + " value is null")
										}
									} else {
										//file.WriteString(nested.ColumnName+" has not fixed")
										insertQuery = strings.Replace(insertQuery, "@"+nested.ColumnName, GetStringValue(nestedValue), -1)
									}
								} else {
									insertQuery = strings.Replace(insertQuery, "@"+nested.ColumnName, "", -1)
									//file.WriteString(insertQuery)
								}

							} else if len(strArray) == 2 {
								index, _ := strconv.Atoi(strArray[1])
								if reflect.ValueOf(m[strArray[0]]).Len() < 1 {
									insertQuery = strings.Replace(insertQuery, "@"+nested.ColumnName, "", -1)
								} else {
									if nestedValue != nil {
										nestedValue = m[strArray[0]].([]interface{})[index]
										if reflect.TypeOf(nestedValue).Kind() == reflect.Float64 {
											accountInt := int64(nestedValue.(float64))
											stringVal := strconv.FormatInt(accountInt, 10)
											insertQuery = strings.Replace(insertQuery, "@"+nested.ColumnName, stringVal, -1)
										} else {
											insertQuery = strings.Replace(insertQuery, "@"+nested.ColumnName, GetStringValue(nestedValue), -1)
										}
									} else {
										file.WriteString(nested.ColumnName + "nested value is null")
									}
								}

							}

						}

						for k, v := range m {

							if v != nil {
								insertQuery = strings.Replace(insertQuery, "@"+k, GetStringValue(v), -1)
							} else {
								insertQuery = strings.Replace(insertQuery, "@"+k, "", -1)
							}
						}

						fmt.Println(insertQuery)
						result, err := db.Exec(insertQuery)
						file.WriteString(insertQuery)
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

	bucket.Close()
	db.Close()
}

func UpdateC2PG(dbname, user, password, host, couchHost, couchPool, couchBucket, couchGetBucket, couchViewName, xmlPath, enableDelete string) (err error) {

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
	skipDeleteCount := 0
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

		file.WriteString("Getting couch data skip " + strconv.Itoa(skipCount) + "\n")
		if(enableDelete == "false"){
			skipDeleteCount = skipCount
		}else{
			skipDeleteCount = 0
		}
		
		res, err := bucket.View(couchViewName, couchViewName, map[string]interface{}{
			"stale": false,
			"limit": 5000,
			"skip":  skipDeleteCount,
		})

		if err != nil {
			fmt.Println("Couch bucket view error : " + err.Error() + "\n")
			file.WriteString(err.Error() + "\n")
		} else {
			file.WriteString("Got couch data" + strconv.Itoa(len(res.Rows)))
		}

		for i := 0; i < len(res.Rows); i++ {

			updateId := reflect.ValueOf(res.Rows[i].Value).Interface().(map[string]interface{})["ID"].(string)
			updateType := reflect.ValueOf(res.Rows[i].Value).Interface().(map[string]interface{})["Type"].(string)
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
									insertQuery = strings.Replace(insertQuery, "@"+prop.ColumnName+"_", stringVal, 100)
								}

							}

							for _, nested := range table.NestedColumn {
								var strArray = []string{}
								strArray = strings.Split(nested.ColumnName, ".")
								if len(strArray) == 3 {
									index, _ := strconv.Atoi(strArray[1])
									if reflect.ValueOf(m[strArray[0]]).Len() > 0 {
										nestedValue := m[strArray[0]].([]interface{})[index].(map[string]interface{})[strArray[2]]

										if nested.Fixed == 1 {
											if reflect.TypeOf(nestedValue).Kind() == reflect.Float64 {
												accountInt := int64(nestedValue.(float64))
												stringVal := strconv.FormatInt(accountInt, 10)
												insertQuery = strings.Replace(insertQuery, "@"+nested.ColumnName, stringVal, -1)
											}
										} else {
											insertQuery = strings.Replace(insertQuery, "@"+nested.ColumnName, GetStringValue(nestedValue), -1)
										}
									} else {
										insertQuery = strings.Replace(insertQuery, "@"+nested.ColumnName, "", -1)
									}

								} else if len(strArray) == 2 {
									index, _ := strconv.Atoi(strArray[1])
									if reflect.ValueOf(m[strArray[0]]).Len() > 0 {
										nestedValue := m[strArray[0]].([]interface{})[index]

										if reflect.TypeOf(nestedValue).Kind() == reflect.Float64 {
											accountInt := int64(nestedValue.(float64))
											stringVal := strconv.FormatInt(accountInt, 10)
											insertQuery = strings.Replace(insertQuery, "@"+nested.ColumnName, stringVal, -1)
										} else {
											insertQuery = strings.Replace(insertQuery, "@"+nested.ColumnName, GetStringValue(nestedValue), -1)
										}
									} else {
										insertQuery = strings.Replace(insertQuery, "@"+nested.ColumnName, "", -1)
									}

								}

							}

							for k, v := range m {

								if v != nil {
									insertQuery = strings.Replace(insertQuery, "@"+k, GetStringValue(v), -1)
								} else {
									insertQuery = strings.Replace(insertQuery, "@"+k, "", -1)
								}
							}

							result, err := db.Exec(insertQuery)
							if enableDelete == "true" {
								bucket.Delete(res.Rows[i].ID)
								fmt.Println(res.Rows[i].ID + " has deleted from bucket " + couchBucket)
							}
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
							if enableDelete == "true" {
								bucket.Delete(res.Rows[i].ID)
								fmt.Println(res.Rows[i].ID + " has deleted from bucket " + couchBucket)
							}
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
							if propValue != nil {
								if reflect.TypeOf(propValue).Kind() == reflect.Float64 {
									accountInt = int64(m[prop.ColumnName].(float64))
									stringVal := strconv.FormatInt(accountInt, 10)
									updateQuery = strings.Replace(updateQuery, "@"+prop.ColumnName+"_", stringVal, 100)
								}
							} else {
								updateQuery = strings.Replace(updateQuery, "@"+prop.ColumnName+"_", "", 100)
							}

						}

						for _, nested := range table.NestedColumn {
							var strArray = []string{}
							strArray = strings.Split(nested.ColumnName, ".")
							if len(strArray) == 3 {
								index, _ := strconv.Atoi(strArray[1])
								if reflect.ValueOf(m[strArray[0]]).Len() > 0 {
									nestedValue := m[strArray[0]].([]interface{})[index].(map[string]interface{})[strArray[2]]

									if nested.Fixed == 1 {
										if reflect.TypeOf(nestedValue).Kind() == reflect.Float64 {
											accountInt := int64(nestedValue.(float64))
											stringVal := strconv.FormatInt(accountInt, 10)
											updateQuery = strings.Replace(updateQuery, "@"+nested.ColumnName, stringVal, -1)
										}
									} else {
										updateQuery = strings.Replace(updateQuery, "@"+nested.ColumnName, GetStringValue(nestedValue), -1)
									}
								} else {
									updateQuery = strings.Replace(updateQuery, "@"+nested.ColumnName, "", -1)
								}

							} else if len(strArray) == 2 {
								index, _ := strconv.Atoi(strArray[1])
								if reflect.ValueOf(m[strArray[0]]).Len() > 0 {
									nestedValue := m[strArray[0]].([]interface{})[index]

									if reflect.TypeOf(nestedValue).Kind() == reflect.Float64 {
										accountInt := int64(nestedValue.(float64))
										stringVal := strconv.FormatInt(accountInt, 10)
										updateQuery = strings.Replace(updateQuery, "@"+nested.ColumnName, stringVal, -1)
									} else {
										updateQuery = strings.Replace(updateQuery, "@"+nested.ColumnName, GetStringValue(nestedValue), -1)
									}
								} else {
									updateQuery = strings.Replace(updateQuery, "@"+nested.ColumnName, "", -1)
								}

							}

						}

						for k, v := range m {

							if v != nil {
								updateQuery = strings.Replace(updateQuery, "@"+k, GetStringValue(v), -1)
							} else {
								updateQuery = strings.Replace(updateQuery, "@"+k, "", -1)
							}
						}

						//file.WriteString(updateQuery)
						result, err := db.Exec(updateQuery)
						if enableDelete == "true" {
							bucket.Delete(res.Rows[i].ID)
							fmt.Println(res.Rows[i].ID + " has deleted from bucket " + couchBucket)
						}
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
					} else {
						fmt.Println("Skipped key " + updateId + "processed " + strconv.Itoa(i) + " out of " + strconv.Itoa(res.TotalRows))
						if enableDelete == "true" {
							bucket.Delete(res.Rows[i].ID)
							fmt.Println(res.Rows[i].ID + " has deleted from bucket " + couchBucket)
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
	fmt.Println("Log file closed")
	db.Close()
	fmt.Println("Postgres connection closed")
	bucket.Close()
	fmt.Println("Couch bucket " + couchBucket + " closed")
	bucketGet.Close()
	fmt.Println("Couch bucket " + couchGetBucket + " closed")
	return
}
