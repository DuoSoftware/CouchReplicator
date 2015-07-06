package postgresrep

import "net/http"
import "fmt"
import _ "pq"
import "couchbase"
import "strings"
import "strconv"
import "reflect"
import "os"
import "database/sql"
import linq "go-linq"
import "redis_1"
import "io/ioutil"
import "encoding/json"

func InitialMigrationC2PG(dbname, user, password, host, couchHost, couchPool, couchBucket, couchViewName, xmlPath, enableDelete, serviceUri string) {

	file, _ := os.Create("loginsert.txt")
	//getting table mappings
	var tables = GetXMLData(xmlPath, file)
	var m map[string]interface{}

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

			result, err := http.Get(serviceUri + res.Rows[i].ID)
			if err != nil {
				fmt.Println("Object not found or mismacthed with structures for key :" + res.Rows[i].ID)
			} else {

				body, bodyErr := ioutil.ReadAll(result.Body)
				if bodyErr != nil {
					println("Body error" + bodyErr.Error())
					goto SilentSkip
				}

				str := string(body)

				bodyCloseErr := result.Body.Close()
				if bodyCloseErr != nil {
					println("Body close error" + bodyCloseErr.Error())
					goto SilentSkip
				}

				println(str)
				str = strings.Replace(str, "\\/", "-", -1)
				strUq, quoteErr := strconv.Unquote(str)
				if quoteErr != nil {
					println(quoteErr.Error())
					goto SilentSkip
				}

				if err := json.Unmarshal([]byte(strUq), &m); err != nil {
					println("json string ~" + str)
					println("Error" + err.Error())
					goto SilentSkip
				}

				fmt.Println("Got value for key :" + res.Rows[i].ID + " ------------ " + str)

				for _, table := range tables.Tables {
					//file.WriteString("Iterating table table.CouchName " + "\n")
					fmt.Println("Iterating table " + table.CouchName + "\n")
					if strings.Contains(res.Rows[i].ID, table.CouchName) {
						var insertQuery = table.PGInsert
						//fmt.Println("Iterating change columns " + "\n")
						for _, prop := range table.PGChange {
							//file.WriteString("Iterating change column " + prop.ColumnName + "\n")
							var propValue = m[prop.ColumnName]
							var accountInt int64
							if m[prop.ColumnName] != nil {
								if reflect.TypeOf(propValue).Kind() == reflect.Float64 {
									accountInt = reflect.Indirect(reflect.ValueOf(propValue)).Convert(reflect.TypeOf(int64(0))).Int()		
									stringVal := strconv.FormatInt(accountInt, 10)
									insertQuery = strings.Replace(insertQuery, "@"+prop.ColumnName+"_", stringVal, 100)
									
									fmt.Println(prop.ColumnName + " - " + reflect.TypeOf(propValue).Kind().String() + " - "+ stringVal)
								}
							}														
						}

						var strArray = []string{}
						fmt.Println("Iterating nested columns " + "\n")
						for _, nested := range table.NestedColumn {
							strArray = strings.Split(nested.ColumnName, ".")
							break
						}

						var nestedTrue = true
						if len(strArray) > 0 {
							if len(strArray[0]) > 0 {

								if reflect.ValueOf(m[strArray[0]]).IsValid() != true {
									goto SilentSkip
								}

								for i := 0; i < reflect.ValueOf(m[strArray[0]]).Len(); i++ {

									// reset the insert query for nested columns. without reset query, data not replace with query parameters
									var insertQuery = table.PGInsert

									for _, nested := range table.NestedColumn {
										var strArray = []string{}
										//file.WriteString("Iterating nested column " + nested.ColumnName +"\n")
										strArray = strings.Split(nested.ColumnName, ".")
										var nestedValue interface{}
										if len(strArray) == 3 {
											//index, _ := strconv.Atoi(strArray[1])
											if reflect.ValueOf(m[strArray[0]]).Len() > 0 {
												//--------------------------------------------------------------------------------------------------------------------------------

												//nestedValue = m[strArray[0]].([]interface{})[index].(map[string]interface{})[strArray[2]]
												nestedValue = m[strArray[0]].([]interface{})[i].(map[string]interface{})[strArray[2]]
												fmt.Println(nestedValue)

												if nested.Fixed == 1 {
													if nestedValue != nil {
														if reflect.TypeOf(nestedValue).Kind() == reflect.Float64 {
															accountInt := reflect.Indirect(reflect.ValueOf(nestedValue)).Convert(reflect.TypeOf(int64(0))).Int()
															stringVal := strconv.FormatInt(accountInt, 10)
															insertQuery = strings.Replace(insertQuery, "@"+nested.ColumnName, stringVal, -1)
															
															fmt.Println(nested.ColumnName + " - " + reflect.TypeOf(nestedValue).Kind().String() + " - "+ stringVal)
														}
													} else {
														file.WriteString(nested.ColumnName + " value is null")
													}
												} else {
													//file.WriteString(nested.ColumnName+" has not fixed")
													fmt.Println("Not Fixed " + nested.ColumnName)
													fmt.Println(insertQuery)
													fmt.Println("-----------------------------")
													insertQuery = strings.Replace(insertQuery, "@"+nested.ColumnName, GetStringValue(nestedValue), -1)
													fmt.Println(insertQuery)
													fmt.Println("Insert should changed")
												}

												//------------------------------------------------------------------------------------------------------------------------------------
											} else {
												insertQuery = strings.Replace(insertQuery, "@"+nested.ColumnName, "", -1)
												//file.WriteString(insertQuery)
											}

										} else if len(strArray) == 2 {
											fmt.Println("array 2 condition")
											index, _ := strconv.Atoi(strArray[1])
											if reflect.ValueOf(m[strArray[0]]).Len() < 1 {
												insertQuery = strings.Replace(insertQuery, "@"+nested.ColumnName, "", -1)
											} else {
												if nestedValue != nil {
													nestedValue = m[strArray[0]].([]interface{})[index]
													if reflect.TypeOf(nestedValue).Kind() == reflect.Float64 {
														accountInt := reflect.Indirect(reflect.ValueOf(nestedValue)).Convert(reflect.TypeOf(int64(0))).Int()	
														stringVal := strconv.FormatInt(accountInt, 10)
														insertQuery = strings.Replace(insertQuery, "@"+nested.ColumnName, stringVal, -1)
														
														fmt.Println(nested.ColumnName + " - " + reflect.TypeOf(nestedValue).Kind().String() + " - "+ stringVal)
													} else {
														insertQuery = strings.Replace(insertQuery, "@"+nested.ColumnName, GetStringValue(nestedValue), -1)
													}
												} else {
													file.WriteString(nested.ColumnName + "nested value is null")
												}
											}

										}

									}

									//insert query here

									nestedTrue = false
									fmt.Println(nestedTrue)

									for k, v := range m {

										if v != nil {
											insertQuery = strings.Replace(insertQuery, "@"+k, GetStringValue(v), -1)
										} else {
											insertQuery = strings.Replace(insertQuery, "@"+k, "", -1)
										}
									}

									//------------------------------------------------
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

									//------------------------------------------------

								} //index loop.....
							}
						}

						for k, v := range m {

							if v != nil {
								insertQuery = strings.Replace(insertQuery, "@"+k, GetStringValue(v), -1)
							} else {
								insertQuery = strings.Replace(insertQuery, "@"+k, "", -1)
							}
						}

						if nestedTrue == true {
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
						}

						fmt.Println("processed " + strconv.Itoa(i) + " out of " + strconv.Itoa(res.TotalRows))
					} else {
						fmt.Println("Skipped key " + res.Rows[i].ID + "processed " + strconv.Itoa(i) + " out of " + strconv.Itoa(res.TotalRows))
					}
				}
			}
		SilentSkip:
		}

		fmt.Println(strconv.Itoa(len(res.Rows)) + " data processed")
		file.WriteString(strconv.Itoa(len(res.Rows)) + " data processed" + "\n")
		skipCount += len(res.Rows)
		file.Close()
	}

	bucket.Close()
	db.Close()
}

func UpdateC2PG(dbname, user, password, host, couchHost, couchPool, couchBucket, xmlPath, enableDelete, redisIp, redisPassword, serviceUri string, redisDb int64) (err error) {

	fmt.Println("Connecting to the redis")
	var redisClient *redis_1.Client
	redisClient = redis_1.NewTCPClient(&redis_1.Options{Addr: redisIp + ":6379",
		Password: redisPassword, DB: redisDb})

	file, _ := os.Create("logupdate.txt")
	//getting table mappings
	var tables = GetXMLData(xmlPath, file)

	totalCouchRows, _ := redisClient.LLen("StatusBucket").Result()
	fmt.Println("Number of rows : " + strconv.FormatInt(totalCouchRows, 10))
	file.WriteString("Number of rows : " + strconv.FormatInt(totalCouchRows, 10) + "\n")

	file.WriteString("Connecting to postgres" + "\n")
	db, err := sql.Open("postgres", "postgres://"+user+":"+password+"@"+host+"/"+dbname+"?sslmode=disable")

	if err != nil {
		fmt.Println("Postgres connectivity error : " + err.Error() + "\n")
		file.WriteString(err.Error() + "\n")
	}

	listDataJson, listError := redisClient.LPop("StatusBucket").Result()
	fmt.Println(listDataJson)

	fmt.Println("Connected to postgres database " + dbname + " with user " + user)
	i := 0
	for listError == nil {

		var m map[string]interface{}
		i++
		listData := Json2UpdateType(listDataJson)
		updateId := listData.ID
		updateType := listData.Type
		objectType := listData.ObjectType

		tableSelected, _, _ := linq.From(tables.Tables).Where(
			func(in linq.T) (bool, error) { return in.(Table).CouchName == objectType, nil }).First()

		if tableSelected != nil {
			table := tableSelected.(Table)

			if updateType == "Insert" {

				fmt.Println("Getting value for key :" + updateId)
				file.WriteString("Getting value for key :" + updateId + "\n")

				result, err := http.Get(serviceUri + updateId)

				if err != nil {
					fmt.Println("Object not found for key :" + updateId + " ------ " + err.Error())
				} else {

					body, bodyErr := ioutil.ReadAll(result.Body)
					if bodyErr != nil {
						println("Body error" + bodyErr.Error())
						goto SilentSkip
					}

					str := string(body)

					bodyCloseErr := result.Body.Close()
					if bodyCloseErr != nil {
						println("Body close error" + bodyCloseErr.Error())
						goto SilentSkip
					}

					str = strings.Replace(str, "\\/", "-", -1)
					strUq, quoteErr := strconv.Unquote(str)
					if quoteErr != nil {
						println(quoteErr.Error())
					}

					if err := json.Unmarshal([]byte(strUq), &m); err != nil {
						println("json string ~" + str)
						println("Error" + err.Error())
						//panic(err)
						redisClient.RPush("StatusBucket", listDataJson)
						goto SilentSkip
					}

					fmt.Println("Got value for key :" + updateId)

					var insertQuery = table.PGInsert
					//file.WriteString("Iterating change columns " + "\n")
					for _, prop := range table.PGChange {
						//file.WriteString("Iterating change coumn " + prop.ColumnName + "\n")
						var propValue = m[prop.ColumnName]
						var accountInt int64
						if propValue != nil {
							if reflect.TypeOf(propValue).Kind() == reflect.Float64 {
								accountInt = int64(m[prop.ColumnName].(float64))
								stringVal := strconv.FormatInt(accountInt, 10)
								insertQuery = strings.Replace(insertQuery, "@"+prop.ColumnName+"_", stringVal, 100)
								
								fmt.Println(prop.ColumnName + " - " + reflect.TypeOf(propValue).Kind().String() + " - "+ stringVal)
							}
						} else {
							insertQuery = strings.Replace(insertQuery, "@"+prop.ColumnName+"_", "", 100)
						}

					}

					var nestedTrue = true

					var strArray = []string{}
					fmt.Println("Iterating nested columns " + "\n")
					for _, nested := range table.NestedColumn {
						strArray = strings.Split(nested.ColumnName, ".")
						break
					}

					if len(strArray) > 0 {
						if len(strArray[0]) > 0 {

							if reflect.ValueOf(m[strArray[0]]).IsValid() != true {
								goto SilentSkip
							}

							for i := 0; i < reflect.ValueOf(m[strArray[0]]).Len(); i++ {

								for _, nested := range table.NestedColumn {
									var strArray = []string{}
									strArray = strings.Split(nested.ColumnName, ".")
									if len(strArray) == 3 {
										//index, _ := strconv.Atoi(strArray[1])
										if reflect.ValueOf(m[strArray[0]]).Len() > 0 {
											nestedValue := m[strArray[0]].([]interface{})[i].(map[string]interface{})[strArray[2]]

											if nested.Fixed == 1 {
												if reflect.TypeOf(nestedValue).Kind() == reflect.Float64 {
													accountInt := int64(nestedValue.(float64))
													stringVal := strconv.FormatInt(accountInt, 10)
													insertQuery = strings.Replace(insertQuery, "@"+nested.ColumnName, stringVal, -1)
													
													fmt.Println(nested.ColumnName + " - " + reflect.TypeOf(nestedValue).Kind().String() + " - "+ stringVal)
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
												
												fmt.Println(nested.ColumnName + " - " + reflect.TypeOf(nestedValue).Kind().String() + " - "+ stringVal)
											} else {
												insertQuery = strings.Replace(insertQuery, "@"+nested.ColumnName, GetStringValue(nestedValue), -1)
											}
										} else {
											insertQuery = strings.Replace(insertQuery, "@"+nested.ColumnName, "", -1)
										}

									}

								}

								//insert query here

								nestedTrue = false
								fmt.Println(nestedTrue)

								for k, v := range m {

									if v != nil {
										insertQuery = strings.Replace(insertQuery, "@"+k, GetStringValue(v), -1)
									} else {
										insertQuery = strings.Replace(insertQuery, "@"+k, "", -1)
									}
								}

								//------------------------------------------------
								fmt.Println(insertQuery)
								result, err := db.Exec(insertQuery)
								file.WriteString(insertQuery)

								if err != nil {
									fmt.Println("Postgres insertion error : " + err.Error())
									file.WriteString(err.Error() + "\n")
									redisClient.RPush("StatusBucket", listDataJson)
								} else {
									status, ok := result.RowsAffected()
									if ok == nil {
										fmt.Println("Migrate status of key " + updateId + " is " + strconv.FormatInt(status, 10))
										file.WriteString("Migrate status of key " + updateId + " is " + strconv.FormatInt(status, 10))
									} else {
										fmt.Println("Migrate status of key " + updateId + " is " + ok.Error())
										file.WriteString("Migrate status of key " + updateId + " is " + ok.Error())
										redisClient.RPush("StatusBucket", listDataJson)
									}
								}

							} // index loop endup here
						}
					}

					for k, v := range m {

						if v != nil {
							insertQuery = strings.Replace(insertQuery, "@"+k, GetStringValue(v), -1)
						} else {
							insertQuery = strings.Replace(insertQuery, "@"+k, "", -1)
						}
					}

					if nestedTrue == true {
						result, err := db.Exec(insertQuery)
						fmt.Println(insertQuery)
						if err != nil {
							fmt.Println("Postgres insertion error : " + err.Error())
							file.WriteString(err.Error() + "\n")
							redisClient.RPush("StatusBucket", listDataJson)
						} else {
							status, ok := result.RowsAffected()
							if ok == nil {
								fmt.Println("Migrate status of key " + updateId + " is " + strconv.FormatInt(status, 10))
								file.WriteString("Migrate status of key " + updateId + " is " + strconv.FormatInt(status, 10))
							} else {
								fmt.Println("Migrate status of key " + updateId + " is " + ok.Error())
								file.WriteString("Migrate status of key " + updateId + " is " + ok.Error())
								redisClient.RPush("StatusBucket", listDataJson)
							}
						}
					}

					fmt.Println("processed " + strconv.Itoa(i) + " out of " + strconv.FormatInt(totalCouchRows, 10))
				}

			} else {

				fmt.Println("Getting value for key :" + updateId)
				result, err := http.Get(serviceUri + updateId)

				if err != nil {
					fmt.Println("Object not found for key :" + updateId)
				} else {

					body, bodyErr := ioutil.ReadAll(result.Body)
					if bodyErr != nil {
						println("Body error" + bodyErr.Error())
						goto SilentSkip
					}

					str := string(body)

					bodyCloseErr := result.Body.Close()
					if bodyCloseErr != nil {
						println("Body close error" + bodyCloseErr.Error())
						goto SilentSkip
					}

					str = strings.Replace(str, "\\/", "-", -1)
					println("Before unquote " + str)
					println()
					strQuoted, quoteErr := strconv.Unquote(str)
					if quoteErr != nil {
						println(quoteErr.Error())
					}

					println("Unquoted " + strQuoted)
					//str = strings.Replace(str, "\\", "", -1)
					fmt.Println("Marshalled string ~ " + strQuoted)
					if err := json.Unmarshal([]byte(strQuoted), &m); err != nil {
						println("json string ~" + str)
						println("Error" + err.Error())
						//panic(err)
						redisClient.RPush("StatusBucket", listDataJson)
						goto SilentSkip
					}

					fmt.Println("Got value for key :" + updateId)

					var updateQuery = table.PGUpdate
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
								
								fmt.Println(prop.ColumnName + " - " + reflect.TypeOf(propValue).Kind().String() + " - "+ stringVal)
							}
						} else {
							updateQuery = strings.Replace(updateQuery, "@"+prop.ColumnName+"_", "", 100)
						}
					}

					var nestedTrue = true

					var strArray = []string{}
					fmt.Println("Iterating nested columns " + "\n")
					for _, nested := range table.NestedColumn {
						strArray = strings.Split(nested.ColumnName, ".")
						break
					}

					if len(strArray) > 0 {
						if len(strArray[0]) > 0 {

							if reflect.ValueOf(m[strArray[0]]).IsValid() != true {
								goto SilentSkip
							}

							for i := 0; i < reflect.ValueOf(m[strArray[0]]).Len(); i++ {

								for _, nested := range table.NestedColumn {
									var strArray = []string{}
									strArray = strings.Split(nested.ColumnName, ".")
									if len(strArray) == 3 {
										//index, _ := strconv.Atoi(strArray[1])
										if reflect.ValueOf(m[strArray[0]]).Len() > 0 {
											nestedValue := m[strArray[0]].([]interface{})[i].(map[string]interface{})[strArray[2]]

											if nested.Fixed == 1 {
												if reflect.TypeOf(nestedValue).Kind() == reflect.Float64 {
													accountInt := int64(nestedValue.(float64))
													stringVal := strconv.FormatInt(accountInt, 10)
													updateQuery = strings.Replace(updateQuery, "@"+nested.ColumnName, stringVal, -1)
													
													fmt.Println(nested.ColumnName + " - " + reflect.TypeOf(nestedValue).Kind().String() + " - "+ stringVal)
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
												
												fmt.Println(nested.ColumnName + " - " + reflect.TypeOf(nestedValue).Kind().String() + " - "+ stringVal)
											} else {
												updateQuery = strings.Replace(updateQuery, "@"+nested.ColumnName, GetStringValue(nestedValue), -1)
											}
										} else {
											updateQuery = strings.Replace(updateQuery, "@"+nested.ColumnName, "", -1)
										}

									}

								}

								//insert query here

								nestedTrue = false
								fmt.Println(nestedTrue)
								//------------------------------------------------
								fmt.Println(updateQuery)
								result, err := db.Exec(updateQuery)
								file.WriteString(updateQuery)

								if err != nil {
									fmt.Println("Postgres insertion error : " + err.Error())
									file.WriteString(err.Error() + "\n")
									redisClient.RPush("StatusBucket", listDataJson)
								} else {
									status, ok := result.RowsAffected()
									if ok == nil {
										fmt.Println("Migrate status of key " + updateId + " is " + strconv.FormatInt(status, 10))
										file.WriteString("Migrate status of key " + updateId + " is " + strconv.FormatInt(status, 10))
									} else {
										fmt.Println("Migrate status of key " + updateId + " is " + ok.Error())
										file.WriteString("Migrate status of key " + updateId + " is " + ok.Error())
										redisClient.RPush("StatusBucket", listDataJson)
									}
								}

							} // index loop endup here
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
					if nestedTrue == true {
						result, err := db.Exec(updateQuery)
						fmt.Println(updateQuery)
						if err != nil {
							fmt.Println("Postgres update error : " + err.Error())
							file.WriteString(err.Error() + "\n")
							redisClient.RPush("StatusBucket", listDataJson)
						} else {
							status, ok := result.RowsAffected()
							if ok == nil {
								fmt.Println("Migrate status of key " + updateId + " is " + strconv.FormatInt(status, 10))
								file.WriteString("Migrate status of key " + updateId + " is " + strconv.FormatInt(status, 10))
							} else {
								fmt.Println("Migrate status of key " + updateId + " is " + ok.Error())
								file.WriteString("Migrate status of key " + updateId + " is " + ok.Error())
								redisClient.RPush("StatusBucket", listDataJson)
							}
						}
					}
				}
			}
		} else {
			fmt.Println("Skipped key " + updateId + ", There was no table found on configuration file. processed ")
		}

	SilentSkip:

		listDataJson, listError = redisClient.LPop("StatusBucket").Result()
	}

	file.Close()
	fmt.Println("Log file closed")
	db.Close()
	fmt.Println("Postgres connection closed")
	redisClient.Close()
	fmt.Println("Postgres connection closed")
	return
}

func BulkDeleteFromCouch(couchHost, couchPool, couchBucket, couchViewName string) {

	file, _ := os.Create("bulkDelete.txt")

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
	continueDelete := true

	if totalCouchRows == 0 {
		continueDelete = false
	}

	fmt.Println("Number of rows : " + strconv.Itoa(totalCouchRows))
	file.WriteString("Number of rows : " + strconv.Itoa(totalCouchRows) + "\n")

	for continueDelete == true {

		file.WriteString("Getting couch data" + "\n")
		res, err := bucket.View(couchViewName, couchViewName, map[string]interface{}{
			"stale": false,
			"limit": 1000,
			"skip":  0,
		})

		if err != nil {
			fmt.Println("Couch bucket view error : " + err.Error() + "\n")
			file.WriteString(err.Error() + "\n")
		}

		if res.TotalRows == 0 {
			break
		}

		for i := 0; i < len(res.Rows); i++ {

			fmt.Println("Deleting value for key :" + res.Rows[i].ID)

			err := bucket.Delete(res.Rows[i].ID)
			if err != nil {
				fmt.Println("Object delete error for key :" + res.Rows[i].ID + " || " + err.Error())
				file.WriteString("Object delete error for key :" + res.Rows[i].ID + " || " + err.Error())
			} else {
				fmt.Println("Object deleted for key :" + res.Rows[i].ID)
			}
		}
	}

	file.Close()
	bucket.Close()
}