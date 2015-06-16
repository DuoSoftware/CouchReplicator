package main

import "fmt"
import "postgresrep"
import "elasticrep"
import "redis_1"
import "time"

func main() {

	var DB, User, Password, Host, CouchHost, CouchPool, CouchBucket, XMLPath, Option, EnableDelete, RedisIP, RedisPasswd, ElasticHost, IndexName, IndexType, CouchView, ServiceURI string
	var WaitTime, NumberOfRecords int
	var RedisDB int64

	fmt.Print("Service URI(http://192.168.1.194/DuoSubscribe5/V5Services/Transformation/TransformationService.svc/Json/Retreve?Key=) : ")
	fmt.Scanf("%s\n", &ServiceURI)
	fmt.Println()

	fmt.Print("Postgres database name(ReportDB) : ")
	fmt.Scanf("%s\n", &DB)
	fmt.Println()

	fmt.Print("Postgres user name(postgres) : ")
	fmt.Scanf("%s\n", &User)
	fmt.Println()

	fmt.Print("Postgres password(password) : ")
	fmt.Scanf("%s\n", &Password)
	fmt.Println()

	fmt.Print("Postgres database host(10.10.10.10) : ")
	fmt.Scanf("%s\n", &Host)
	fmt.Println()

	fmt.Print("Couch host name(10.10.10.11) : ")
	fmt.Scanf("%s\n", &CouchHost)
	fmt.Println()

	fmt.Print("Couch pool name(default) : ")
	fmt.Scanf("%s\n", &CouchPool)
	fmt.Println()

	fmt.Print("Couch bucket name for insert(BucketInsert) : ")
	fmt.Scanf("%s\n", &CouchBucket)
	fmt.Println()

	fmt.Print("Couch view name for insert(report) : ")
	fmt.Scanf("%s\n", &CouchView)
	fmt.Println()

	fmt.Print("XML Path(c:\\pg.xml) : ")
	fmt.Scanf("%s\n", &XMLPath)
	fmt.Println()

	fmt.Print("Redis DB(0) : ")
	fmt.Scanf("%d\n", &RedisDB)
	fmt.Println()

	fmt.Print("Redis IP(tcp:10.10.10.12) : ")
	fmt.Scanf("%s\n", &RedisIP)
	fmt.Println()

	fmt.Print("Redis Password(password): ")
	fmt.Scanf("%s\n", &RedisPasswd)
	fmt.Println()

	fmt.Print("Enable delete from update bucket(true/false) : ")
	fmt.Scanf("%s\n", &EnableDelete)
	fmt.Println()

	fmt.Print("Update status checking wait time(in seconds - 10) : ")
	fmt.Scanf("%d\n", &WaitTime)
	fmt.Println()

	fmt.Print("Elastic host(192.168.1.2) : ")
	fmt.Scanf("%s\n", &ElasticHost)
	fmt.Println()

	fmt.Println("1 : InitialMigration")
	fmt.Println("2 : Updates")
	fmt.Println("3 : Continuous Update")
	fmt.Println("4 : Bulk delete from couch")
	fmt.Println("5 : Bulk Insert to Elastic")
	fmt.Scanf("%s\n", &Option)

	if Option == "1" {
		postgresrep.InitialMigrationC2PG(DB, User, Password, Host, CouchHost, CouchPool, CouchBucket, CouchView, XMLPath, EnableDelete, ServiceURI)
	} else if Option == "2" {
		err := postgresrep.UpdateC2PG(DB, User, Password, Host, CouchHost, CouchPool, CouchBucket, XMLPath, EnableDelete, RedisIP, RedisPasswd, ServiceURI, RedisDB)
		if err != nil {
			fmt.Println(err.Error())
		}
	} else if Option == "3" {
		//Cluster awarenes checking
		var redisClient *redis_1.Client
		redisClient = redis_1.NewTCPClient(&redis_1.Options{Addr:RedisIP+":6379", 
				Password: RedisPasswd, DB:RedisDB})
		nodeStatus := redisClient.Get("DTClusterStatus")	

		if nodeStatus.String() == "0" {
			fmt.Println("No other instances are running and setting lock for current instance")
			err := redisClient.Set("DTClusterStatus", "1")
			if err != nil {
				fmt.Println("Cannot aquire lock from redis")
			}
			continuousUpdate(DB, User, Password, Host, CouchHost, CouchPool, CouchBucket, XMLPath, EnableDelete, RedisIP, RedisPasswd, ServiceURI, RedisDB, WaitTime)
		} else {

			checkStatus := true
			for checkStatus == true {
				fmt.Println("Waiting ...")
				time.Sleep(time.Duration(WaitTime) * time.Second)
				fmt.Println("Checking status ...")
				check, _ := redisClient.Get("DTClusterStatus").Result()
							
				if check == "1" {
					checkStatus = true
				} else {
					checkStatus = false
					redisClient.Set("DTClusterStatus","1")
					redisClient.Close()
					continuousUpdate(DB, User, Password, Host, CouchHost, CouchPool, CouchBucket, XMLPath, EnableDelete, RedisIP, RedisPasswd, ServiceURI, RedisDB, WaitTime)
				}
			}
		}
	} else if Option == "4" {
		postgresrep.BulkDeleteFromCouch(CouchHost, CouchPool, CouchBucket, CouchView)
	} else if Option == "5" {

		fmt.Print("Number of records : ")
		fmt.Scanf("%d\n", &NumberOfRecords)
		fmt.Println()

		fmt.Print("Index Name : ")
		fmt.Scanf("%s\n", &IndexName)
		fmt.Println()

		fmt.Print("Index Type : ")
		fmt.Scanf("%s\n", &IndexType)
		fmt.Println()

		elasticrep.BulkInsert(IndexName, IndexType, ElasticHost, NumberOfRecords)
	}
}

func continuousUpdate(dbname, user, password, host, couchHost, couchPool, couchBucket, xmlPath, enableDelete, redisIp, redisPassword, ServiceURI string, redisDb int64, waitTime int) {
	continueUpdate := true

	for continueUpdate == true {
		err := postgresrep.UpdateC2PG(dbname, user, password, host, couchHost, couchPool, couchBucket, xmlPath, enableDelete, redisIp, redisPassword, ServiceURI, redisDb)
		if err != nil {
			fmt.Println(err.Error())
			continueUpdate = false
		}
		time.Sleep(time.Duration(waitTime) * time.Second)
	}
}