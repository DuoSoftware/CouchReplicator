package main

import "fmt"
import "postgresrep"
import "elasticrep"
import "redis"
import "time"

func main() {

	var DB, User, Password, Host, CouchHost, CouchPool, CouchBucketInsert, CouchBucketUpdate, CouchViewInsert, CouchViewUpdate, XMLPath, Option, EnableDelete, RedisIP, RedisPasswd, ElasticHost, IndexName, IndexType string
	var WaitTime, RedisDB, NumberOfRecords int

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
	fmt.Scanf("%s\n", &CouchBucketInsert)
	fmt.Println()

	fmt.Print("Couch bucket name for update(BucketUpdate) : ")
	fmt.Scanf("%s\n", &CouchBucketUpdate)
	fmt.Println()

	fmt.Print("Couch view name for insert(report) : ")
	fmt.Scanf("%s\n", &CouchViewInsert)
	fmt.Println()

	fmt.Print("Couch view name for update(report) : ")
	fmt.Scanf("%s\n", &CouchViewUpdate)
	fmt.Println()

	fmt.Print("XML Path(c:\\pg.xml) : ")
	fmt.Scanf("%s\n", &XMLPath)
	fmt.Println()

	fmt.Print("Redis DB(0) : ")
	fmt.Scanf("%d\n", &RedisDB)
	fmt.Println()

	fmt.Print("Redis IP(tcp:10.10.10.12:6379) : ")
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
		postgresrep.InitialMigrationC2PG(DB, User, Password, Host, CouchHost, CouchPool, CouchBucketInsert, CouchViewInsert, XMLPath, EnableDelete)
	} else if Option == "2" {
		err := postgresrep.UpdateC2PG(DB, User, Password, Host, CouchHost, CouchPool, CouchBucketUpdate, CouchBucketInsert, CouchViewUpdate, XMLPath, EnableDelete)
		if err != nil {
			fmt.Println(err.Error())
		}
	} else if Option == "3" {
		//Cluster awarenes checking
		redisClient := redis.New(RedisIP, RedisDB, RedisPasswd)
		nodeStatus, err := redisClient.Get("DTClusterStatus")
		if err != nil {
			fmt.Println("Cannot continue since the redis has connectivity issue " + err.Error())
			return
		}

		if nodeStatus.String() == "0" {
			fmt.Println("No other instances are running and setting lock for current instance")
			err := redisClient.Set("DTClusterStatus", "1")
			if err != nil {
				fmt.Println("Cannot aquire lock from redis")
			}
			continuousUpdate(DB, User, Password, Host, CouchHost, CouchPool, CouchBucketUpdate, CouchBucketInsert, CouchViewUpdate, XMLPath, EnableDelete)
		} else {

			checkStatus := true
			for checkStatus == true {
				fmt.Println("Waiting ...")
				time.Sleep(time.Duration(WaitTime) * time.Second)
				fmt.Println("Checking status ...")
				check, err := redisClient.Get("DTClusterStatus")

				if err != nil {
					fmt.Println("Cannot continue since the redis has connectivity issue " + err.Error())
					return
				}

				if check.String() == "1" {
					checkStatus = true
				} else {
					checkStatus = false
					redisClient.Quit()
					continuousUpdate(DB, User, Password, Host, CouchHost, CouchPool, CouchBucketUpdate, CouchBucketInsert, CouchViewUpdate, XMLPath, EnableDelete)
				}
			}
		}
	} else if Option == "4" {
		postgresrep.BulkDeleteFromCouch(CouchHost, CouchPool, CouchBucketInsert, CouchViewInsert)
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

func continuousUpdate(DB, User, Password, Host, CouchHost, CouchPool, CouchBucketUpdate, CouchBucketInsert, CouchViewUpdate, XMLPath, EnableDelete string) {
	continueUpdate := true

	for continueUpdate == true {
		err := postgresrep.UpdateC2PG(DB, User, Password, Host, CouchHost, CouchPool, CouchBucketUpdate, CouchBucketInsert, CouchViewUpdate, XMLPath, EnableDelete)
		if err != nil {
			fmt.Println(err.Error())
			continueUpdate = false
		}
	}
}
