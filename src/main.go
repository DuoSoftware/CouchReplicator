package main

import "fmt"
import "postgresrep"

func main() {
	
	var DB, User, Password, Host, CouchHost, CouchPool, CouchBucketInsert, CouchBucketUpdate, CouchViewInsert, CouchViewUpdate, XMLPath, Option, EnableDelete string

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

	fmt.Print("Enable delete from update bucket : ")
	fmt.Scanf("%s\n", &EnableDelete)

	fmt.Println("1 : InitialMigration")
	fmt.Println("2 : Updates")
	fmt.Println("3 : Continuous Update")
	fmt.Scanf("%s\n", &Option)

	if Option == "1" {
		postgresrep.InitialMigrationC2PG(DB, User, Password, Host, CouchHost, CouchPool, CouchBucketInsert, CouchViewInsert, XMLPath, EnableDelete)
	} else if Option == "2" {
		err := postgresrep.UpdateC2PG(DB, User, Password, Host, CouchHost, CouchPool, CouchBucketUpdate, CouchBucketInsert, CouchViewUpdate, XMLPath, EnableDelete)
		if err != nil {
			fmt.Println(err.Error())
		}
	} else if Option == "3" {
		continueUpdate := true
		for continueUpdate == true {
			err := postgresrep.UpdateC2PG(DB, User, Password, Host, CouchHost, CouchPool, CouchBucketUpdate, CouchBucketInsert, CouchViewUpdate, XMLPath, EnableDelete)
			if err != nil {
				fmt.Println(err.Error())
			}
		}
	}
}
