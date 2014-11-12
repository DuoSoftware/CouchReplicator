package main

import "postgresrep"

func main() {
			
		postgresrep.InitialMigrationC2PG("ReportDB", "postgres", "p@ssw0rd", "192.168.1.148", "192.168.1.20", "default", "WaterBucket", "report");
	}
