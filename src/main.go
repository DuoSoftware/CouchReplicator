package main
import "fmt"
import "couchbase"

func main() {
	
		fmt.Println("Connecting to the couch")
		client, err := couchbase.Connect("http://192.168.1.20:8091/")
		if(err != nil){
			fmt.Println("couch connection error : "+err.Error());
		}
		
		pool, err := client.GetPool("default")
		if(err != nil){
			fmt.Println("couch connection error : "+err.Error());
		}
		
		bucket, err := pool.GetBucket("ObjectStoreBucket")
		
		//vbm := bucket.VBServerMap()
		//fmt.Printf("     %v uses %s\n", bucket.Name, vbm.HashAlgorithm)
		
		res, err := bucket.View("dev_report", "report", map[string]interface{}{
				    "stale": false,
				    "limit": 2,
				    "skip": 0,
				    })
			var row = res.Rows[0]
    		fmt.Println(row.ID)
	}
