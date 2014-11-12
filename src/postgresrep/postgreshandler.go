package postgresrep

import "database/sql"
import "fmt"
import _ "pq"
import "couchbase"
import "strings"
import "strconv"
import "log"

func InitialMigrationC2PG (dbname, user, password, host, couchHost, couchPool, couchBucket, couchViewName string) {
	
	fmt.Println("Connecting to the couch")
		client, err := couchbase.Connect("http://"+couchHost+":8091/")
		if(err != nil){
			fmt.Println("couch connection error : "+err.Error());
			log.Fatal(err)
		}
		
		pool, err := client.GetPool(couchPool)
		if(err != nil){
			fmt.Println("couch connection error : "+err.Error());
			log.Fatal(err)
		}
		
		bucket, err := pool.GetBucket(couchBucket)
		if(err != nil){
			fmt.Println("couch get bucket error : "+err.Error());
			log.Fatal(err)
		}
		
		skipCount := 0
		res, err := bucket.View(couchViewName, couchViewName, map[string]interface{}{
				    "stale": false,
				    "limit": 1,
				    "skip": 0,
				    })
		
		if(err != nil){
			fmt.Println("couch bucket view : "+err.Error());
			log.Fatal(err)
		}
		
		totalCouchRows := res.TotalRows 
		fmt.Println("Number of rows : "+ strconv.Itoa(totalCouchRows))
		
		var f interface{}
		
		db, err := sql.Open("postgres", "postgres://"+user+":"+password+"@"+host+"/"+dbname+"?sslmode=disable")
	
		if err != nil {
			fmt.Println("Postgres connectivity error : "+err.Error())
			log.Fatal(err)
		}
		
		fmt.Println("Connected to postgres database "+dbname +" with user "+user)
		
		for skipCount <= totalCouchRows {
	        
	        res, err := bucket.View(couchViewName, couchViewName, map[string]interface{}{
				    "stale": false,
				    "limit": 5000,
				    "skip": skipCount,
				    })

			if err != nil {
					fmt.Println("Couch bucket view error : "+err.Error())
					log.Fatal(err)
				}			
	        
	        for i := 0; i < 5000 ; i++ {
				fmt.Println("Getting value for key :"+res.Rows[i].ID)							
				
				bucket.Get(res.Rows[i].ID,&f)
				
				// Inserting account informations
				if(strings.Contains(res.Rows[i].ID,"duosoftware.subscriber.subscribermanagment.domainmodel.sms_accountinformation")){						
						m := f.(map[string]interface{})	
						var x float64 = m["AccountNo"].(float64)
						var accountInt int = int(x)
						result,err := db.Exec("INSERT INTO sms_accountinformation(guaccountid, accountno, gupromotionid, gudealerid, gucustid, accountclass, accountcategory, accounttype, status, createdate, displayaccountno, companyid, tenantid, viewobjectid) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",m["GUAccountID"],accountInt,m["GUPromotionID"],m["GUDealerID"],m["GUCustID"],m["AccountClass"],m["AccountCategory"],m["AccountType"],m["Status"],m["CreateDate"],m["DisplayAccountNo"],1,3,0)
						if(err != nil){
							fmt.Println("Postgres insertion error : "+err.Error())
						}else{
							fmt.Println(result.RowsAffected())
						}
						
					}else if(strings.Contains(res.Rows[i].ID,"duosoftware.subscriber.subscribermasters.profile")){ // inserting profile informations
						m := f.(map[string]interface{})
						
						var profileId float64 = m["ProfileID"].(float64)
						var profileIdInt int64 = int64(profileId)
						
						var guAddressIdInt int64 = 0
						if(m["GUAddressID"] != nil){
							var guAddressId float64 = m["GUAddressID"].(float64)
							guAddressIdInt = int64(guAddressId)
						}
						
						var guBillingIdInt int64 = 0
						if(m["GUBillingID"] != nil){
							var guBillingId float64 = m["GUBillingID"].(float64)
							guBillingIdInt = int64(guBillingId)
						}
						
						var guInstallationIdInt int64 = 0
						if(m["GUInstallationID"] != nil){
							var guInstallationId float64 = m["GUInstallationID"].(float64)
							guInstallationIdInt = int64(guInstallationId)
						}
						
						result,err := db.Exec("INSERT INTO sms_profile(profileid, profilecode, profileclass, profiletype, gender, firstname, secondname, lastname, title, phonenumber, mobilenumber, faxnumber, vatregno, email, country, guaddressid, gubillingid, guinstallationid) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)",profileIdInt, m["ProfileCode"], m["ProfileClass"], m["ProfileType"], m["Gender"], m["FirstName"], m["SecondName"], m["LastName"], m["Title"], m["Phonenumber"], m["Mobilenumber"], m["FaxNumber"], m["VatRegNo"], m["EMail"], m["Country"], guAddressIdInt, guBillingIdInt, guInstallationIdInt)
						if(err != nil){
							fmt.Println("Postgres insertion error : "+err.Error())
						}else{
							fmt.Println(result.RowsAffected())
						}
					}else if(strings.Contains(res.Rows[i].ID,"duosoftware.subscriber.billingmanagment.accountledger")){ // inserting account ledger
						m := f.(map[string]interface{})
						
						var recordStatusInt int64 = 0
						if(m["RecordStatus"] != nil){
							var recordStatus float64 = m["RecordStatus"].(float64)
							recordStatusInt = int64(recordStatus)
						}
						
						var tranTypeInt int64 = 0
						if(m["TranType"] != nil){
							var tranType float64 = m["TranType"].(float64)
							tranTypeInt = int64(tranType)
						}
						
						var ledgerIdInt int64 = 0
						if(m["LedgerID"] != nil){
							var ledgerId float64 = m["LedgerID"].(float64)
							ledgerIdInt = int64(ledgerId)
						}
						
						result,err := db.Exec("INSERT INTO sms_accountledger(balance, billed, refno, ledgercode, recordstatus, currencycode, createuser, trandate, refid, guaccountid, ledgerid, transactiontype, trantype, description, amount) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",m["Balance"], m["Billed"], m["RefNo"], m["ledgerCode"], recordStatusInt, m["CurrencyCode"], m["CreateUser"], m["TranDate"], m["RefID"], m["GuAccountID"], ledgerIdInt, m["TransactionType"], tranTypeInt, m["Description"], m["Amount"])
						if(err != nil){
							fmt.Println("Postgres insertion error : "+err.Error())
						}else{
							fmt.Println(result.RowsAffected())
						}
					}else{
						fmt.Println("skipped")
					}
        	}
	        
	        fmt.Println(strconv.Itoa(skipCount) +" data migrated")
	        skipCount += 5000
	    }
}

