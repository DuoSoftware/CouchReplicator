package elasticrep

import "fmt"
import "elastigo/lib"
import "crypto/rand"
import "log"
import "flag"

func BulkInsert(indexName, indexType, elasticHost string, numberOfRecords int) {

	fmt.Println("Connecting to the elastic")
log.SetFlags(log.LstdFlags)
flag.Parse()
	c := elastigo.NewConn()
	c.RequestTracer = func(method, url, body string) {
		log.Printf("Requesting %s %s", method, url)
		log.Printf("Request body: %s", body)
	}
	c.Domain = elasticHost

	fmt.Println("Starting to insert ...")
	var continueOn = 1

	//	result, err := c.Search(indexName, indexType, nil, `{
	//	    "query" : {
	//	        "term" : { "GUOrderID" : "201405290929509501" }
	//	    }
	//	}`)

	for continueOn <= numberOfRecords {
		
		randNum := randStr(18, "number")
		
		result, err := c.Index(indexName, indexType, "duosoftware.subscriber.ordermanagment.order_"+randNum, nil, `{
  "CustType": null,
  "tag": "OrderType:Postpaid",
  "GUDeviceID": "0",
  "IsAvailable": false,
  "GUOrderID": "`+randNum+`",
  "OrderID": "3",
  "OrderDate": "2015-01-06T15:23:42.7198285+05:30",
  "GUAccountID": "15010615234251403",
  "CreateUser": "admin",
  "CreateDate": "2015-01-06T15:23:42.7198285+05:30",
  "CancelledDate": "1899-01-01T00:00:00",
  "CancelledUser": null,
  "GUTranID": "15010615234271604",
  "TenentID": 39,
  "CompanyID": 42,
  "ViewObjectID": 0,
  "ObjectID": null,
  "Status": 2,
  "OrderDetails": [
    {
      "GUPromtionID": "15010519341113508",
      "GUOrderID": "15010615234271703",
      "ChangeID": 0,
      "GUPackageID": "14100112243589402",
      "startdate": "2015-01-06T00:00:00",
      "endtdate": "9999-12-31T23:59:59.9999999",
      "UOMCode": "units",
      "Qty": "1",
      "GURatePlanID": "0",
      "PackageCatogory": 0,
      "PackageCode": "PKG_SMS",
      "Description": "SMS Solution",
      "Status": 0,
      "LineId": "0",
      "ExpectedStartDate": "2015-01-06T00:00:00",
      "ExpectedEndDate": "9999-12-31T23:59:59.9999999",
      "PackageType": "ApplicationPkg",
      "GUPackageType": null,
      "GUAccountID": "15010615234251403",
      "DtlKey": null
    }
  ],
  "OrderItemDetails": [
    {
      "ItemCatogary": null,
      "GUItemID": "0",
      "ItemCode": null,
      "GUPromotionID": "15010519341113508",
      "Manditory": null,
      "AutoRenew": 1,
      "GUInvcID": "0",
      "NumberOfBillCycle": 0,
      "Balance": "0",
      "Used": null,
      "SerialNo": null,
      "Amount": "0",
      "DicountAmount": "0",
      "DisPrecentage": "0",
      "PrintDescription": "SMS Solution",
      "BillingType": "Block",
      "OrderCount": 0,
      "PrvUOM": null,
      "PrvQty": "0",
      "LastBillDate": "2015-01-06T00:00:00",
      "GUPromtionID": "15010519341113508",
      "GUOrderID": "15010615234271703",
      "ChangeID": 0,
      "GUPackageID": "14100112243589402",
      "startdate": "2015-01-06T00:00:00",
      "endtdate": "9999-12-31T23:59:59.9999999",
      "UOMCode": "units",
      "Qty": "1",
      "GURatePlanID": "0",
      "PackageCatogory": 0,
      "PackageCode": null,
      "Description": null,
      "Status": 0,
      "LineId": "0",
      "ExpectedStartDate": "0001-01-01T00:00:00",
      "ExpectedEndDate": "0001-01-01T00:00:00",
      "PackageType": null,
      "GUPackageType": null,
      "GUAccountID": "15010615234251403",
      "DtlKey": null
    }
  ],
  "Editable": false,
  "Attribute": []
}`)

		if err != nil {
			fmt.Println("Sample search error " + err.Error())
		} else {
			fmt.Println(result.Created)
		}

		continueOn++
	}
}

func randStr(strSize int, randType string) string {

	var dictionary string

	if randType == "alphanum" {
		dictionary = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	}

	if randType == "alpha" {
		dictionary = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	}

	if randType == "number" {
		dictionary = "0123456789"
	}

	var bytes = make([]byte, strSize)
	rand.Read(bytes)
	for k, v := range bytes {
		bytes[k] = dictionary[v%byte(len(dictionary))]
	}
	return string(bytes)
}
