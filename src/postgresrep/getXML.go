package postgresrep

import "encoding/xml"
import "fmt"
import "io/ioutil"
import "os"
import "encoding/json"

	type UpdateStruct struct{
			ID       string `json:"ID"`
			Type 	 string `json:"Type"`
			ObjectType string `json:"ObjectType"`
	}


	type PGChange struct{
			XMLName xml.Name `xml:PGChange`
			ColumnName string `xml:ColumnName`
		}
	
	type PGChanges struct{
			XMLName	xml.Name `xml:"PGChanges"`
			PGChange []PGChange `xml:PGChange`
		}
	
	type NestedColumn struct{
			XMLName xml.Name `xml:NestedColumn`
			ColumnName string `xml:ColumnName`
			Fixed	int `xml:Fixed`
		}
	
	type NestedColumns struct{
		XMLName xml.Name `xml:NestedColumns`
		NestedColumn []NestedColumn `xml:NestedColumn`
	}
	
	type Table struct{
			XMLName	xml.Name `xml:"Table"`
			PGName	string	`xml:"PGName"`
			CouchName	string	`xml:"CouchName"`
			PGInsert	string	`xml:"PGInsert"`
			PGUpdate	string	`xml:"PGUpdate"`
			PGDelete	string	`xml:"PGDelete"`
			PGChange []PGChange `xml:PGChange`
			NestedColumn []NestedColumn	`xml:NestedColumn`
		}
	
	type Tables struct{
			XMLName	xml.Name `xml:"Tables"`
			Tables []Table `xml:"Table"`
		}

func GetXMLData(filePath string,file *os.File)(xmlData Tables){
	
	var tables Tables
	pg, err :=  ioutil.ReadFile(filePath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		file.WriteString(err.Error()+"\n")
			}
	err2 := xml.Unmarshal(pg, &tables)
	if err2 != nil {
		fmt.Println("Error unmarshal file:", err2)
		file.WriteString(err.Error()+"\n")
	}
	
	return tables
}

func Json2UpdateType(jsonString string) (structObject UpdateStruct) {

	var dataStruct UpdateStruct

	err := json.Unmarshal([]byte(jsonString), &dataStruct)

	if err != nil {
		fmt.Println(err.Error() + "\n")
	}

	return dataStruct
}