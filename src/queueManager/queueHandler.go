package queueManager

import "redismq"
import "fmt"

func manageQueue (){
	testQueue := redismq.CreateBufferedQueue("192.168.1.194", "6379", "", 10, "clicks", 100)
    testQueue.Start()
	testQueue.Put("{\"ID\":\"123\",\"Type\":\"123\",\"ObjectType\":\"123\"}")
	
	consumer, err := testQueue.AddConsumer("testconsumer")
    if err != nil {
        panic(err)
    }
    package1, err := consumer.Get()
    if err != nil {
        panic(err)
    }
    err = package1.Ack()
    if err != nil {
        panic(err)
    }
    fmt.Println(package1.Payload)
	testQueue.FlushBuffer()

return	
}	