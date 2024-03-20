package main

import (
	"bufio"
	"fmt"
	client "github.com/InfluxDB-client/v2"
	"github.com/bradfitz/gomemcache/memcache"
	"log"
	"os"
)

var c, err = client.NewHTTPClient(client.HTTPConfig{
	Addr: "http://10.170.48.244:8086",
	//Addr: "http://localhost:8086",
})

// 连接cache
var mc = memcache.New("localhost:11213")

//MyDB := "test"

func main() {

	//queryString := `select usage_system,usage_user,usage_guest,usage_nice,usage_guest_nice from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:00:20Z' and hostname='host_0'`
	//qs := client.NewQuery(queryString, MyDB, "ns")
	//resp, _ := c.Query(qs)
	//
	//semanticSegment := client.GetSemanticSegment(queryString)
	//respCacheByte := resp.ToByteArray(queryString)
	//fmt.Println(resp.ToString())
	//
	//if err != nil {
	//	log.Fatal(err)
	//}
	//
	//mc := memcache.New("localhost:11213")
	//
	//err = mc.Set(&memcache.Item{Key: semanticSegment, Value: respCacheByte})
	//if err != nil {
	//	log.Fatalf("Error setting value: %v", err)
	//} else {
	//	log.Printf("STORED.")
	//}
	//
	//values, err := mc.Get(semanticSegment)
	//if err == memcache.ErrCacheMiss {
	//	log.Printf("Key not found in cache")
	//} else if err != nil {
	//	log.Fatalf("Error getting value: %v", err)
	//} else {
	//	log.Printf("GET.")
	//}
	//
	//convertedResp := client.ByteArrayToResponse(values.Value)
	//fmt.Println(convertedResp.ToString())

	file, err := os.Open("C:\\Users\\DELL\\Desktop\\workloads.txt")
	if err != nil {
		fmt.Println("打开文件时发生错误:", err)
		return
	}
	defer file.Close()

	// 使用 bufio 包创建一个新的 Scanner 对象
	scanner := bufio.NewScanner(file)

	queryString := ""
	// 逐行读取文件内容并输出
	for scanner.Scan() {
		//fmt.Println(scanner.Text())
		queryString = scanner.Text()
		client.SetToCache(queryString)

		st, et := client.GetQueryTimeRange(queryString)
		et += 1
		ss := client.GetSemanticSegment(queryString)
		ss = fmt.Sprintf("%s[%d,%d]", ss, st, et)
		items, err := mc.Get(ss)
		log.Printf("get:%s\n", ss)
		if err != nil {
			//log.Fatal(err)
			//log.Println("NOT GET.")
		} else {
			log.Println("GET.")
			fmt.Println(items.Value)
		}

	}

	// 检查是否有错误发生
	if err := scanner.Err(); err != nil {
		fmt.Println("读取文件时发生错误:", err)
	}

}
