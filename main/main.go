package main

import (
	"bufio"
	"fmt"
	stscache "github.com/InfluxDB-client/memcache"
	client "github.com/InfluxDB-client/v2"
	fatcache "github.com/bradfitz/gomemcache/memcache"
	"log"
	"os"
)

var c, err = client.NewHTTPClient(client.HTTPConfig{
	Addr: "http://10.170.48.244:8086",
	//Addr: "http://localhost:8086",
})

// MyDB := "test"
// 连接cache
var stscacheConn = stscache.New("localhost:11214")
var fatcacheConn = fatcache.New("localhost:11213")

func main() {

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
		client.SetToFatache(queryString)

		st, et := client.GetQueryTimeRange(queryString)
		ss := client.GetSemanticSegment(queryString)
		ss = fmt.Sprintf("%s[%d,%d]", ss, st, et)
		items, err := fatcacheConn.Get(ss)
		log.Printf("\tget:%s\n", ss)
		if err != nil {
			//log.Fatal(err)
			//log.Println("NOT GET.")
		} else {
			log.Println("\tGET.")
			log.Println("\tget byte length:", len(items.Value))
		}

	}

	// 检查是否有错误发生
	if err := scanner.Err(); err != nil {
		fmt.Println("读取文件时发生错误:", err)
	}

}
