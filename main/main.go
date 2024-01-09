package main

import (
	"encoding/json"
	"fmt"
	"github.com/InfluxDB-client/memcache"
	"github.com/InfluxDB-client/v2"
	"log"
	"strconv"
)

const (
	MyDB     = "NOAA_water_database"
	username = "root"
	password = "12345678"
)

/*
开启InfluxDB服务
wsl
influxd
另开wsl
influx	开启shell，输入influxql查询

cd ./Desktop/fatcache-alter/fatcache-alter-main/src
./fatcache -D ../ssd -p 11212	开启fatcache，端口11212
telnet localhost 11212	从终端访问fatcache，写入get/set

set key len ts te
value

get key1 key2 ts te

*/

func main() {
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: "http://localhost:8086",
		//Username: username,
		//Password: password,
	})
	if err != nil {
		log.Fatal(err)
	}

	//var int1, int2 int64
	//int1 = math.MaxInt - 1
	//int2 = 12
	//bytes := make([]byte, 4)
	//binary.LittleEndian.PutUint64(bytes, uint64(int1))
	//fmt.Printf("%b\n", bytes)
	//fmt.Printf("%b\n", byte(int2))

	//test := 123.45
	//str := string(test)
	//strF := strconv.FormatFloat(test, 'f', -1, 64)
	//fmt.Printf("%b\n", []byte(strF))
	//fmt.Printf("%b\n", strF)
	//fmt.Printf("%f\n", []byte(strF))
	//strI := "123.45"
	//fmt.Printf("%b\n", []byte(strI))
	//buf := make([]byte, 8) // 假设转换后的字节数组长度为8字节
	//
	//binary.LittleEndian.PutUint64(buf, math.Float64bits(test))
	//fmt.Printf("%f\n", math.Float64frombits(math.Float64bits(test)))

	//queryColumns := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T03:40:00Z' AND time <= '2019-08-18T04:00:00Z' GROUP BY randtag,location"
	//qc := client.NewQuery(queryColumns, MyDB, "")
	//for i := 0; i < 100; i++ {
	//resp, err := c.Query(qc)
	//	if err != nil {
	//		fmt.Println(err)
	//	}
	//for s := range resp.Results[0].Series {
	//	for _, cc := range resp.Results[0].Series[s].Columns {
	//		fmt.Printf("%s ", cc)
	//	}
	//	fmt.Println()
	//}
	//fmt.Println(resp.ToString())
	//SM := client.GetSM(resp)
	//fmt.Println(SM)
	//SF, _ := client.GetSFSG(queryColumns)
	//fmt.Println(SF)
	//	fmt.Println()
	//}

	queryString := "SELECT COUNT(water_level) FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY time(12m),location"
	q := client.NewQuery(queryString, MyDB, "")
	response, err := c.Query(q)
	//res := response.ToString()
	//fmt.Println("res1:\n", res)

	// 和 query1 相差一分钟	00:01:00Z
	queryString2 := "SELECT COUNT(water_level) FROM h2o_feet WHERE time >= '2019-08-18T00:31:00Z' AND time <= '2019-08-18T01:00:00Z' GROUP BY time(12m),location"
	q2 := client.NewQuery(queryString2, MyDB, "")
	response2, err := c.Query(q2)
	//res2 := response2.ToString()
	//fmt.Println("res2:\n", res2)

	// 和 query2 相差一小时	01:00:00Z
	queryString3 := "SELECT COUNT(water_level) FROM h2o_feet WHERE time >= '2019-08-18T02:00:00Z' AND time <= '2019-08-18T02:30:00Z' GROUP BY time(12m),location"
	q3 := client.NewQuery(queryString3, MyDB, "")
	response3, err := c.Query(q3)
	//res3 := response3.ToString()
	//fmt.Println("res3:\n", res3)

	respTmp := client.Merge("m", response3, response2, response)
	for _, r := range respTmp {
		r.ToString()
		//fmt.Println("respTmp:\n", r.ToString())
	}

	//queryString := "SELECT index FROM h2o_quality WHERE location='coyote_creek' AND  time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY location,randtag"
	//q := client.NewQuery(queryString, MyDB, "")
	//response, err := c.Query(q)

	if err == nil && response.Error() == nil {
		//fmt.Println(response.Results)
		//fmt.Println(response.Err)
		//fmt.Println(response.Results[0])
		//fmt.Println(response.Results[0].Err)
		//fmt.Println(response.Results[0].StatementId)
		//fmt.Println(response.Results[0].Messages)
		//fmt.Println(response.Results[0].Series[0])
		//fmt.Println(response.Results[0].Series[1])
		//fmt.Println(response.Results[0].Series[0].Name)
		//fmt.Println("Tags:\t ", response.Results[0].Series[0].Tags)
		//fmt.Println("Tag value:\t ", response.Results[0].Series[0].Tags["location"])
		//fmt.Println("Partial:\t ", response.Results[0].Series[0].Partial)
		//fmt.Println("Columns:\t ", response.Results[0].Series[0].Columns)
		//fmt.Println("Values:\t ", response.Results[0].Series[0].Values)
		//fmt.Println("Value[0]:\t ", response.Results[0].Series[0].Values[0])
		//fmt.Println("Value[0][0]:\t ", response.Results[0].Series[0].Values[0][0])
		//fmt.Println("type of Value[0][0]:\t ", reflect.TypeOf(response.Results[0].Series[0].Values[0][0]))
		//fmt.Println("type of Value[0][1]:\t ", reflect.TypeOf(response.Results[0].Series[0].Values[0][1]))
	} else {
		print(response.Error())
	}

	//SM := client.GetSM(queryString, response)
	//fmt.Println("SM:\t", SM)
	//
	//SF, SG := client.GetSFSG(queryString)
	//fmt.Println("SF:\t", SF)
	//fmt.Println("SG:\t", SG)
	//
	//SPST := client.GetSPST(queryString)
	//fmt.Println("SP and ST:\t", SPST)
	//
	//Interval := client.GetInterval(queryString)
	//fmt.Println("Interval:\t", Interval)
	//
	//Fields, Aggr := client.GetSFSG(queryString)
	//fmt.Println("Fields:\t", Fields)
	//fmt.Println("Aggr:\t", Aggr)

	//semantic_segment := client.SemanticSegment(queryString, response)
	//fmt.Println("semantic segment:\t", semantic_segment)
	//
	//semantic_segment2 := client.SemanticSegment(queryString2, response2)
	//fmt.Println("semantic segment2:\t", semantic_segment2)
	//semantic_segment3 := client.SemanticSegment(queryString3, response3)
	//fmt.Println("semantic segment:\t", semantic_segment3)

	/* memcache */
	/*
		set key len ts te
		value


		get key1 key2 ts te

	*/

	num := json.Number("42.12") // 一个json.Number类型的数字
	// 将json.Number类型转换为字节数组
	byteArray, err := json.Marshal(num)
	fmt.Println(num)
	fmt.Printf("%b\n", byteArray)

	num2 := json.Number("123.45")
	byteArray2, err := json.Marshal(num2)
	fmt.Println(num2)
	fmt.Printf("%b\n", byteArray2)

	num3 := json.Number("123.456")
	byteArray3, err := json.Marshal(num3)
	fmt.Println(num3)
	fmt.Printf("%b\n", byteArray3)
	fmt.Println(string(byteArray3))
	fmt.Println(strconv.ParseFloat(string(byteArray3), 64))

	queryMemcache := "SELECT randtag,index FROM h2o_quality limit 5"
	qm := client.NewQuery(queryMemcache, MyDB, "")
	respCache, _ := c.Query(qm)

	fmt.Println(respCache.Results[0].Series[0].Partial)

	fmt.Printf("byte array:\n%s\n\n", respCache.ToByteArray())

	var str string
	str = respCache.ToString()
	fmt.Printf("To be set:\n%s\n\n", str)
	mc := memcache.New("localhost:11213")
	// 在缓存中设置值
	// todo set的Value是字节流，需要写 tostring()方法
	err = mc.Set(&memcache.Item{Key: "mykey", Value: []byte(str), Time_start: 134123, Time_end: 53421432123})
	if err != nil {
		log.Fatalf("Error setting value: %v", err)
	}

	// 从缓存中获取值
	itemValues, _, err := mc.Get("mykey mykey1", 10, 20)
	if err == memcache.ErrCacheMiss {
		log.Printf("Key not found in cache")
	} else if err != nil {
		log.Fatalf("Error getting value: %v", err)
	} else {
		//log.Printf("Value: %s", item.Value)
	}

	fmt.Printf("Get:\n")
	for i := range itemValues {
		//print(i)
		fmt.Printf("line:%d\n", i)
		print(itemValues[i])

	}

	// 在缓存中删除值
	err = mc.Delete("mykey")
	if err != nil {
		log.Fatalf("Error deleting value: %v", err)
	}
}
