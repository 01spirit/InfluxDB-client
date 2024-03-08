package main

import (
	"bytes"
	"fmt"
	"github.com/InfluxDB-client/memcache"
	"github.com/InfluxDB-client/v2"
	"log"
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

	lstr := "{(h2o_quality.location=coyote_creek,h2o_quality.randtag=2)(h2o_quality.location=coyote_creek,h2o_quality.randtag=3)(h2o_quality.location=santa_monica,h2o_quality.randtag=2)(h2o_quality.location=santa_monica,h2o_quality.randtag=3)}#{time[int64],index[int64]}#{(randtag!='1'[string])(index>=50[int64])}#{1566086400000000000,1566261000000000000}#{max,12m}"
	fmt.Println(len(lstr))

	//tmp := float64(123.1234567890987654321)
	//s := strconv.FormatFloat(tmp, 'g', -1, 64)
	//jNumber := json.Number(s)
	//fmt.Println(jNumber)

	//tmp := int64(123)
	//s := strconv.FormatInt(tmp, 10)
	//jNumber := json.Number(s)
	//fmt.Println(jNumber)

	//ii := json.Number("123")
	//ff := json.Number("123.456")
	//ss := json.Number("asd")

	//d, errD := ii.Int64()
	//f, errF := ii.Float64()
	//s := ii.String()
	//d, errD := ff.Int64()
	//f, errF := ff.Float64()
	//s := ff.String()
	//fmt.Println(d, errD)
	//fmt.Println(f, errF)
	//fmt.Println(s)
	//fmt.Println()

	//var int1, int2 int64
	//int1 = math.MaxInt - 1
	//int2 = 12
	//bytes := make([]byte, 4)
	//binary.LittleEndian.PutUint64(bytes, uint64(int1))
	//fmt.Printf("%b\n", bytes)
	//fmt.Printf("%b\n", byte(int2))
	//tmp1 := int64(100200300)
	//bytesBuffer1 := bytes.NewBuffer([]byte{})
	//binary.Write(bytesBuffer1, binary.BigEndian, &tmp1)
	//fmt.Println(tmp1)
	//fmt.Printf("int64:\t%b\n", bytesBuffer1.Bytes())
	//fmt.Println("length:\t", len(bytesBuffer1.Bytes()))
	//fmt.Println("string:\t", bytesBuffer1.String())
	//var back1 int64
	//binary.Read(bytesBuffer1, binary.BigEndian, &back1)
	//fmt.Println("back:\t", back1)
	//
	//fmt.Println()

	//tmp2 := int32(100200300)
	//bytesBuffer2 := bytes.NewBuffer([]byte{})
	//binary.Write(bytesBuffer2, binary.BigEndian, &tmp2)
	//fmt.Println(tmp2)
	//fmt.Printf("int32:\t%b\n", bytesBuffer2.Bytes())
	//fmt.Println("length:\t", len(bytesBuffer2.Bytes()))
	//var back2 int32
	//binary.Read(bytesBuffer2, binary.BigEndian, &back2)
	//fmt.Println("back:\t", back2)
	//fmt.Println()
	//
	//tmp3 := int16(10020)
	//bytesBuffer3 := bytes.NewBuffer([]byte{})
	//binary.Write(bytesBuffer3, binary.BigEndian, &tmp3)
	//fmt.Println(tmp3)
	//fmt.Printf("int16:\t%b\n", bytesBuffer3.Bytes())
	//fmt.Println("length:\t", len(bytesBuffer3.Bytes()))
	//var back3 int16
	//binary.Read(bytesBuffer3, binary.BigEndian, &back3)
	//fmt.Println("back:\t", back3)
	//fmt.Println()
	//
	//tmp4 := uint8(100)
	//bytesBuffer4 := bytes.NewBuffer([]byte{})
	//binary.Write(bytesBuffer4, binary.BigEndian, &tmp4)
	//fmt.Println(tmp4)
	//fmt.Printf("uint8:\t%b\n", bytesBuffer4.Bytes())
	//fmt.Println("length:\t", len(bytesBuffer4.Bytes()))
	//var back4 uint8
	//binary.Read(bytesBuffer4, binary.BigEndian, &back4)
	//fmt.Println("back:\t", back4)
	//fmt.Println()
	//
	//tmpBoolTrue := bool(true)
	//bytesBufferBoolTrue := bytes.NewBuffer([]byte{})
	//binary.Write(bytesBufferBoolTrue, binary.BigEndian, &tmpBoolTrue)
	//fmt.Println(tmpBoolTrue)
	//fmt.Printf("BoolTrue:\t%b\n", bytesBufferBoolTrue.Bytes())
	//fmt.Println("length:\t", len(bytesBufferBoolTrue.Bytes()))
	//var backBT bool
	//binary.Read(bytesBufferBoolTrue, binary.BigEndian, &backBT)
	//fmt.Println("back:\t", backBT)
	//fmt.Println()
	//
	//tmpBoolFalse := bool(false)
	//bytesBufferBoolFalse := bytes.NewBuffer([]byte{})
	//binary.Write(bytesBufferBoolFalse, binary.BigEndian, &tmpBoolFalse)
	//fmt.Println(tmpBoolFalse)
	//fmt.Printf("BoolFalse:\t%b\n", bytesBufferBoolFalse.Bytes())
	//fmt.Println("length:\t", len(bytesBufferBoolFalse.Bytes()))
	//var backBF bool
	//binary.Read(bytesBufferBoolFalse, binary.BigEndian, &backBF)
	//fmt.Println("back:\t", backBF)
	//fmt.Println()
	//
	//tmpFloat64 := float64(123.456)
	//bytesBufferFloat64 := bytes.NewBuffer([]byte{})
	//binary.Write(bytesBufferFloat64, binary.BigEndian, &tmpFloat64)
	//fmt.Println(tmpFloat64)
	//fmt.Printf("Float64:\t%b\n", bytesBufferFloat64.Bytes())
	//fmt.Println("length:\t", len(bytesBufferFloat64.Bytes()))
	//var backF float64
	//binary.Read(bytesBufferFloat64, binary.BigEndian, &backF)
	//fmt.Println("back:\t", backF)
	//fmt.Println()
	//
	//tmpFloat64S := float64(123.456123)
	//bytesBufferFloat64S := bytes.NewBuffer([]byte{})
	//binary.Write(bytesBufferFloat64S, binary.BigEndian, &tmpFloat64S)
	//fmt.Println(tmpFloat64S)
	//fmt.Printf("Float64:\t%b\n", bytesBufferFloat64S.Bytes())
	//fmt.Println("length:\t", len(bytesBufferFloat64S.Bytes()))
	//var backFS float64
	//binary.Read(bytesBufferFloat64S, binary.BigEndian, &backFS)
	//fmt.Println("back:\t", backFS)
	//fmt.Println()
	//
	//tmpFloat64T := float64(111123.456123)
	//bytesBufferFloat64T := bytes.NewBuffer([]byte{})
	//binary.Write(bytesBufferFloat64T, binary.BigEndian, &tmpFloat64T)
	//fmt.Println(tmpFloat64T)
	//fmt.Printf("Float64:\t%b\n", bytesBufferFloat64T.Bytes())
	//fmt.Println("length:\t", len(bytesBufferFloat64T.Bytes()))
	//var backFT float64
	//binary.Read(bytesBufferFloat64T, binary.BigEndian, &backFT)
	//fmt.Println("back:\t", backFT)
	//fmt.Println()

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

	} else {
		print(response.Error())
	}

	/* memcache */
	/*
		set key len ts te
		value


		get key1 key2 ts te

	*/

	//fmt.Println(time.Hour.Nanoseconds())

	queryMemcache := "SELECT randtag,index,location FROM h2o_quality GROUP BY location limit 90"
	qm := client.NewQuery(queryMemcache, MyDB, "ns")
	respCache, _ := c.Query(qm)
	start_time, end_time := client.GetResponseTimeRange(respCache)
	numOfTab := int64(len(respCache.Results[0].Series))

	semanticSegment := client.SemanticSegment(queryMemcache, respCache)
	single_seg := client.SeperateSemanticSegment(queryMemcache, respCache)
	respCacheByte := respCache.ToByteArray(queryMemcache)
	fmt.Printf("byte array:\n%d\n\n", respCacheByte)

	//for _, i := range respCache.Results[0].Series[0].Values {
	//	num, _ := i[0].(json.Number).Int64()
	//	fmt.Println(client.Int64ToByteArray(num))
	//}
	var str string
	str = respCache.ToString()
	fmt.Printf("To be set:\n%s\n\n", str)
	mc := memcache.New("localhost:11214")

	// 在缓存中存入值
	err = mc.Set(&memcache.Item{Key: semanticSegment, Value: respCacheByte, Time_start: start_time, Time_end: end_time, NumOfTables: numOfTab})

	if err != nil {
		log.Fatalf("Error setting value: %v", err)
	} else {
		log.Printf("STORED.")
	}

	// 从缓存中获取值
	//itemValues, _, err := mc.Get(semanticSegment, start_time, end_time)

	for i := range single_seg {
		itemValues, _, err := mc.Get(single_seg[i], start_time, end_time)
		if err == memcache.ErrCacheMiss {
			log.Printf("Key not found in cache")
		} else if err != nil {
			log.Fatalf("Error getting value: %v", err)
		} else {
			log.Printf("GET.")
		}

		fmt.Println("len:", len(itemValues))
		fmt.Printf("Get:\n")
		fmt.Printf("%d", itemValues)

		//fmt.Printf("\nequal:%v\n", bytes.Equal(respCacheByte, itemValues[:len(itemValues)-2]))
		//
		respConverted := client.ByteArrayToResponse(itemValues)
		respConvertedStr := respConverted.ToString()
		respConvertedByteArray := respConverted.ToByteArray(queryMemcache)

		fmt.Println("\nconvert byte array to response string:")
		fmt.Println(respConvertedStr)
		fmt.Println("\nconvert byte array to response byte array:")
		fmt.Println(respConvertedByteArray)
		compare := bytes.Equal(respCacheByte, respConvertedByteArray)
		fmt.Println("\ncompare byte array before and after convert:", compare)
		fmt.Println("\nrespCache:\t", *respCache)
		fmt.Println("\nrespConverted:\t", *respConverted)
	}

	//itemValues, _, err := mc.Get(single_seg[0], start_time, end_time)
	//if err == memcache.ErrCacheMiss {
	//	log.Printf("Key not found in cache")
	//} else if err != nil {
	//	log.Fatalf("Error getting value: %v", err)
	//} else {
	//	log.Printf("GET.")
	//}
	//
	//fmt.Println("len:", len(itemValues))
	//fmt.Printf("Get:\n")
	//fmt.Printf("%d", itemValues)
	//
	////fmt.Printf("\nequal:%v\n", bytes.Equal(respCacheByte, itemValues[:len(itemValues)-2]))
	////
	//respConverted := client.ByteArrayToResponse(itemValues)
	//respConvertedStr := respConverted.ToString()
	//respConvertedByteArray := respConverted.ToByteArray(queryMemcache)
	//
	//fmt.Println("\nconvert byte array to response string:")
	//fmt.Println(respConvertedStr)
	//fmt.Println("\nconvert byte array to response byte array:")
	//fmt.Println(respConvertedByteArray)
	//compare := bytes.Equal(respCacheByte, respConvertedByteArray)
	//fmt.Println("\ncompare byte array before and after convert:", compare)
	//fmt.Println("\nrespCache:\t", *respCache)
	//fmt.Println("\nrespConverted:\t", *respConverted)
	//fmt.Println("\ncompare:\t", respCache.ToString() == respConverted.ToString())

	fmt.Println()

	// 在缓存中删除值
	//err = mc.Delete(semanticSegment)
	//if err != nil {
	//	log.Fatalf("Error deleting value: %v", err)
	//}
}
