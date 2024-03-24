package client

import (
	"bytes"
	"errors"
	"fmt"
	stscache "github.com/InfluxDB-client/memcache"
	"github.com/bradfitz/gomemcache/memcache"
	"log"
	"strings"
	"testing"
)

func TestResponse_ToByteArray(t *testing.T) {
	// 连接数据库
	var c, _ = NewHTTPClient(HTTPConfig{
		Addr: "http://10.170.48.244:8086",
		//Addr: "http://localhost:8086",
	})
	MyDB := "NOAA_water_database"
	//queryMemcache := "SELECT randtag,index FROM h2o_quality limit 5"
	queryMemcache := "SELECT index FROM h2o_quality WHERE location='coyote_creek' AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag"
	qm := NewQuery(queryMemcache, MyDB, "s")
	respCache, _ := c.Query(qm)

	semanticSegment := GetSemanticSegment(queryMemcache)
	respCacheByte := ResponseToByteArray(respCache, queryMemcache)

	startTime, endTime := GetResponseTimeRange(respCache)
	numOfTable := GetNumOfTable(respCache)

	fmt.Printf("byte array:\n%d\n\n", respCacheByte)

	var str string
	str = respCache.ToString()
	fmt.Printf("To be set:\n%s\n\n", str)

	err = stscacheConn.Set(&stscache.Item{Key: semanticSegment, Value: respCacheByte, Time_start: startTime, Time_end: endTime, NumOfTables: numOfTable})

	if err != nil {
		log.Fatalf("Error setting value: %v", err)
	}

	// 从缓存中获取值
	itemValues, _, err := stscacheConn.Get(semanticSegment, startTime, endTime)
	if errors.Is(err, memcache.ErrCacheMiss) {
		log.Printf("Key not found in cache")
	} else if err != nil {
		log.Fatalf("Error getting value: %v", err)
	} else {
		//log.Printf("Value: %s", item.Value)
	}

	fmt.Println("len:", len(itemValues))
	fmt.Printf("Get:\n")
	fmt.Printf("%d", itemValues)

	fmt.Printf("\nGet equals Set:%v\n", bytes.Equal(respCacheByte, itemValues[:len(itemValues)-2]))

	fmt.Println()

	/* 查询结果转换成字节数组的格式如下
		seprateSM1 len1
		values
		seprateSM2 len2
		values
		......

	seprateSM: 每张表的 tags 和整个查询的其余元数据组合成的 每张表的元数据	string，到空格符为止
	len: 每张表中数据的总字节数		int64，空格符后面的8个字节
	values: 数据，没有换行符
	*/
	// {(h2o_quality.randtag=1)}#{time[int64],index[int64]}#{(location='coyote_creek'[string])}#{empty,empty} [0 0 0 0 0 0 0 48]
	// 2019-08-18T00:06:00Z 66
	// 2019-08-18T00:18:00Z 91
	// 2019-08-18T00:24:00Z 29
	// {(h2o_quality.randtag=2)}#{time[int64],index[int64]}#{(location='coyote_creek'[string])}#{empty,empty} [0 0 0 0 0 0 0 16]
	// 2019-08-18T00:12:00Z 78
	// {(h2o_quality.randtag=3)}#{time[int64],index[int64]}#{(location='coyote_creek'[string])}#{empty,empty} [0 0 0 0 0 0 0 32]
	// 2019-08-18T00:00:00Z 85
	// 2019-08-18T00:30:00Z 75
}

func TestByteArrayToResponse(t *testing.T) {

	tests := []struct {
		name        string
		queryString string
		expected    string
	}{
		{ // 	在由字节数组转换为结果类型时，谓词中的tag会被错误当作GROUP BY tag; 要用谓词tag的话最好把它也写进GROUP BY tag，这样就能保证转换前后结果的结构一致
			name:        "one table two columns",
			queryString: "SELECT index,location FROM h2o_quality WHERE location='coyote_creek' AND  time >= '2019-08-18T00:00:00Z' GROUP BY location limit 5",
			expected: "{(h2o_quality.location=coyote_creek)}#{index[int64],location[string]}#{empty}#{empty,empty} [0 0 0 0 0 0 4 0]\r\n" +
				"[1566086400000000000 85]\r\n" +
				"[1566086760000000000 66]\r\n" +
				"......(共64条数据)",
		},
		{
			name:        "three tables two columns",
			queryString: "SELECT index FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag",
			expected: "{(h2o_quality.randtag=1)}#{index[int64]}#{empty}#{empty,empty} [0 0 0 0 0 0 0 48]\r\n" +
				"[1566086760000000000 66]\r\n" +
				"[1566087480000000000 91]\r\n" +
				"[1566087840000000000 29]\r\n" +
				"{(h2o_quality.randtag=2)}#{index[int64]}#{empty}#{empty,empty} [0 0 0 0 0 0 0 16]\r\n" +
				"[1566087120000000000 78]\r\n" +
				"{(h2o_quality.randtag=3)}#{index[int64]}#{empty}#{empty,empty} [0 0 0 0 0 0 0 32]\r\n" +
				"[1566086400000000000 85]\r\n" +
				"[1566088200000000000 75]\r\n",
		},
		{ // length of key out of range(309 bytes) 不能超过250字节?
			name:        "three tables four columns",
			queryString: "SELECT index,location,randtag FROM h2o_quality WHERE location='coyote_creek' AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location",
			expected: "{(h2o_quality.location=coyote_creek,h2o_quality.randtag=1)}#{index[int64],location[string],randtag[string]}#{empty}#{empty,empty} [0 0 0 0 0 0 0 198]\r\n" +
				"[1566086760000000000 66 coyote_creek 1]\r\n" +
				"[1566087480000000000 91 coyote_creek 1]\r\n" +
				"[1566087840000000000 29 coyote_creek 1]\r\n" +
				"{(h2o_quality.location=coyote_creek,h2o_quality.randtag=2)}#{index[int64],location[string],randtag[string]}#{empty}#{empty,empty} [0 0 0 0 0 0 0 66]\r\n" +
				"[1566087120000000000 78 coyote_creek 2]\r\n" +
				"{(h2o_quality.location=coyote_creek,h2o_quality.randtag=3)}#{index[int64],location[string],randtag[string]}#{empty}#{empty,empty} [0 0 0 0 0 0 0 132]\r\n" +
				"[1566086400000000000 85 coyote_creek 3]\r\n" +
				"[1566088200000000000 75 coyote_creek 3]\r\n",
		},
		{
			name:        "one table four columns",
			queryString: "SELECT index,location,randtag FROM h2o_quality WHERE location='coyote_creek' AND randtag='2' AND index>50 AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location",
			expected: "{(h2o_quality.location=coyote_creek,h2o_quality.randtag=2)}#{index[int64],location[string],randtag[string]}#{(randtag='2'[string])(index>50[int64])}#{empty,empty} [0 0 0 0 0 0 0 66]\r\n" +
				"[1566087120000000000 78 coyote_creek 2]\r\n",
		},
		{
			name:        "two tables four columns",
			queryString: "SELECT index,location,randtag FROM h2o_quality WHERE location='coyote_creek' AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location",
			expected: "{(h2o_quality.location=coyote_creek,h2o_quality.randtag=1)}#{index[int64],location[string],randtag[string]}#{empty}#{empty,empty} [0 0 0 0 0 0 0 198]\r\n" +
				"[1566086760000000000 66 coyote_creek 1]\r\n" +
				"[1566087480000000000 91 coyote_creek 1]\r\n" +
				"[1566087840000000000 29 coyote_creek 1]\r\n" +
				"{(h2o_quality.location=coyote_creek,h2o_quality.randtag=2)}#{index[int64],location[string],randtag[string]}#{empty}#{empty,empty} [0 0 0 0 0 0 0 66]\r\n" +
				"[1566087120000000000 78 coyote_creek 2]\r\n" +
				"{(h2o_quality.location=coyote_creek,h2o_quality.randtag=3)}#{index[int64],location[string],randtag[string]}#{empty}#{empty,empty} [0 0 0 0 0 0 0 132]\r\n" +
				"[1566086400000000000 85 coyote_creek 3]\r\n" +
				"[1566088200000000000 75 coyote_creek 3]\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 连接数据库
			var c, _ = NewHTTPClient(HTTPConfig{
				//Addr: "http://10.170.48.244:8086",
				Addr: "http://localhost:8086",
			})
			MyDB := "NOAA_water_database"

			query := NewQuery(tt.queryString, MyDB, "s")
			resp, err := c.Query(query)
			if err != nil {
				t.Errorf(err.Error())
			}

			/* Set() 存入cache */
			semanticSegment := GetSemanticSegment(tt.queryString)
			startTime, endTime := GetResponseTimeRange(resp)
			respString := resp.ToString()
			respCacheByte := ResponseToByteArray(resp, tt.queryString)
			tableNumbers := int64(len(resp.Results[0].Series))
			err = stscacheConn.Set(&stscache.Item{Key: semanticSegment, Value: respCacheByte, Time_start: startTime, Time_end: endTime, NumOfTables: tableNumbers})

			if err != nil {
				log.Fatalf("Set error: %v", err)
			}
			fmt.Println("Set successfully")

			/* Get() 从cache取出 */
			valueBytes, _, err := stscacheConn.Get(semanticSegment, startTime, endTime)
			if err == stscache.ErrCacheMiss {
				log.Printf("Key not found in cache")
			} else if err != nil {
				log.Fatalf("Error getting value: %v", err)
			}
			fmt.Println("Get successfully")

			/* 字节数组转换为结果类型 */
			respConverted := ByteArrayToResponse(valueBytes)
			fmt.Println("Convert successfully")

			if strings.Compare(respString, respConverted.ToString()) != 0 {
				t.Errorf("fail to convert:different response")
			}
			fmt.Println("Same before and after convert")

			fmt.Println("resp:\n", *resp)
			fmt.Println("resp converted:\n", *respConverted)
			fmt.Println("resp:\n", resp.ToString())
			fmt.Println("resp converted:\n", respConverted.ToString())
			fmt.Println()
			fmt.Println()
		})
	}

}

func TestBoolToByteArray(t *testing.T) {
	bvs := []bool{true, false}
	expected := [][]byte{{1}, {0}}

	for i := range bvs {
		byteArr, err := BoolToByteArray(bvs[i])
		if err != nil {
			fmt.Println(err)
		} else {
			if !bytes.Equal(byteArr, expected[i]) {
				t.Errorf("byte array%b", byteArr)
				t.Errorf("exected:%b", expected[i])
			}
			fmt.Println(byteArr)
		}
	}

}

func TestByteArrayToBool(t *testing.T) {
	expected := []bool{true, false}
	byteArray := [][]byte{{1}, {0}}

	for i := range byteArray {
		b, err := ByteArrayToBool(byteArray[i])
		if err != nil {
			fmt.Println(err)
		} else {
			if b != expected[i] {
				t.Errorf("bool:%v", b)
				t.Errorf("expected:%v", expected[i])
			}
			fmt.Println(b)
		}
	}

}

func TestStringToByteArray(t *testing.T) {
	tests := []struct {
		name     string
		str      string
		expected []byte
	}{
		{
			name:     "empty",
			str:      "",
			expected: []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name:     "normal",
			str:      "SCHEMA ",
			expected: []byte{83, 67, 72, 69, 77, 65, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name:     "white spaces",
			str:      "          ",
			expected: []byte{32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		}, {
			name:     "CRLF",
			str:      "a\r\ns\r\nd\r\n",
			expected: []byte{97, 13, 10, 115, 13, 10, 100, 13, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		}, {
			name:     "normal2",
			str:      "asd zxc",
			expected: []byte{97, 115, 100, 32, 122, 120, 99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name:     "symbols",
			str:      "-=.,/\\][()!@#$%^&*?\":",
			expected: []byte{45, 61, 46, 44, 47, 92, 93, 91, 40, 41, 33, 64, 35, 36, 37, 94, 38, 42, 63, 34, 58, 0, 0, 0, 0},
		},
		{
			name:     "length out of range(25)",
			str:      "AaaaBbbbCcccDdddEeeeFfffGggg",
			expected: []byte{65, 97, 97, 97, 66, 98, 98, 98, 67, 99, 99, 99, 68, 100, 100, 100, 69, 101, 101, 101, 70, 102, 102, 102, 71},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			byteArray := StringToByteArray(tt.str)

			if !bytes.Equal(byteArray, tt.expected) {
				t.Errorf("byte array:%d", byteArray)
				t.Errorf("expected:%b", tt.expected)
			}

			fmt.Printf("expected:%d\n", tt.expected)
		})
	}

}

func TestByteArrayToString(t *testing.T) {
	tests := []struct {
		name      string
		expected  string
		byteArray []byte
	}{
		{
			name:      "empty",
			expected:  "",
			byteArray: []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name:      "normal",
			expected:  "SCHEMA ",
			byteArray: []byte{83, 67, 72, 69, 77, 65, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name:      "white spaces",
			expected:  "          ",
			byteArray: []byte{32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		}, {
			name:      "CRLF",
			expected:  "a\r\ns\r\nd\r\n",
			byteArray: []byte{97, 13, 10, 115, 13, 10, 100, 13, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		}, {
			name:      "normal2",
			expected:  "asd zxc",
			byteArray: []byte{97, 115, 100, 32, 122, 120, 99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name:      "symbols",
			expected:  "-=.,/\\][()!@#$%^&*?\":",
			byteArray: []byte{45, 61, 46, 44, 47, 92, 93, 91, 40, 41, 33, 64, 35, 36, 37, 94, 38, 42, 63, 34, 58, 0, 0, 0, 0},
		},
		{
			name:      "length out of range(25)",
			expected:  "AaaaBbbbCcccDdddEeeeFfffG",
			byteArray: []byte{65, 97, 97, 97, 66, 98, 98, 98, 67, 99, 99, 99, 68, 100, 100, 100, 69, 101, 101, 101, 70, 102, 102, 102, 71},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			str := ByteArrayToString(tt.byteArray)

			if strings.Compare(str, tt.expected) != 0 {
				t.Errorf("string:%s", str)
				t.Errorf("expected:%s", tt.expected)
			}

			fmt.Printf("string:%s\n", str)
		})
	}
}

func TestInt64ToByteArray(t *testing.T) {
	numbers := []int64{123, 2000300, 100020003000, 10000200030004000, 101001000100101010, 9000800070006000500, 1566088200000000000}
	expected := [][]byte{
		{123, 0, 0, 0, 0, 0, 0, 0},
		{172, 133, 30, 0, 0, 0, 0, 0},
		{184, 32, 168, 73, 23, 0, 0, 0},
		{32, 163, 120, 2, 33, 135, 35, 0},
		{146, 251, 236, 220, 223, 211, 102, 1},
		{116, 203, 4, 179, 249, 67, 233, 124},
		{0, 80, 238, 159, 235, 220, 187, 21},
	}

	for i := range numbers {
		bytesArray, err := Int64ToByteArray(numbers[i])
		if err != nil {
			fmt.Errorf(err.Error())
		}
		if !bytes.Equal(bytesArray, expected[i]) {
			t.Errorf("byte array:%d", bytesArray)
			t.Errorf("expected:%d", expected[i])
		}
		//fmt.Printf("bytesArray:%d\n", bytesArray)
		//fmt.Printf("expected:%d\n", expected[i])
	}
}

func TestByteArrayToInt64(t *testing.T) {
	expected := []int64{0, 0, 123, 2000300, 100020003000, 10000200030004000, 101001000100101010, 9000800070006000500}
	byteArrays := [][]byte{
		{0, 0, 0, 0, 0, 0},
		{0, 0, 0, 0, 0, 0, 0, 0},
		{123, 0, 0, 0, 0, 0, 0, 0},
		{172, 133, 30, 0, 0, 0, 0, 0},
		{184, 32, 168, 73, 23, 0, 0, 0},
		{32, 163, 120, 2, 33, 135, 35, 0},
		{146, 251, 236, 220, 223, 211, 102, 1},
		{116, 203, 4, 179, 249, 67, 233, 124},
	}

	for i := range byteArrays {
		number, err := ByteArrayToInt64(byteArrays[i])
		if err != nil {
			fmt.Printf(err.Error())
		}
		if number != expected[i] {
			t.Errorf("number:%d", number)
			t.Errorf("expected:%d", expected[i])
		}
		fmt.Printf("number:%d\n", number)
	}

}

func TestFloat64ToByteArray(t *testing.T) {
	numbers := []float64{0, 123, 123.4, 12.34, 123.456, 1.2345, 12.34567, 123.456789, 123.4567890, 0.00}
	expected := [][]byte{
		{0, 0, 0, 0, 0, 0, 0, 0},
		{0, 0, 0, 0, 0, 192, 94, 64},
		{154, 153, 153, 153, 153, 217, 94, 64},
		{174, 71, 225, 122, 20, 174, 40, 64},
		{119, 190, 159, 26, 47, 221, 94, 64},
		{141, 151, 110, 18, 131, 192, 243, 63},
		{169, 106, 130, 168, 251, 176, 40, 64},
		{11, 11, 238, 7, 60, 221, 94, 64},
		{11, 11, 238, 7, 60, 221, 94, 64},
		{0, 0, 0, 0, 0, 0, 0, 0},
	}

	for i := range numbers {
		bytesArray, err := Float64ToByteArray(numbers[i])
		if err != nil {
			fmt.Println(err.Error())
		}
		if !bytes.Equal(bytesArray, expected[i]) {
			t.Errorf("byte array:%b", bytesArray)
			t.Errorf("expected:%b", expected[i])
		}
		//fmt.Printf("bytesArray:%d\n", bytesArray)
		//fmt.Printf("expected:%d\n", expected[i])
	}

}

func TestByteArrayToFloat64(t *testing.T) {
	expected := []float64{0, 123, 123.4, 12.34, 123.456, 1.2345, 12.34567, 123.456789, 123.4567890, 0.00, 0.0}
	byteArrays := [][]byte{
		{0, 0, 0, 0, 0, 0, 0, 0},
		{0, 0, 0, 0, 0, 192, 94, 64},
		{154, 153, 153, 153, 153, 217, 94, 64},
		{174, 71, 225, 122, 20, 174, 40, 64},
		{119, 190, 159, 26, 47, 221, 94, 64},
		{141, 151, 110, 18, 131, 192, 243, 63},
		{169, 106, 130, 168, 251, 176, 40, 64},
		{11, 11, 238, 7, 60, 221, 94, 64},
		{11, 11, 238, 7, 60, 221, 94, 64},
		{0, 0, 0, 0, 0, 0, 0, 0},
		{0, 0, 0, 0, 0, 0, 0, 0},
	}

	for i := range byteArrays {
		number, err := ByteArrayToFloat64(byteArrays[i])
		if err != nil {
			fmt.Println(err)
		}
		if number != expected[i] {
			t.Errorf("number:%f", number)
			t.Errorf("expected:%f", expected[i])
		}
		//fmt.Printf("number:%f\n", number)
	}

}

func TestTimeStringToInt64(t *testing.T) {
	timeStrings := []string{"2019-08-18T00:00:00Z", "2000-01-01T00:00:00Z", "2261-01-01T00:00:00Z"}
	expected := []int64{1566086400, 946684800, 9183110400}
	for i := range timeStrings {
		numberN := TimeStringToInt64(timeStrings[i])
		if numberN != expected[i] {
			t.Errorf("timestamp:%s", timeStrings[i])
			t.Errorf("number:%d", numberN)
		}
	}
}

func TestTimeInt64ToString(t *testing.T) {
	timeIntegers := []int64{1566086400, 946684800, 9183110400}
	expected := []string{"2019-08-18T00:00:00Z", "2000-01-01T00:00:00Z", "2261-01-01T00:00:00Z"}
	for i := range timeIntegers {
		numberStr := TimeInt64ToString(timeIntegers[i])
		if numberStr != expected[i] {
			t.Errorf("time string:%s", numberStr)
			t.Errorf("expected:%s", expected[i])
		}
	}
}
