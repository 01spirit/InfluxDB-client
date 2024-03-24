package client

import (
	"fmt"
	"log"
	"math"
	"reflect"
	"testing"
)

func TestGetResponseTimeRange(t *testing.T) {
	tests := []struct {
		name        string
		queryString string
		expected    []int64
	}{
		{
			name:        "common situation",
			queryString: "SELECT index FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag",
			expected:    []int64{1566086400, 1566088200},
		},
		{
			name:        "no results",
			queryString: "SELECT index FROM h2o_quality WHERE time >= '2029-08-18T00:00:00Z' AND time <= '2029-08-18T00:30:00Z' GROUP BY randtag",
			expected:    []int64{math.MaxInt64, 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 连接数据库
			var c, _ = NewHTTPClient(HTTPConfig{
				Addr: "http://10.170.48.244:8086",
				//Addr: "http://localhost:8086",
			})
			MyDB := "NOAA_water_database"

			q := NewQuery(tt.queryString, MyDB, "s")
			response, err := c.Query(q)
			if err != nil {
				log.Println(err)
			}
			st, et := GetResponseTimeRange(response)
			if st != tt.expected[0] {
				t.Errorf("start time:\t%d\nexpected:\t%d", st, tt.expected[0])
			}
			if et != tt.expected[1] {
				t.Errorf("end time:\t%d\nexpected:\t%d", et, tt.expected[1])
			}
		})
	}
}

func TestGetQueryTimeRange(t *testing.T) {
	tests := []struct {
		name        string
		queryString string
		expected    []int64
	}{
		{
			name:        "1",
			queryString: "SELECT index FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time < '2019-08-18T00:30:00Z'",
			expected:    []int64{1566086400, 1566088200},
		},
		{
			name:        "2",
			queryString: "SELECT index FROM h2o_quality WHERE time > '2019-08-18T00:00:00Z'",
			expected:    []int64{1566086400, -1},
		},
		{
			name:        "3",
			queryString: "SELECT index FROM h2o_quality WHERE time < '2019-08-18T00:30:00Z'",
			expected:    []int64{-1, 1566088200},
		},
		{
			name:        "4",
			queryString: "SELECT index FROM h2o_quality WHERE time = '2019-08-18T00:00:00Z'",
			expected:    []int64{1566086400, 1566086400},
		},
		{
			name:        "5",
			queryString: "SELECT index FROM h2o_quality",
			expected:    []int64{-1, -1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			startTime, endTime := GetQueryTimeRange(tt.queryString)

			if startTime != tt.expected[0] || endTime != tt.expected[1] {
				t.Errorf("start time:\t%d\n", startTime)
				t.Errorf("expected:\t%d\n", tt.expected[0])
				t.Errorf("end time:\t%d\n", endTime)
				t.Errorf("expected:\t%d\n", tt.expected[1])
			}

		})
	}
}

func TestTimeReplace(t *testing.T) {
	tests := []struct {
		name        string
		queryString string
		expected    string
	}{
		{
			name:        "1",
			queryString: "SELECT index FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z'",
			expected:    "SELECT index FROM h2o_quality WHERE time >= ? AND time <= ?",
		},
		{
			name:        "2",
			queryString: "SELECT index FROM h2o_quality WHERE time <= '2019-08-18T00:30:00Z'",
			expected:    "SELECT index FROM h2o_quality WHERE time <= ?",
		},
		{
			name:        "3",
			queryString: "SELECT index FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z'",
			expected:    "SELECT index FROM h2o_quality WHERE time >= ?",
		},
		{
			name:        "4",
			queryString: "SELECT index FROM h2o_quality WHERE time = '2019-08-18T00:00:00Z'",
			expected:    "SELECT index FROM h2o_quality WHERE time = ?",
		},
		{
			name:        "5",
			queryString: "SELECT index FROM h2o_quality",
			expected:    "SELECT index FROM h2o_quality",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			replaced := GetQueryTemplate(tt.queryString)

			//fmt.Println(replaced)

			if replaced != tt.expected {
				t.Errorf("replaces:%s", replaced)
				t.Errorf("expected:%s", tt.expected)
			}
		})
	}
}

func TestGetFieldKeys(t *testing.T) {
	// 连接数据库
	var c, _ = NewHTTPClient(HTTPConfig{
		Addr: "http://10.170.48.244:8086",
		//Addr: "http://localhost:8086",
	})
	MyDB := "NOAA_water_database"

	fieldKeys := GetFieldKeys(c, MyDB)

	expected := make(map[string]map[string]string)
	expected["h2o_feet"] = map[string]string{"level description": "string", "water_level": "float64"}
	expected["h2o_pH"] = map[string]string{"pH": "float64"}
	expected["h2o_quality"] = map[string]string{"index": "float64"}
	expected["h2o_temperature"] = map[string]string{"degrees": "float64"}
	expected["average_temperature"] = map[string]string{"degrees": "float64"}

	fmt.Println(fieldKeys)
	fmt.Println("measurement:")
	for key, val := range fieldKeys {
		fmt.Printf("%s\n", key)
		//fmt.Println("\tfield and datatype:")
		for k, v := range val {
			fmt.Printf("\t%s:%s\n", k, v)
		}
	}

}

func TestGetTagKV(t *testing.T) {
	// 连接数据库
	var c, _ = NewHTTPClient(HTTPConfig{
		Addr: "http://10.170.48.244:8086",
		//Addr: "http://localhost:8086",
	})
	MyDB := "NOAA_water_database"

	measurementTagMap := GetTagKV(c, MyDB)

	fmt.Println(measurementTagMap.Measurement)
	for name, tagmap := range measurementTagMap.Measurement {
		fmt.Println(name) // 表名
		for i := range tagmap {
			for tagkey, tagvalue := range tagmap[i].Tag {
				fmt.Println(tagkey, tagvalue.Values) // tag key value
			}
		}
	}
	// 运行结果:
	//h2o_pH
	//location [coyote_creek santa_monica]
	//h2o_quality
	//location [coyote_creek santa_monica]
	//randtag [1 2 3]
	//h2o_temperature
	//location [coyote_creek santa_monica]
	//average_temperature
	//location [coyote_creek santa_monica]
	//h2o_feet
	//location [coyote_creek santa_monica]

}

func TestGetTagArr(t *testing.T) {
	tests := []struct {
		name        string
		queryString string
		expected    []string
	}{
		{
			name:        "one tag",
			queryString: "SELECT index FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag",
			expected:    []string{"randtag"},
		},
		{
			name:        "two tags",
			queryString: "SELECT index FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location",
			expected:    []string{"location", "randtag"},
		},
		{
			name:        "two tags in different sequence",
			queryString: "SELECT index FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY location,randtag",
			expected:    []string{"location", "randtag"},
		},
		{
			name:        "two tags with time interval",
			queryString: "SELECT COUNT(index) FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY location,time(12m),randtag",
			expected:    []string{"location", "randtag"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 连接数据库
			var c, _ = NewHTTPClient(HTTPConfig{
				Addr: "http://10.170.48.244:8086",
				//Addr: "http://localhost:8086",
			})
			MyDB := "NOAA_water_database"

			q := NewQuery(tt.queryString, MyDB, "")
			response, err := c.Query(q)
			if err != nil {
				log.Println(err)
			}
			tags := GetTagNameArr(response)
			for i := range tags {
				if tags[i] != tt.expected[i] {
					t.Errorf("tag:\t%s\nexpected:\t%s", tags[i], tt.expected[i])
				}
			}
		})
	}
}

func TestDataTypeArrayFromSF(t *testing.T) {
	sfStringArr := []string{
		"time[int64], index[int64]",
		"time[int64],index[int64],location[string],randtag[string]",
		"Int[int64],Float[float64],Bool[bool],String[string]",
	}
	expected := [][]string{
		{"int64", "int64"},
		{"int64", "int64", "string", "string"},
		{"int64", "float64", "bool", "string"},
	}

	for i := range sfStringArr {
		datatypes := GetDataTypeArrayFromSF(sfStringArr[i])
		if !reflect.DeepEqual(datatypes, expected[i]) {
			t.Errorf("datatypes:%s", datatypes)
			t.Errorf("expected:%s", expected[i])
		}
	}

}
