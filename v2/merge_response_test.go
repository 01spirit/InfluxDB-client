package client

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// 构造查询时 调用 NewQuery()，如果不指定 precision ,查询结果的时间戳是 RFC3339 格式的字符串；指定为 s，查询结果的时间戳是以秒为单位的 int64；指定为 ns，查询结果的时间戳是以纳秒为单位的 int64

func TestSortResponseWithTimeRange(t *testing.T) {
	// 连接数据库
	var c, _ = NewHTTPClient(HTTPConfig{
		//Addr: "http://10.170.48.244:8086",
		Addr: "http://localhost:8086",
	})
	MyDB := "NOAA_water_database"

	queryString1 := "SELECT COUNT(water_level) FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY time(12m),location"
	q := NewQuery(queryString1, MyDB, "")
	response1, _ := c.Query(q)
	st1, et1 := GetResponseTimeRange(response1)

	// 和 query1 相差一分钟	00:01:00Z
	queryString2 := "SELECT COUNT(water_level) FROM h2o_feet WHERE time >= '2019-08-18T00:31:00Z' AND time <= '2019-08-18T01:00:00Z' GROUP BY time(12m),location"
	q2 := NewQuery(queryString2, MyDB, "")
	response2, _ := c.Query(q2)
	st2, et2 := GetResponseTimeRange(response2)

	// 和 query2 相差一小时	01:00:00Z
	queryString3 := "SELECT COUNT(water_level) FROM h2o_feet WHERE time >= '2019-08-18T02:00:00Z' AND time <= '2019-08-18T02:30:00Z' GROUP BY time(12m),location"
	q3 := NewQuery(queryString3, MyDB, "")
	response3, _ := c.Query(q3)
	st3, et3 := GetResponseTimeRange(response3)

	tests := []struct {
		name     string
		rts      []RespWithTimeRange
		expected []RespWithTimeRange
	}{
		{
			name:     "fake time",
			rts:      []RespWithTimeRange{{nil, 1, 5}, {nil, 90, 100}, {nil, 30, 50}, {nil, 10, 20}, {nil, 6, 9}},
			expected: []RespWithTimeRange{{nil, 1, 5}, {nil, 6, 9}, {nil, 10, 20}, {nil, 30, 50}, {nil, 90, 100}},
		},
		{
			name:     "real Responses fake time",
			rts:      []RespWithTimeRange{{response2, 90, 100}, {response1, 30, 50}, {response3, 1, 5}},
			expected: []RespWithTimeRange{{response3, 1, 5}, {response1, 30, 50}, {response2, 90, 100}},
		},
		{
			name:     " 3 1 2 ",
			rts:      []RespWithTimeRange{{response3, st3, et3}, {response1, st1, et1}, {response2, st2, et2}},
			expected: []RespWithTimeRange{{response1, st1, et1}, {response2, st2, et2}, {response3, st3, et3}},
		},
		{
			name:     " 3 2 1 ",
			rts:      []RespWithTimeRange{{response3, st3, et3}, {response2, st2, et2}, {response1, st1, et1}},
			expected: []RespWithTimeRange{{response1, st1, et1}, {response2, st2, et2}, {response3, st3, et3}},
		},
		{
			name:     " 2 3 1 ",
			rts:      []RespWithTimeRange{{response2, st2, et2}, {response3, st3, et3}, {response1, st1, et1}},
			expected: []RespWithTimeRange{{response1, st1, et1}, {response2, st2, et2}, {response3, st3, et3}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sorted := SortResponseWithTimeRange(tt.rts)
			for i := range sorted {
				if sorted[i] != tt.expected[i] {
					t.Error("sorted:\t", sorted)
					t.Error("expected:\t", tt.expected)
					break
				}
			}
		})
	}
}

func TestSortResponseWithTimeRange2(t *testing.T) {
	// 连接数据库
	var c, _ = NewHTTPClient(HTTPConfig{
		//Addr: "http://10.170.48.244:8086",
		Addr: "http://localhost:8086",
	})
	MyDB := "NOAA_water_database"

	queryString1 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:10:00Z' GROUP BY randtag,location"
	q1 := NewQuery(queryString1, MyDB, "")
	response1, _ := c.Query(q1)
	st1, et1 := GetResponseTimeRange(response1)
	rwtr1 := RespWithTimeRange{response1, st1, et1}

	queryString2 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:15:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location"
	q2 := NewQuery(queryString2, MyDB, "")
	response2, _ := c.Query(q2)
	st2, et2 := GetResponseTimeRange(response2)
	rwtr2 := RespWithTimeRange{response2, st2, et2}

	queryString3 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T01:31:00Z' AND time <= '2019-08-18T01:40:00Z' GROUP BY randtag,location"
	q3 := NewQuery(queryString3, MyDB, "")
	response3, _ := c.Query(q3)
	st3, et3 := GetResponseTimeRange(response3)
	rwtr3 := RespWithTimeRange{response3, st3, et3}

	queryString4 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T03:31:00Z' AND time <= '2019-08-18T03:40:00Z' GROUP BY randtag,location"
	q4 := NewQuery(queryString4, MyDB, "")
	response4, _ := c.Query(q4)
	st4, et4 := GetResponseTimeRange(response4)
	rwtr4 := RespWithTimeRange{response4, st4, et4}

	queryString5 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T03:40:00Z' AND time <= '2019-08-18T04:00:00Z' GROUP BY randtag,location"
	q5 := NewQuery(queryString5, MyDB, "")
	response5, _ := c.Query(q5)
	st5, et5 := GetResponseTimeRange(response5)
	rwtr5 := RespWithTimeRange{response5, st5, et5}

	tests := []struct {
		name     string
		rts      []RespWithTimeRange
		expected []RespWithTimeRange
	}{
		{
			name:     " 1 2 3 4 5 ",
			rts:      []RespWithTimeRange{rwtr1, rwtr2, rwtr3, rwtr4, rwtr5},
			expected: []RespWithTimeRange{rwtr1, rwtr2, rwtr3, rwtr4, rwtr5},
		},
		{
			name:     " 3 1 2 5 4 ",
			rts:      []RespWithTimeRange{rwtr3, rwtr1, rwtr2, rwtr5, rwtr4},
			expected: []RespWithTimeRange{rwtr1, rwtr2, rwtr3, rwtr4, rwtr5},
		},
		{
			name:     " 5 4 3 2 1 ",
			rts:      []RespWithTimeRange{rwtr5, rwtr4, rwtr3, rwtr2, rwtr1},
			expected: []RespWithTimeRange{rwtr1, rwtr2, rwtr3, rwtr4, rwtr5},
		},
		{
			name:     " 4 1 5 3 2 ",
			rts:      []RespWithTimeRange{rwtr4, rwtr1, rwtr5, rwtr3, rwtr2},
			expected: []RespWithTimeRange{rwtr1, rwtr2, rwtr3, rwtr4, rwtr5},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sorted := SortResponseWithTimeRange(tt.rts)
			for i := range sorted {
				if sorted[i] != tt.expected[i] {
					t.Error("sorted:\t", sorted)
					t.Error("expected:\t", tt.expected)
					break
				}
			}
		})
	}
}

func TestSortResponses(t *testing.T) {
	// 连接数据库
	var c, _ = NewHTTPClient(HTTPConfig{
		//Addr: "http://10.170.48.244:8086",
		Addr: "http://localhost:8086",
	})
	MyDB := "NOAA_water_database"

	queryString1 := "SELECT COUNT(water_level) FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY time(12m),location"
	q := NewQuery(queryString1, MyDB, "")
	response1, _ := c.Query(q)

	// 和 query1 相差一分钟	00:01:00Z
	queryString2 := "SELECT COUNT(water_level) FROM h2o_feet WHERE time >= '2019-08-18T00:31:00Z' AND time <= '2019-08-18T01:00:00Z' GROUP BY time(12m),location"
	q2 := NewQuery(queryString2, MyDB, "")
	response2, _ := c.Query(q2)

	// 和 query2 相差一小时	01:00:00Z
	queryString3 := "SELECT COUNT(water_level) FROM h2o_feet WHERE time >= '2019-08-18T02:00:00Z' AND time <= '2019-08-18T02:30:00Z' GROUP BY time(12m),location"
	q3 := NewQuery(queryString3, MyDB, "")
	response3, _ := c.Query(q3)

	var responseNil *Response
	responseNil = nil

	query1 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag"
	nq1 := NewQuery(query1, MyDB, "")
	resp1, _ := c.Query(nq1)

	// 1 min
	query2 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:31:00Z' AND time <= '2019-08-18T01:00:00Z' GROUP BY randtag"
	nq2 := NewQuery(query2, MyDB, "")
	resp2, _ := c.Query(nq2)

	// 0.5 h
	query3 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T01:31:00Z' AND time <= '2019-08-18T02:00:00Z' GROUP BY randtag"
	nq3 := NewQuery(query3, MyDB, "")
	resp3, _ := c.Query(nq3)

	// 1 h
	query4 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T03:00:00Z' AND time <= '2019-08-18T04:00:00Z' GROUP BY randtag"
	nq4 := NewQuery(query4, MyDB, "")
	resp4, _ := c.Query(nq4)

	// 1 s
	query5 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T04:00:01Z' AND time <= '2019-08-18T04:30:00Z' GROUP BY randtag"
	nq5 := NewQuery(query5, MyDB, "")
	resp5, _ := c.Query(nq5)

	tests := []struct {
		name     string
		resps    []*Response
		expected []*Response
	}{
		{
			name:     " 3 2 1 ",
			resps:    []*Response{response3, response2, response1},
			expected: []*Response{response1, response2, response3},
		},
		{
			name:     " 1 2 3 ",
			resps:    []*Response{response1, response2, response3},
			expected: []*Response{response1, response2, response3},
		},
		{
			name:     " 3 1 2 ",
			resps:    []*Response{response3, response1, response2},
			expected: []*Response{response1, response2, response3},
		},
		{
			name:     " 2 3 1 ",
			resps:    []*Response{response2, response3, response1},
			expected: []*Response{response1, response2, response3},
		},
		{
			name:     " 2 3 1 nil ",
			resps:    []*Response{response2, response3, response1, responseNil},
			expected: []*Response{response1, response2, response3},
		},
		{
			name:     " 3 nil 2 1 ",
			resps:    []*Response{response3, responseNil, response2, response1},
			expected: []*Response{response1, response2, response3},
		},
		/**/
		{
			name:     " 5 2 4 nil 1 3 ",
			resps:    []*Response{resp5, resp2, resp4, responseNil, resp1, resp3},
			expected: []*Response{resp1, resp2, resp3, resp4, resp5},
		},
		{
			name:     " 3 1 2 5 4 ",
			resps:    []*Response{resp3, resp1, resp2, resp5, resp4},
			expected: []*Response{resp1, resp2, resp3, resp4, resp5},
		},
		{
			name:     " 5 4 3 2 1 ",
			resps:    []*Response{resp5, resp4, resp3, resp2, resp1},
			expected: []*Response{resp1, resp2, resp3, resp4, resp5},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sorted := SortResponses(tt.resps)
			for i := range sorted {
				if sorted[i] != tt.expected[i] {
					t.Error("sorted:\t", sorted)
					t.Error("expected:\t", tt.expected)
					break
				}
			}
		})
	}
}

func TestSortResponses2(t *testing.T) {
	// 连接数据库
	var c, _ = NewHTTPClient(HTTPConfig{
		//Addr: "http://10.170.48.244:8086",
		Addr: "http://localhost:8086",
	})
	MyDB := "NOAA_water_database"

	queryString1 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:10:00Z' GROUP BY randtag,location"
	q1 := NewQuery(queryString1, MyDB, "")
	resp1, _ := c.Query(q1)

	queryString2 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:15:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location"
	q2 := NewQuery(queryString2, MyDB, "")
	resp2, _ := c.Query(q2)

	queryString3 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T01:31:00Z' AND time <= '2019-08-18T01:40:00Z' GROUP BY randtag,location"
	q3 := NewQuery(queryString3, MyDB, "")
	resp3, _ := c.Query(q3)

	queryString4 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T03:31:00Z' AND time <= '2019-08-18T03:40:00Z' GROUP BY randtag,location"
	q4 := NewQuery(queryString4, MyDB, "")
	resp4, _ := c.Query(q4)

	queryString5 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T03:40:00Z' AND time <= '2019-08-18T04:00:00Z' GROUP BY randtag,location"
	q5 := NewQuery(queryString5, MyDB, "")
	resp5, _ := c.Query(q5)

	var respNil *Response
	respNil = nil

	tests := []struct {
		name     string
		resps    []*Response
		expected []*Response
	}{
		{
			name:     " 5 nil 2 4 nil 1 3 ",
			resps:    []*Response{resp5, respNil, resp2, resp4, respNil, resp1, resp3},
			expected: []*Response{resp1, resp2, resp3, resp4, resp5},
		},
		{
			name:     " 1 2 3 4 5 ",
			resps:    []*Response{resp1, resp2, resp3, resp4, resp5},
			expected: []*Response{resp1, resp2, resp3, resp4, resp5},
		},
		{
			name:     " 5 4 3 2 1 ",
			resps:    []*Response{resp5, resp4, resp3, resp2, resp1},
			expected: []*Response{resp1, resp2, resp3, resp4, resp5},
		},
		{
			name:     " 3 5 1 4 2 ",
			resps:    []*Response{resp3, resp5, resp1, resp4, resp2},
			expected: []*Response{resp1, resp2, resp3, resp4, resp5},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sorted := SortResponses(tt.resps)
			for i := range sorted {
				if sorted[i] != tt.expected[i] {
					t.Error("sorted:\t", sorted)
					t.Error("expected:\t", tt.expected)
					break
				}
			}
		})
	}
}

func TestMergeResultTable(t *testing.T) {
	// 连接数据库
	var c, _ = NewHTTPClient(HTTPConfig{
		//Addr: "http://10.170.48.244:8086",
		Addr: "http://localhost:8086",
	})
	MyDB := "NOAA_water_database"

	query1 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location"
	nq1 := NewQuery(query1, MyDB, "")
	resp1, _ := c.Query(nq1)
	resp1.ToString()
	//SCHEMA time index location randtag location=coyote_creek randtag=1
	//2019-08-18T00:06:00Z 66 coyote_creek 1
	//2019-08-18T00:18:00Z 91 coyote_creek 1
	//2019-08-18T00:24:00Z 29 coyote_creek 1
	//SCHEMA time index location randtag location=coyote_creek randtag=2
	//2019-08-18T00:12:00Z 78 coyote_creek 2
	//SCHEMA time index location randtag location=coyote_creek randtag=3
	//2019-08-18T00:00:00Z 85 coyote_creek 3
	//2019-08-18T00:30:00Z 75 coyote_creek 3
	//SCHEMA time index location randtag location=santa_monica randtag=1
	//2019-08-18T00:06:00Z 67 santa_monica 1
	//2019-08-18T00:18:00Z 14 santa_monica 1
	//SCHEMA time index location randtag location=santa_monica randtag=2
	//2019-08-18T00:00:00Z 11 santa_monica 2
	//2019-08-18T00:12:00Z 91 santa_monica 2
	//2019-08-18T00:30:00Z 79 santa_monica 2
	//SCHEMA time index location randtag location=santa_monica randtag=3
	//2019-08-18T00:24:00Z 44 santa_monica 3
	//end

	// 1 min
	query2 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:31:00Z' AND time <= '2019-08-18T01:00:00Z' GROUP BY randtag,location"
	nq2 := NewQuery(query2, MyDB, "")
	resp2, _ := c.Query(nq2)
	resp2.ToString()
	//SCHEMA time index location randtag location=coyote_creek randtag=1
	//2019-08-18T00:42:00Z 55 coyote_creek 1
	//SCHEMA time index location randtag location=coyote_creek randtag=3
	//2019-08-18T00:36:00Z 33 coyote_creek 3
	//2019-08-18T00:48:00Z 29 coyote_creek 3
	//2019-08-18T00:54:00Z 94 coyote_creek 3
	//2019-08-18T01:00:00Z 16 coyote_creek 3
	//SCHEMA time index location randtag location=santa_monica randtag=1
	//2019-08-18T00:36:00Z 25 santa_monica 1
	//2019-08-18T00:42:00Z 10 santa_monica 1
	//2019-08-18T00:48:00Z 7 santa_monica 1
	//SCHEMA time index location randtag location=santa_monica randtag=2
	//2019-08-18T01:00:00Z 83 santa_monica 2
	//SCHEMA time index location randtag location=santa_monica randtag=3
	//2019-08-18T00:54:00Z 27 santa_monica 3
	//end

	// 0.5 h
	query3 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T01:31:00Z' AND time <= '2019-08-18T02:00:00Z' GROUP BY randtag,location"
	nq3 := NewQuery(query3, MyDB, "")
	resp3, _ := c.Query(nq3)
	fmt.Println(resp3)

	// 1 h
	query4 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T03:00:00Z' AND time <= '2019-08-18T04:00:00Z' GROUP BY randtag,location"
	nq4 := NewQuery(query4, MyDB, "")
	resp4, _ := c.Query(nq4)
	fmt.Println(resp4)

	// 1 s
	query5 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T04:00:01Z' AND time <= '2019-08-18T04:30:00Z' GROUP BY randtag,location"
	nq5 := NewQuery(query5, MyDB, "")
	resp5, _ := c.Query(nq5)
	fmt.Println(resp5)

	tests := []struct {
		name        string
		queryString []string
		expected    string
	}{
		{
			name: " 1 2 ",
			queryString: []string{
				"SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location",
				"SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:31:00Z' AND time <= '2019-08-18T01:00:00Z' GROUP BY randtag,location",
			},
			expected: "SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
				"2019-08-18T00:06:00Z 66 coyote_creek 1 \r\n" +
				"2019-08-18T00:18:00Z 91 coyote_creek 1 \r\n" +
				"2019-08-18T00:24:00Z 29 coyote_creek 1 \r\n" +
				"2019-08-18T00:42:00Z 55 coyote_creek 1 \r\n" +
				"SCHEMA time index location randtag location=coyote_creek randtag=2 \r\n" +
				"2019-08-18T00:12:00Z 78 coyote_creek 2 \r\n" +
				"SCHEMA time index location randtag location=coyote_creek randtag=3 \r\n" +
				"2019-08-18T00:00:00Z 85 coyote_creek 3 \r\n" +
				"2019-08-18T00:30:00Z 75 coyote_creek 3 \r\n" +
				"2019-08-18T00:36:00Z 33 coyote_creek 3 \r\n" +
				"2019-08-18T00:48:00Z 29 coyote_creek 3 \r\n" +
				"2019-08-18T00:54:00Z 94 coyote_creek 3 \r\n" +
				"2019-08-18T01:00:00Z 16 coyote_creek 3 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=1 \r\n" +
				"2019-08-18T00:06:00Z 67 santa_monica 1 \r\n" +
				"2019-08-18T00:18:00Z 14 santa_monica 1 \r\n" +
				"2019-08-18T00:36:00Z 25 santa_monica 1 \r\n" +
				"2019-08-18T00:42:00Z 10 santa_monica 1 \r\n" +
				"2019-08-18T00:48:00Z 7 santa_monica 1 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=2 \r\n" +
				"2019-08-18T00:00:00Z 11 santa_monica 2 \r\n" +
				"2019-08-18T00:12:00Z 91 santa_monica 2 \r\n" +
				"2019-08-18T00:30:00Z 79 santa_monica 2 \r\n" +
				"2019-08-18T01:00:00Z 83 santa_monica 2 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
				"2019-08-18T00:24:00Z 44 santa_monica 3 \r\n" +
				"2019-08-18T00:54:00Z 27 santa_monica 3 \r\n" +
				"end",
		},
		{
			name: " 2 1 ",
			queryString: []string{
				"SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:31:00Z' AND time <= '2019-08-18T01:00:00Z' GROUP BY randtag,location",
				"SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location",
			},
			expected: "SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
				"2019-08-18T00:42:00Z 55 coyote_creek 1 \r\n" +
				"2019-08-18T00:06:00Z 66 coyote_creek 1 \r\n" +
				"2019-08-18T00:18:00Z 91 coyote_creek 1 \r\n" +
				"2019-08-18T00:24:00Z 29 coyote_creek 1 \r\n" +
				"SCHEMA time index location randtag location=coyote_creek randtag=2 \r\n" +
				"2019-08-18T00:12:00Z 78 coyote_creek 2 \r\n" +
				"SCHEMA time index location randtag location=coyote_creek randtag=3 \r\n" +
				"2019-08-18T00:36:00Z 33 coyote_creek 3 \r\n" +
				"2019-08-18T00:48:00Z 29 coyote_creek 3 \r\n" +
				"2019-08-18T00:54:00Z 94 coyote_creek 3 \r\n" +
				"2019-08-18T01:00:00Z 16 coyote_creek 3 \r\n" +
				"2019-08-18T00:00:00Z 85 coyote_creek 3 \r\n" +
				"2019-08-18T00:30:00Z 75 coyote_creek 3 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=1 \r\n" +
				"2019-08-18T00:36:00Z 25 santa_monica 1 \r\n" +
				"2019-08-18T00:42:00Z 10 santa_monica 1 \r\n" +
				"2019-08-18T00:48:00Z 7 santa_monica 1 \r\n" +
				"2019-08-18T00:06:00Z 67 santa_monica 1 \r\n" +
				"2019-08-18T00:18:00Z 14 santa_monica 1 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=2 \r\n" +
				"2019-08-18T01:00:00Z 83 santa_monica 2 \r\n" +
				"2019-08-18T00:00:00Z 11 santa_monica 2 \r\n" +
				"2019-08-18T00:12:00Z 91 santa_monica 2 \r\n" +
				"2019-08-18T00:30:00Z 79 santa_monica 2 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
				"2019-08-18T00:54:00Z 27 santa_monica 3 \r\n" +
				"2019-08-18T00:24:00Z 44 santa_monica 3 \r\n" +
				"end",
		},
		{
			name: " 2 1 without GROUP BY ",
			queryString: []string{
				"SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:31:00Z' AND time <= '2019-08-18T01:00:00Z'",
				//SCHEMA time index location randtag
				//2019-08-18T00:36:00Z 33 coyote_creek 3
				//2019-08-18T00:36:00Z 25 santa_monica 1
				//2019-08-18T00:42:00Z 55 coyote_creek 1
				//2019-08-18T00:42:00Z 10 santa_monica 1
				//2019-08-18T00:48:00Z 29 coyote_creek 3
				//2019-08-18T00:48:00Z 7 santa_monica 1
				//2019-08-18T00:54:00Z 94 coyote_creek 3
				//2019-08-18T00:54:00Z 27 santa_monica 3
				//2019-08-18T01:00:00Z 16 coyote_creek 3
				//2019-08-18T01:00:00Z 83 santa_monica 2
				//end
				"SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z'",
				//SCHEMA time index location randtag
				//2019-08-18T00:00:00Z 11 santa_monica 2
				//2019-08-18T00:00:00Z 85 coyote_creek 3
				//2019-08-18T00:06:00Z 66 coyote_creek 1
				//2019-08-18T00:06:00Z 67 santa_monica 1
				//2019-08-18T00:12:00Z 78 coyote_creek 2
				//2019-08-18T00:12:00Z 91 santa_monica 2
				//2019-08-18T00:18:00Z 91 coyote_creek 1
				//2019-08-18T00:18:00Z 14 santa_monica 1
				//2019-08-18T00:24:00Z 29 coyote_creek 1
				//2019-08-18T00:24:00Z 44 santa_monica 3
				//2019-08-18T00:30:00Z 79 santa_monica 2
				//2019-08-18T00:30:00Z 75 coyote_creek 3
				//end
			},
			expected: "SCHEMA time index location randtag \r\n" +
				"2019-08-18T00:36:00Z 33 coyote_creek 3 \r\n" +
				"2019-08-18T00:36:00Z 25 santa_monica 1 \r\n" +
				"2019-08-18T00:42:00Z 55 coyote_creek 1 \r\n" +
				"2019-08-18T00:42:00Z 10 santa_monica 1 \r\n" +
				"2019-08-18T00:48:00Z 29 coyote_creek 3 \r\n" +
				"2019-08-18T00:48:00Z 7 santa_monica 1 \r\n" +
				"2019-08-18T00:54:00Z 94 coyote_creek 3 \r\n" +
				"2019-08-18T00:54:00Z 27 santa_monica 3 \r\n" +
				"2019-08-18T01:00:00Z 16 coyote_creek 3 \r\n" +
				"2019-08-18T01:00:00Z 83 santa_monica 2 \r\n" +
				"2019-08-18T00:00:00Z 11 santa_monica 2 \r\n" +
				"2019-08-18T00:00:00Z 85 coyote_creek 3 \r\n" +
				"2019-08-18T00:06:00Z 66 coyote_creek 1 \r\n" +
				"2019-08-18T00:06:00Z 67 santa_monica 1 \r\n" +
				"2019-08-18T00:12:00Z 78 coyote_creek 2 \r\n" +
				"2019-08-18T00:12:00Z 91 santa_monica 2 \r\n" +
				"2019-08-18T00:18:00Z 91 coyote_creek 1 \r\n" +
				"2019-08-18T00:18:00Z 14 santa_monica 1 \r\n" +
				"2019-08-18T00:24:00Z 29 coyote_creek 1 \r\n" +
				"2019-08-18T00:24:00Z 44 santa_monica 3 \r\n" +
				"2019-08-18T00:30:00Z 79 santa_monica 2 \r\n" +
				"2019-08-18T00:30:00Z 75 coyote_creek 3 \r\n" +
				"end",
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
			q1 := NewQuery(tt.queryString[0], MyDB, "")
			resp1, _ := c.Query(q1)
			q2 := NewQuery(tt.queryString[1], MyDB, "")
			resp2, _ := c.Query(q2)
			resp := MergeResultTable(resp1, resp2)
			if resp.ToString() != tt.expected {
				t.Error("merged resp:\t", resp.ToString())
				t.Error("expected:\t", tt.expected)
			}
		})
	}
}

func TestMergeResultTable2(t *testing.T) {

	queryString1 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:10:00Z' GROUP BY randtag,location"
	//SCHEMA time index location randtag location=coyote_creek randtag=1
	//2019-08-18T00:06:00Z 66 coyote_creek 1
	//SCHEMA time index location randtag location=coyote_creek randtag=3
	//2019-08-18T00:00:00Z 85 coyote_creek 3
	//SCHEMA time index location randtag location=santa_monica randtag=1
	//2019-08-18T00:06:00Z 67 santa_monica 1
	//SCHEMA time index location randtag location=santa_monica randtag=2
	//2019-08-18T00:00:00Z 11 santa_monica 2
	//end
	queryString2 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:15:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location"
	//SCHEMA time index location randtag location=coyote_creek randtag=1
	//2019-08-18T00:18:00Z 91 coyote_creek 1
	//2019-08-18T00:24:00Z 29 coyote_creek 1
	//SCHEMA time index location randtag location=coyote_creek randtag=3
	//2019-08-18T00:30:00Z 75 coyote_creek 3
	//SCHEMA time index location randtag location=santa_monica randtag=1
	//2019-08-18T00:18:00Z 14 santa_monica 1
	//SCHEMA time index location randtag location=santa_monica randtag=2
	//2019-08-18T00:30:00Z 79 santa_monica 2
	//SCHEMA time index location randtag location=santa_monica randtag=3
	//2019-08-18T00:24:00Z 44 santa_monica 3
	//end
	queryString3 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T01:31:00Z' AND time <= '2019-08-18T01:40:00Z' GROUP BY randtag,location"
	//SCHEMA time index location randtag location=coyote_creek randtag=1
	//2019-08-18T01:36:00Z 71 coyote_creek 1
	//SCHEMA time index location randtag location=santa_monica randtag=3
	//2019-08-18T01:36:00Z 75 santa_monica 3
	//end
	queryString4 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T03:31:00Z' AND time <= '2019-08-18T03:40:00Z' GROUP BY randtag,location"
	//SCHEMA time index location randtag location=coyote_creek randtag=2
	//2019-08-18T03:36:00Z 5 coyote_creek 2
	//SCHEMA time index location randtag location=santa_monica randtag=2
	//2019-08-18T03:36:00Z 66 santa_monica 2
	//end
	queryString5 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T03:40:00Z' AND time <= '2019-08-18T04:00:00Z' GROUP BY randtag,location"
	//SCHEMA time index location randtag location=coyote_creek randtag=1
	//2019-08-18T03:48:00Z 43 coyote_creek 1
	//SCHEMA time index location randtag location=coyote_creek randtag=2
	//2019-08-18T03:42:00Z 77 coyote_creek 2
	//SCHEMA time index location randtag location=coyote_creek randtag=3
	//2019-08-18T03:54:00Z 73 coyote_creek 3
	//2019-08-18T04:00:00Z 57 coyote_creek 3
	//SCHEMA time index location randtag location=santa_monica randtag=1
	//2019-08-18T03:48:00Z 62 santa_monica 1
	//2019-08-18T03:54:00Z 27 santa_monica 1
	//SCHEMA time index location randtag location=santa_monica randtag=3
	//2019-08-18T03:42:00Z 69 santa_monica 3
	//2019-08-18T04:00:00Z 22 santa_monica 3
	//end

	tests := []struct {
		name     string
		querys   []string
		expected string
	}{
		{
			name:   " 1 2 ",
			querys: []string{queryString1, queryString2},
			expected: "SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
				"2019-08-18T00:06:00Z 66 coyote_creek 1 \r\n" +
				"2019-08-18T00:18:00Z 91 coyote_creek 1 \r\n" +
				"2019-08-18T00:24:00Z 29 coyote_creek 1 \r\n" +
				"SCHEMA time index location randtag location=coyote_creek randtag=3 \r\n" +
				"2019-08-18T00:00:00Z 85 coyote_creek 3 \r\n" +
				"2019-08-18T00:30:00Z 75 coyote_creek 3 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=1 \r\n" +
				"2019-08-18T00:06:00Z 67 santa_monica 1 \r\n" +
				"2019-08-18T00:18:00Z 14 santa_monica 1 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=2 \r\n" +
				"2019-08-18T00:00:00Z 11 santa_monica 2 \r\n" +
				"2019-08-18T00:30:00Z 79 santa_monica 2 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
				"2019-08-18T00:24:00Z 44 santa_monica 3 \r\n" +
				"end",
		},
		{
			name:   " 3 2 ",
			querys: []string{queryString3, queryString2},
			expected: "SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
				"2019-08-18T01:36:00Z 71 coyote_creek 1 \r\n" +
				"2019-08-18T00:18:00Z 91 coyote_creek 1 \r\n" +
				"2019-08-18T00:24:00Z 29 coyote_creek 1 \r\n" +
				"SCHEMA time index location randtag location=coyote_creek randtag=3 \r\n" +
				"2019-08-18T00:30:00Z 75 coyote_creek 3 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=1 \r\n" +
				"2019-08-18T00:18:00Z 14 santa_monica 1 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=2 \r\n" +
				"2019-08-18T00:30:00Z 79 santa_monica 2 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
				"2019-08-18T01:36:00Z 75 santa_monica 3 \r\n" +
				"2019-08-18T00:24:00Z 44 santa_monica 3 \r\n" +
				"end",
		},
		{
			name:   " 3 4 ",
			querys: []string{queryString3, queryString4},
			expected: "SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
				"2019-08-18T01:36:00Z 71 coyote_creek 1 \r\n" +
				"SCHEMA time index location randtag location=coyote_creek randtag=2 \r\n" +
				"2019-08-18T03:36:00Z 5 coyote_creek 2 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=2 \r\n" +
				"2019-08-18T03:36:00Z 66 santa_monica 2 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
				"2019-08-18T01:36:00Z 75 santa_monica 3 \r\n" +
				"end",
		},
		{
			name:   " 4 5 ",
			querys: []string{queryString4, queryString5},
			expected: "SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
				"2019-08-18T03:48:00Z 43 coyote_creek 1 \r\n" +
				"SCHEMA time index location randtag location=coyote_creek randtag=2 \r\n" +
				"2019-08-18T03:36:00Z 5 coyote_creek 2 \r\n" +
				"2019-08-18T03:42:00Z 77 coyote_creek 2 \r\n" +
				"SCHEMA time index location randtag location=coyote_creek randtag=3 \r\n" +
				"2019-08-18T03:54:00Z 73 coyote_creek 3 \r\n" +
				"2019-08-18T04:00:00Z 57 coyote_creek 3 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=1 \r\n" +
				"2019-08-18T03:48:00Z 62 santa_monica 1 \r\n" +
				"2019-08-18T03:54:00Z 27 santa_monica 1 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=2 \r\n" +
				"2019-08-18T03:36:00Z 66 santa_monica 2 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
				"2019-08-18T03:42:00Z 69 santa_monica 3 \r\n" +
				"2019-08-18T04:00:00Z 22 santa_monica 3 \r\n" +
				"end",
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
			query1 := NewQuery(tt.querys[0], MyDB, "")
			resp1, _ := c.Query(query1)
			query2 := NewQuery(tt.querys[1], MyDB, "")
			resp2, _ := c.Query(query2)

			merged := MergeResultTable(resp1, resp2)
			if strings.Compare(merged.ToString(), tt.expected) != 0 {
				t.Errorf("merged:\n%s", merged.ToString())
				t.Errorf("expected:\n%s", tt.expected)
			}
		})
	}

}

func TestMerge(t *testing.T) {
	// 连接数据库
	var c, _ = NewHTTPClient(HTTPConfig{
		//Addr: "http://10.170.48.244:8086",
		Addr: "http://localhost:8086",
	})
	MyDB := "NOAA_water_database"
	query1 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location"
	nq1 := NewQuery(query1, MyDB, "")
	resp1, _ := c.Query(nq1)
	resp1.ToString()
	//SCHEMA time index location randtag location=coyote_creek randtag=1
	//2019-08-18T00:06:00Z 66 coyote_creek 1
	//2019-08-18T00:18:00Z 91 coyote_creek 1
	//2019-08-18T00:24:00Z 29 coyote_creek 1
	//SCHEMA time index location randtag location=coyote_creek randtag=2
	//2019-08-18T00:12:00Z 78 coyote_creek 2
	//SCHEMA time index location randtag location=coyote_creek randtag=3
	//2019-08-18T00:00:00Z 85 coyote_creek 3
	//2019-08-18T00:30:00Z 75 coyote_creek 3
	//SCHEMA time index location randtag location=santa_monica randtag=1
	//2019-08-18T00:06:00Z 67 santa_monica 1
	//2019-08-18T00:18:00Z 14 santa_monica 1
	//SCHEMA time index location randtag location=santa_monica randtag=2
	//2019-08-18T00:00:00Z 11 santa_monica 2
	//2019-08-18T00:12:00Z 91 santa_monica 2
	//2019-08-18T00:30:00Z 79 santa_monica 2
	//SCHEMA time index location randtag location=santa_monica randtag=3
	//2019-08-18T00:24:00Z 44 santa_monica 3
	//end

	// 1 min
	query2 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:31:00Z' AND time <= '2019-08-18T01:00:00Z' GROUP BY randtag,location"
	nq2 := NewQuery(query2, MyDB, "")
	resp2, _ := c.Query(nq2)
	resp2.ToString()
	//SCHEMA time index location randtag location=coyote_creek randtag=1
	//2019-08-18T00:42:00Z 55 coyote_creek 1
	//SCHEMA time index location randtag location=coyote_creek randtag=3
	//2019-08-18T00:36:00Z 33 coyote_creek 3
	//2019-08-18T00:48:00Z 29 coyote_creek 3
	//2019-08-18T00:54:00Z 94 coyote_creek 3
	//2019-08-18T01:00:00Z 16 coyote_creek 3
	//SCHEMA time index location randtag location=santa_monica randtag=1
	//2019-08-18T00:36:00Z 25 santa_monica 1
	//2019-08-18T00:42:00Z 10 santa_monica 1
	//2019-08-18T00:48:00Z 7 santa_monica 1
	//SCHEMA time index location randtag location=santa_monica randtag=2
	//2019-08-18T01:00:00Z 83 santa_monica 2
	//SCHEMA time index location randtag location=santa_monica randtag=3
	//2019-08-18T00:54:00Z 27 santa_monica 3
	//end

	// 30 min
	query3 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T01:31:00Z' AND time <= '2019-08-18T02:00:00Z' GROUP BY randtag,location"
	nq3 := NewQuery(query3, MyDB, "")
	resp3, _ := c.Query(nq3)
	resp3.ToString()
	//SCHEMA time index location randtag location=coyote_creek randtag=1
	//2019-08-18T01:36:00Z 71 coyote_creek 1
	//2019-08-18T01:54:00Z 8 coyote_creek 1
	//2019-08-18T02:00:00Z 97 coyote_creek 1
	//SCHEMA time index location randtag location=coyote_creek randtag=2
	//2019-08-18T01:48:00Z 24 coyote_creek 2
	//SCHEMA time index location randtag location=coyote_creek randtag=3
	//2019-08-18T01:42:00Z 67 coyote_creek 3
	//SCHEMA time index location randtag location=santa_monica randtag=1
	//2019-08-18T01:42:00Z 8 santa_monica 1
	//2019-08-18T01:48:00Z 70 santa_monica 1
	//2019-08-18T02:00:00Z 82 santa_monica 1
	//SCHEMA time index location randtag location=santa_monica randtag=2
	//2019-08-18T01:54:00Z 86 santa_monica 2
	//SCHEMA time index location randtag location=santa_monica randtag=3
	//2019-08-18T01:36:00Z 75 santa_monica 3
	//end

	// 1 h
	query4 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T03:00:00Z' AND time <= '2019-08-18T04:00:00Z' GROUP BY randtag,location"
	nq4 := NewQuery(query4, MyDB, "")
	resp4, _ := c.Query(nq4)
	st4, et4 := GetResponseTimeRange(resp4)
	fmt.Printf("st4:%d\tet4:%d\n", st4, et4)
	resp4.ToString()
	//SCHEMA time index location randtag location=coyote_creek randtag=1
	//2019-08-18T03:12:00Z 90 coyote_creek 1
	//2019-08-18T03:18:00Z 41 coyote_creek 1
	//2019-08-18T03:48:00Z 43 coyote_creek 1
	//SCHEMA time index location randtag location=coyote_creek randtag=2
	//2019-08-18T03:30:00Z 70 coyote_creek 2
	//2019-08-18T03:36:00Z 5 coyote_creek 2
	//2019-08-18T03:42:00Z 77 coyote_creek 2
	//SCHEMA time index location randtag location=coyote_creek randtag=3
	//2019-08-18T03:00:00Z 37 coyote_creek 3
	//2019-08-18T03:06:00Z 13 coyote_creek 3
	//2019-08-18T03:24:00Z 22 coyote_creek 3
	//2019-08-18T03:54:00Z 73 coyote_creek 3
	//2019-08-18T04:00:00Z 57 coyote_creek 3
	//SCHEMA time index location randtag location=santa_monica randtag=1
	//2019-08-18T03:06:00Z 28 santa_monica 1
	//2019-08-18T03:12:00Z 19 santa_monica 1
	//2019-08-18T03:48:00Z 62 santa_monica 1
	//2019-08-18T03:54:00Z 27 santa_monica 1
	//SCHEMA time index location randtag location=santa_monica randtag=2
	//2019-08-18T03:00:00Z 90 santa_monica 2
	//2019-08-18T03:18:00Z 56 santa_monica 2
	//2019-08-18T03:30:00Z 96 santa_monica 2
	//2019-08-18T03:36:00Z 66 santa_monica 2
	//SCHEMA time index location randtag location=santa_monica randtag=3
	//2019-08-18T03:24:00Z 1 santa_monica 3
	//2019-08-18T03:42:00Z 69 santa_monica 3
	//2019-08-18T04:00:00Z 22 santa_monica 3
	//end

	// 1 s
	query5 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T04:00:01Z' AND time <= '2019-08-18T04:30:00Z' GROUP BY randtag,location"
	nq5 := NewQuery(query5, MyDB, "")
	resp5, _ := c.Query(nq5)
	st5, et5 := GetResponseTimeRange(resp5)
	fmt.Printf("st5:%d\tet5:%d\n", st5, et5)
	resp5.ToString()
	//SCHEMA time index location randtag location=coyote_creek randtag=1
	//2019-08-18T04:18:00Z 64 coyote_creek 1
	//2019-08-18T04:30:00Z 14 coyote_creek 1
	//SCHEMA time index location randtag location=coyote_creek randtag=2
	//2019-08-18T04:06:00Z 63 coyote_creek 2
	//2019-08-18T04:24:00Z 59 coyote_creek 2
	//SCHEMA time index location randtag location=coyote_creek randtag=3
	//2019-08-18T04:12:00Z 41 coyote_creek 3
	//SCHEMA time index location randtag location=santa_monica randtag=1
	//2019-08-18T04:18:00Z 89 santa_monica 1
	//2019-08-18T04:24:00Z 80 santa_monica 1
	//SCHEMA time index location randtag location=santa_monica randtag=2
	//2019-08-18T04:06:00Z 24 santa_monica 2
	//SCHEMA time index location randtag location=santa_monica randtag=3
	//2019-08-18T04:12:00Z 48 santa_monica 3
	//2019-08-18T04:30:00Z 42 santa_monica 3
	//end

	//当前时间间隔设置为 1 min,	上面的五个结果中，resp1和resp2、resp4和resp5 理论上可以合并，实际上resp1和resp2的起止时间之差超过了误差范围，不能合并
	// 时间间隔设置为 1h 时，可以合并	暂时修改为 1h
	fmt.Printf("st5 - et4:%d\t\n", st5-et4)
	fmt.Println("(st5-et4)>int64(time.Minute):", (st5-et4) > time.Minute.Nanoseconds())
	fmt.Println("(st5-et4)>int64(time.Hour):", (st5-et4) > time.Hour.Nanoseconds())

	tests := []struct {
		name     string
		resps    []*Response
		expected []string
	}{
		{
			name:  " 5 4 ",
			resps: []*Response{resp5, resp4},
			expected: []string{"SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
				"2019-08-18T03:12:00Z 90 coyote_creek 1 \r\n" +
				"2019-08-18T03:18:00Z 41 coyote_creek 1 \r\n" +
				"2019-08-18T03:48:00Z 43 coyote_creek 1 \r\n" +
				"2019-08-18T04:18:00Z 64 coyote_creek 1 \r\n" +
				"2019-08-18T04:30:00Z 14 coyote_creek 1 \r\n" +
				"SCHEMA time index location randtag location=coyote_creek randtag=2 \r\n" +
				"2019-08-18T03:30:00Z 70 coyote_creek 2 \r\n" +
				"2019-08-18T03:36:00Z 5 coyote_creek 2 \r\n" +
				"2019-08-18T03:42:00Z 77 coyote_creek 2 \r\n" +
				"2019-08-18T04:06:00Z 63 coyote_creek 2 \r\n" +
				"2019-08-18T04:24:00Z 59 coyote_creek 2 \r\n" +
				"SCHEMA time index location randtag location=coyote_creek randtag=3 \r\n" +
				"2019-08-18T03:00:00Z 37 coyote_creek 3 \r\n" +
				"2019-08-18T03:06:00Z 13 coyote_creek 3 \r\n" +
				"2019-08-18T03:24:00Z 22 coyote_creek 3 \r\n" +
				"2019-08-18T03:54:00Z 73 coyote_creek 3 \r\n" +
				"2019-08-18T04:00:00Z 57 coyote_creek 3 \r\n" +
				"2019-08-18T04:12:00Z 41 coyote_creek 3 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=1 \r\n" +
				"2019-08-18T03:06:00Z 28 santa_monica 1 \r\n" +
				"2019-08-18T03:12:00Z 19 santa_monica 1 \r\n" +
				"2019-08-18T03:48:00Z 62 santa_monica 1 \r\n" +
				"2019-08-18T03:54:00Z 27 santa_monica 1 \r\n" +
				"2019-08-18T04:18:00Z 89 santa_monica 1 \r\n" +
				"2019-08-18T04:24:00Z 80 santa_monica 1 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=2 \r\n" +
				"2019-08-18T03:00:00Z 90 santa_monica 2 \r\n" +
				"2019-08-18T03:18:00Z 56 santa_monica 2 \r\n" +
				"2019-08-18T03:30:00Z 96 santa_monica 2 \r\n" +
				"2019-08-18T03:36:00Z 66 santa_monica 2 \r\n" +
				"2019-08-18T04:06:00Z 24 santa_monica 2 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
				"2019-08-18T03:24:00Z 1 santa_monica 3 \r\n" +
				"2019-08-18T03:42:00Z 69 santa_monica 3 \r\n" +
				"2019-08-18T04:00:00Z 22 santa_monica 3 \r\n" +
				"2019-08-18T04:12:00Z 48 santa_monica 3 \r\n" +
				"2019-08-18T04:30:00Z 42 santa_monica 3 \r\n" +
				"end"},
		},
		{
			name:  " 2 1 ",
			resps: []*Response{resp2, resp1},
			expected: []string{"SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
				"2019-08-18T00:06:00Z 66 coyote_creek 1 \r\n" +
				"2019-08-18T00:18:00Z 91 coyote_creek 1 \r\n" +
				"2019-08-18T00:24:00Z 29 coyote_creek 1 \r\n" +
				"2019-08-18T00:42:00Z 55 coyote_creek 1 \r\n" +
				"SCHEMA time index location randtag location=coyote_creek randtag=2 \r\n" +
				"2019-08-18T00:12:00Z 78 coyote_creek 2 \r\n" +
				"SCHEMA time index location randtag location=coyote_creek randtag=3 \r\n" +
				"2019-08-18T00:00:00Z 85 coyote_creek 3 \r\n" +
				"2019-08-18T00:30:00Z 75 coyote_creek 3 \r\n" +
				"2019-08-18T00:36:00Z 33 coyote_creek 3 \r\n" +
				"2019-08-18T00:48:00Z 29 coyote_creek 3 \r\n" +
				"2019-08-18T00:54:00Z 94 coyote_creek 3 \r\n" +
				"2019-08-18T01:00:00Z 16 coyote_creek 3 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=1 \r\n" +
				"2019-08-18T00:06:00Z 67 santa_monica 1 \r\n" +
				"2019-08-18T00:18:00Z 14 santa_monica 1 \r\n" +
				"2019-08-18T00:36:00Z 25 santa_monica 1 \r\n" +
				"2019-08-18T00:42:00Z 10 santa_monica 1 \r\n" +
				"2019-08-18T00:48:00Z 7 santa_monica 1 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=2 \r\n" +
				"2019-08-18T00:00:00Z 11 santa_monica 2 \r\n" +
				"2019-08-18T00:12:00Z 91 santa_monica 2 \r\n" +
				"2019-08-18T00:30:00Z 79 santa_monica 2 \r\n" +
				"2019-08-18T01:00:00Z 83 santa_monica 2 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
				"2019-08-18T00:24:00Z 44 santa_monica 3 \r\n" +
				"2019-08-18T00:54:00Z 27 santa_monica 3 \r\n" +
				"end",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			merged := Merge("1h", tt.resps[0], tt.resps[1])
			for m := range merged {
				if merged[m].ToString() != tt.expected[m] {
					t.Error("merged:\t", merged[m].ToString())
					t.Error("expected:\t", tt.expected[m])
				}
				//fmt.Printf("merged:\t%s\n", merged[m].ToString())
			}
		})
	}

}

func TestMerge2(t *testing.T) {

	queryString1 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:10:00Z' GROUP BY randtag,location"
	//SCHEMA time index location randtag location=coyote_creek randtag=1
	//2019-08-18T00:06:00Z 66 coyote_creek 1
	//SCHEMA time index location randtag location=coyote_creek randtag=3
	//2019-08-18T00:00:00Z 85 coyote_creek 3
	//SCHEMA time index location randtag location=santa_monica randtag=1
	//2019-08-18T00:06:00Z 67 santa_monica 1
	//SCHEMA time index location randtag location=santa_monica randtag=2
	//2019-08-18T00:00:00Z 11 santa_monica 2
	//end
	queryString2 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:15:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location"
	//SCHEMA time index location randtag location=coyote_creek randtag=1
	//2019-08-18T00:18:00Z 91 coyote_creek 1
	//2019-08-18T00:24:00Z 29 coyote_creek 1
	//SCHEMA time index location randtag location=coyote_creek randtag=3
	//2019-08-18T00:30:00Z 75 coyote_creek 3
	//SCHEMA time index location randtag location=santa_monica randtag=1
	//2019-08-18T00:18:00Z 14 santa_monica 1
	//SCHEMA time index location randtag location=santa_monica randtag=2
	//2019-08-18T00:30:00Z 79 santa_monica 2
	//SCHEMA time index location randtag location=santa_monica randtag=3
	//2019-08-18T00:24:00Z 44 santa_monica 3
	//end
	queryString3 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T01:31:00Z' AND time <= '2019-08-18T01:40:00Z' GROUP BY randtag,location"
	//SCHEMA time index location randtag location=coyote_creek randtag=1
	//2019-08-18T01:36:00Z 71 coyote_creek 1
	//SCHEMA time index location randtag location=santa_monica randtag=3
	//2019-08-18T01:36:00Z 75 santa_monica 3
	//end
	queryString4 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T03:31:00Z' AND time <= '2019-08-18T03:40:00Z' GROUP BY randtag,location"
	//SCHEMA time index location randtag location=coyote_creek randtag=2
	//2019-08-18T03:36:00Z 5 coyote_creek 2
	//SCHEMA time index location randtag location=santa_monica randtag=2
	//2019-08-18T03:36:00Z 66 santa_monica 2
	//end
	queryString5 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T03:40:00Z' AND time <= '2019-08-18T04:00:00Z' GROUP BY randtag,location"
	//SCHEMA time index location randtag location=coyote_creek randtag=1
	//2019-08-18T03:48:00Z 43 coyote_creek 1
	//SCHEMA time index location randtag location=coyote_creek randtag=2
	//2019-08-18T03:42:00Z 77 coyote_creek 2
	//SCHEMA time index location randtag location=coyote_creek randtag=3
	//2019-08-18T03:54:00Z 73 coyote_creek 3
	//2019-08-18T04:00:00Z 57 coyote_creek 3
	//SCHEMA time index location randtag location=santa_monica randtag=1
	//2019-08-18T03:48:00Z 62 santa_monica 1
	//2019-08-18T03:54:00Z 27 santa_monica 1
	//SCHEMA time index location randtag location=santa_monica randtag=3
	//2019-08-18T03:42:00Z 69 santa_monica 3
	//2019-08-18T04:00:00Z 22 santa_monica 3
	//end
	tests := []struct {
		name     string
		querys   []string
		expected []string
	}{
		{
			name:   " 1 2 3 4 5 precision=\"h\" merged: 1 with 2 , 4 with 5 ",
			querys: []string{queryString1, queryString2, queryString3, queryString4, queryString5},
			expected: []string{
				"SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
					"2019-08-18T00:06:00Z 66 coyote_creek 1 \r\n" +
					"2019-08-18T00:18:00Z 91 coyote_creek 1 \r\n" +
					"2019-08-18T00:24:00Z 29 coyote_creek 1 \r\n" +
					"SCHEMA time index location randtag location=coyote_creek randtag=3 \r\n" +
					"2019-08-18T00:00:00Z 85 coyote_creek 3 \r\n" +
					"2019-08-18T00:30:00Z 75 coyote_creek 3 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=1 \r\n" +
					"2019-08-18T00:06:00Z 67 santa_monica 1 \r\n" +
					"2019-08-18T00:18:00Z 14 santa_monica 1 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=2 \r\n" +
					"2019-08-18T00:00:00Z 11 santa_monica 2 \r\n" +
					"2019-08-18T00:30:00Z 79 santa_monica 2 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
					"2019-08-18T00:24:00Z 44 santa_monica 3 \r\n" +
					"end",
				"SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
					"2019-08-18T01:36:00Z 71 coyote_creek 1 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
					"2019-08-18T01:36:00Z 75 santa_monica 3 \r\n" +
					"end",
				"SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
					"2019-08-18T03:48:00Z 43 coyote_creek 1 \r\n" +
					"SCHEMA time index location randtag location=coyote_creek randtag=2 \r\n" +
					"2019-08-18T03:36:00Z 5 coyote_creek 2 \r\n" +
					"2019-08-18T03:42:00Z 77 coyote_creek 2 \r\n" +
					"SCHEMA time index location randtag location=coyote_creek randtag=3 \r\n" +
					"2019-08-18T03:54:00Z 73 coyote_creek 3 \r\n" +
					"2019-08-18T04:00:00Z 57 coyote_creek 3 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=1 \r\n" +
					"2019-08-18T03:48:00Z 62 santa_monica 1 \r\n" +
					"2019-08-18T03:54:00Z 27 santa_monica 1 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=2 \r\n" +
					"2019-08-18T03:36:00Z 66 santa_monica 2 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
					"2019-08-18T03:42:00Z 69 santa_monica 3 \r\n" +
					"2019-08-18T04:00:00Z 22 santa_monica 3 \r\n" +
					"end",
			},
		},
		{
			name:   " 3 5 2 1 4 precision=\"h\" merged: 1 with 2 , 4 with 5 ",
			querys: []string{queryString3, queryString5, queryString2, queryString1, queryString4},
			expected: []string{
				"SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
					"2019-08-18T00:06:00Z 66 coyote_creek 1 \r\n" +
					"2019-08-18T00:18:00Z 91 coyote_creek 1 \r\n" +
					"2019-08-18T00:24:00Z 29 coyote_creek 1 \r\n" +
					"SCHEMA time index location randtag location=coyote_creek randtag=3 \r\n" +
					"2019-08-18T00:00:00Z 85 coyote_creek 3 \r\n" +
					"2019-08-18T00:30:00Z 75 coyote_creek 3 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=1 \r\n" +
					"2019-08-18T00:06:00Z 67 santa_monica 1 \r\n" +
					"2019-08-18T00:18:00Z 14 santa_monica 1 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=2 \r\n" +
					"2019-08-18T00:00:00Z 11 santa_monica 2 \r\n" +
					"2019-08-18T00:30:00Z 79 santa_monica 2 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
					"2019-08-18T00:24:00Z 44 santa_monica 3 \r\n" +
					"end",
				"SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
					"2019-08-18T01:36:00Z 71 coyote_creek 1 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
					"2019-08-18T01:36:00Z 75 santa_monica 3 \r\n" +
					"end",
				"SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
					"2019-08-18T03:48:00Z 43 coyote_creek 1 \r\n" +
					"SCHEMA time index location randtag location=coyote_creek randtag=2 \r\n" +
					"2019-08-18T03:36:00Z 5 coyote_creek 2 \r\n" +
					"2019-08-18T03:42:00Z 77 coyote_creek 2 \r\n" +
					"SCHEMA time index location randtag location=coyote_creek randtag=3 \r\n" +
					"2019-08-18T03:54:00Z 73 coyote_creek 3 \r\n" +
					"2019-08-18T04:00:00Z 57 coyote_creek 3 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=1 \r\n" +
					"2019-08-18T03:48:00Z 62 santa_monica 1 \r\n" +
					"2019-08-18T03:54:00Z 27 santa_monica 1 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=2 \r\n" +
					"2019-08-18T03:36:00Z 66 santa_monica 2 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
					"2019-08-18T03:42:00Z 69 santa_monica 3 \r\n" +
					"2019-08-18T04:00:00Z 22 santa_monica 3 \r\n" +
					"end",
			},
		},
		{
			name:   " 5 4 3 2 1 precision=\"h\" merged: 1 with 2 , 4 with 5 ",
			querys: []string{queryString5, queryString4, queryString3, queryString2, queryString1},
			expected: []string{
				"SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
					"2019-08-18T00:06:00Z 66 coyote_creek 1 \r\n" +
					"2019-08-18T00:18:00Z 91 coyote_creek 1 \r\n" +
					"2019-08-18T00:24:00Z 29 coyote_creek 1 \r\n" +
					"SCHEMA time index location randtag location=coyote_creek randtag=3 \r\n" +
					"2019-08-18T00:00:00Z 85 coyote_creek 3 \r\n" +
					"2019-08-18T00:30:00Z 75 coyote_creek 3 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=1 \r\n" +
					"2019-08-18T00:06:00Z 67 santa_monica 1 \r\n" +
					"2019-08-18T00:18:00Z 14 santa_monica 1 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=2 \r\n" +
					"2019-08-18T00:00:00Z 11 santa_monica 2 \r\n" +
					"2019-08-18T00:30:00Z 79 santa_monica 2 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
					"2019-08-18T00:24:00Z 44 santa_monica 3 \r\n" +
					"end",
				"SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
					"2019-08-18T01:36:00Z 71 coyote_creek 1 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
					"2019-08-18T01:36:00Z 75 santa_monica 3 \r\n" +
					"end",
				"SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
					"2019-08-18T03:48:00Z 43 coyote_creek 1 \r\n" +
					"SCHEMA time index location randtag location=coyote_creek randtag=2 \r\n" +
					"2019-08-18T03:36:00Z 5 coyote_creek 2 \r\n" +
					"2019-08-18T03:42:00Z 77 coyote_creek 2 \r\n" +
					"SCHEMA time index location randtag location=coyote_creek randtag=3 \r\n" +
					"2019-08-18T03:54:00Z 73 coyote_creek 3 \r\n" +
					"2019-08-18T04:00:00Z 57 coyote_creek 3 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=1 \r\n" +
					"2019-08-18T03:48:00Z 62 santa_monica 1 \r\n" +
					"2019-08-18T03:54:00Z 27 santa_monica 1 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=2 \r\n" +
					"2019-08-18T03:36:00Z 66 santa_monica 2 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
					"2019-08-18T03:42:00Z 69 santa_monica 3 \r\n" +
					"2019-08-18T04:00:00Z 22 santa_monica 3 \r\n" +
					"end",
			},
		},
		{
			name:   " 5 4 2  precision=\"h\" merged:  4 with 5 ",
			querys: []string{queryString5, queryString4, queryString2},
			expected: []string{
				"SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
					"2019-08-18T00:18:00Z 91 coyote_creek 1 \r\n" +
					"2019-08-18T00:24:00Z 29 coyote_creek 1 \r\n" +
					"SCHEMA time index location randtag location=coyote_creek randtag=3 \r\n" +
					"2019-08-18T00:30:00Z 75 coyote_creek 3 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=1 \r\n" +
					"2019-08-18T00:18:00Z 14 santa_monica 1 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=2 \r\n" +
					"2019-08-18T00:30:00Z 79 santa_monica 2 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
					"2019-08-18T00:24:00Z 44 santa_monica 3 \r\n" +
					"end",
				"SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
					"2019-08-18T03:48:00Z 43 coyote_creek 1 \r\n" +
					"SCHEMA time index location randtag location=coyote_creek randtag=2 \r\n" +
					"2019-08-18T03:36:00Z 5 coyote_creek 2 \r\n" +
					"2019-08-18T03:42:00Z 77 coyote_creek 2 \r\n" +
					"SCHEMA time index location randtag location=coyote_creek randtag=3 \r\n" +
					"2019-08-18T03:54:00Z 73 coyote_creek 3 \r\n" +
					"2019-08-18T04:00:00Z 57 coyote_creek 3 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=1 \r\n" +
					"2019-08-18T03:48:00Z 62 santa_monica 1 \r\n" +
					"2019-08-18T03:54:00Z 27 santa_monica 1 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=2 \r\n" +
					"2019-08-18T03:36:00Z 66 santa_monica 2 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
					"2019-08-18T03:42:00Z 69 santa_monica 3 \r\n" +
					"2019-08-18T04:00:00Z 22 santa_monica 3 \r\n" +
					"end",
			},
		},
		{
			name:   " 3 1 4  precision=\"h\" merged: none ",
			querys: []string{queryString3, queryString1, queryString4},
			expected: []string{
				"SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
					"2019-08-18T00:06:00Z 66 coyote_creek 1 \r\n" +
					"SCHEMA time index location randtag location=coyote_creek randtag=3 \r\n" +
					"2019-08-18T00:00:00Z 85 coyote_creek 3 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=1 \r\n" +
					"2019-08-18T00:06:00Z 67 santa_monica 1 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=2 \r\n" +
					"2019-08-18T00:00:00Z 11 santa_monica 2 \r\n" +
					"end",
				"SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
					"2019-08-18T01:36:00Z 71 coyote_creek 1 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
					"2019-08-18T01:36:00Z 75 santa_monica 3 \r\n" +
					"end",
				"SCHEMA time index location randtag location=coyote_creek randtag=2 \r\n" +
					"2019-08-18T03:36:00Z 5 coyote_creek 2 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=2 \r\n" +
					"2019-08-18T03:36:00Z 66 santa_monica 2 \r\n" +
					"end",
			},
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
			var resps []*Response
			for i := range tt.querys {
				query := NewQuery(tt.querys[i], MyDB, "")
				respTmp, _ := c.Query(query)
				resps = append(resps, respTmp)
			}
			merged := Merge("1h", resps...)
			for i, m := range merged {
				//fmt.Println(m.ToString())
				if strings.Compare(m.ToString(), tt.expected[i]) != 0 {
					t.Errorf("merged:\n%s", m.ToString())
					t.Errorf("expexted:\n%s", tt.expected[i])
				}
			}
		})
	}

}

func TestGetSeriesTagsMap(t *testing.T) {

	tests := []struct {
		name        string
		queryString string
		expected    string
	}{
		{
			name:        " 6 series ",
			queryString: "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location",
			expected:    "length == 6  map[0:map[location:coyote_creek randtag:1] 1:map[location:coyote_creek randtag:2] 2:map[location:coyote_creek randtag:3] 3:map[location:santa_monica randtag:1] 4:map[location:santa_monica randtag:2] 5:map[location:santa_monica randtag:3]]",
		},
		{
			name:        " 5 series ",
			queryString: "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:31:00Z' AND time <= '2019-08-18T01:00:00Z' GROUP BY randtag,location",
			expected:    "length == 5  map[0:map[location:coyote_creek randtag:1] 1:map[location:coyote_creek randtag:3] 2:map[location:santa_monica randtag:1] 3:map[location:santa_monica randtag:2] 4:map[location:santa_monica randtag:3]]",
		},
		{
			name:        " 1 series (without GROUP BY) ",
			queryString: "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:31:00Z' AND time <= '2019-08-18T01:00:00Z'",
			expected:    "length == 1  map[0:map[]]",
		},
		{
			name:        " 1 series (with GROUP BY) ",
			queryString: "SELECT index,location,randtag FROM h2o_quality WHERE randtag='1' AND time >= '2019-08-18T00:31:00Z' AND time <= '2019-08-18T01:00:00Z' GROUP BY randtag",
			expected:    "length == 1  map[0:map[randtag:1]]",
		},
		{
			name:        " 0 series ",
			queryString: "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2029-08-18T00:31:00Z' AND time <= '2029-08-18T01:00:00Z'",
			expected:    "length == 0  map[]",
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
			resp, _ := c.Query(query)
			tagsMap := GetSeriesTagsMap(resp)
			fmt.Println(len(tagsMap))
			fmt.Println(tagsMap)
		})
	}

}

func TestTagsMapToString(t *testing.T) {
	tests := []struct {
		name     string
		tagsMap  map[string]string
		expected string
	}{
		{
			name:     "empty",
			tagsMap:  map[string]string{},
			expected: "",
		},
		{
			name:     "single",
			tagsMap:  map[string]string{"location": "LA"},
			expected: "location=LA ",
		},
		{
			name:     "double",
			tagsMap:  map[string]string{"location": "LA", "randtag": "2"},
			expected: "location=LA randtag=2 ",
		},
		{
			name:     "multy",
			tagsMap:  map[string]string{"location": "LA", "randtag": "2", "age": "4", "test": "tt"},
			expected: "age=4 location=LA randtag=2 test=tt ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			str := TagsMapToString(tt.tagsMap)
			if str != tt.expected {
				t.Errorf("string:\t%s\nexpected:\t%s", str, tt.expected)
			}
		})
	}
}

func TestMergeSeries(t *testing.T) {

	tests := []struct {
		name        string
		queryString []string
		expected    string
	}{
		{
			name: " one table without GROUP BY",
			queryString: []string{
				"SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z'",
				"SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:31:00Z' AND time <= '2019-08-18T01:00:00Z'",
			},
			expected: "\r\n",
		},
		{
			name: " first 6 tables, second 5 tables, merged 6 tables",
			queryString: []string{
				"SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location",
				"SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:31:00Z' AND time <= '2019-08-18T01:00:00Z' GROUP BY randtag,location",
			},
			expected: "location=coyote_creek randtag=1 \r\n" +
				"location=coyote_creek randtag=2 \r\n" +
				"location=coyote_creek randtag=3 \r\n" +
				"location=santa_monica randtag=1 \r\n" +
				"location=santa_monica randtag=2 \r\n" +
				"location=santa_monica randtag=3 \r\n",
		},
		{
			name: " first 2 tables, second 2 tables, merged 2 tables ",
			queryString: []string{
				"SELECT COUNT(water_level) FROM h2o_feet WHERE time >= '2019-08-18T00:31:00Z' AND time <= '2019-08-18T01:00:00Z' GROUP BY time(12m),location",
				"SELECT COUNT(water_level) FROM h2o_feet WHERE time >= '2019-08-18T02:00:00Z' AND time <= '2019-08-18T02:30:00Z' GROUP BY time(12m),location",
			},
			expected: "location=coyote_creek \r\n" +
				"location=santa_monica \r\n",
		},
		{
			name: " first 6 tables, second 2 tables, merged 6 tables ",
			queryString: []string{
				"SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T01:31:00Z' AND time <= '2019-08-18T02:00:00Z' GROUP BY randtag,location",
				"SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T01:31:00Z' AND time <= '2019-08-18T01:40:00Z' GROUP BY randtag,location",
			},
			expected: "location=coyote_creek randtag=1 \r\n" +
				"location=coyote_creek randtag=2 \r\n" +
				"location=coyote_creek randtag=3 \r\n" +
				"location=santa_monica randtag=1 \r\n" +
				"location=santa_monica randtag=2 \r\n" +
				"location=santa_monica randtag=3 \r\n",
		},
		{
			name: " first 2 tables, second 6 tables, merged 6 tables",
			queryString: []string{
				"SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T01:31:00Z' AND time <= '2019-08-18T01:40:00Z' GROUP BY randtag,location",
				"SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T01:31:00Z' AND time <= '2019-08-18T02:00:00Z' GROUP BY randtag,location",
			},
			expected: "location=coyote_creek randtag=1 \r\n" +
				"location=coyote_creek randtag=2 \r\n" +
				"location=coyote_creek randtag=3 \r\n" +
				"location=santa_monica randtag=1 \r\n" +
				"location=santa_monica randtag=2 \r\n" +
				"location=santa_monica randtag=3 \r\n",
		},
		{
			name: " first 2 tables, second 5 tables, merged 6 tables",
			queryString: []string{
				"SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T01:31:00Z' AND time <= '2019-08-18T01:40:00Z' GROUP BY randtag,location",
				"SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T01:40:00Z' AND time <= '2019-08-18T02:00:00Z' GROUP BY randtag,location",
			},
			expected: "location=coyote_creek randtag=1 \r\n" +
				"location=coyote_creek randtag=2 \r\n" +
				"location=coyote_creek randtag=3 \r\n" +
				"location=santa_monica randtag=1 \r\n" +
				"location=santa_monica randtag=2 \r\n" +
				"location=santa_monica randtag=3 \r\n",
		},
		{
			name: "first 2 tables, second 3 tables, merged 5 tables",
			queryString: []string{
				"SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T01:31:00Z' AND time <= '2019-08-18T01:40:00Z' GROUP BY randtag,location",
				"SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T01:40:00Z' AND time <= '2019-08-18T01:50:00Z' GROUP BY randtag,location",
			},
			expected: "location=coyote_creek randtag=1 \r\n" +
				"location=coyote_creek randtag=2 \r\n" +
				"location=coyote_creek randtag=3 \r\n" +
				"location=santa_monica randtag=1 \r\n" +
				"location=santa_monica randtag=3 \r\n",
		},
		{
			name: "redundant tag",
			queryString: []string{
				"SELECT index,location,randtag FROM h2o_quality WHERE randtag='3' AND time >= '2019-08-18T01:31:00Z' AND time <= '2019-08-18T01:40:00Z' GROUP BY location",
				"SELECT index,location,randtag FROM h2o_quality WHERE randtag='3' AND time >= '2019-08-18T01:40:00Z' AND time <= '2019-08-18T01:50:00Z' GROUP BY location",
			},
			expected: "location=coyote_creek \r\n" +
				"location=santa_monica \r\n",
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
			q1 := NewQuery(tt.queryString[0], MyDB, "s")
			q2 := NewQuery(tt.queryString[1], MyDB, "s")
			resp1, _ := c.Query(q1)
			resp2, _ := c.Query(q2)

			seriesMerged := MergeSeries(resp1, resp2)
			//fmt.Printf("len:%d\n", len(seriesMerged))
			var tagStr string
			for _, s := range seriesMerged {
				tagStr += TagsMapToString(s.Tags)
				tagStr += "\r\n"
			}
			//fmt.Println(tagStr)
			if strings.Compare(tagStr, tt.expected) != 0 {
				t.Errorf("merged:\n%s", tagStr)
				t.Errorf("expected:\n%s", tt.expected)
			}
		})
	}
}

func TestMergeSeries2(t *testing.T) {

	queryString1 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:10:00Z' GROUP BY randtag,location"
	//SCHEMA time index location randtag location=coyote_creek randtag=1
	//2019-08-18T00:06:00Z 66 coyote_creek 1
	//SCHEMA time index location randtag location=coyote_creek randtag=3
	//2019-08-18T00:00:00Z 85 coyote_creek 3
	//SCHEMA time index location randtag location=santa_monica randtag=1
	//2019-08-18T00:06:00Z 67 santa_monica 1
	//SCHEMA time index location randtag location=santa_monica randtag=2
	//2019-08-18T00:00:00Z 11 santa_monica 2
	//end
	queryString2 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:15:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location"
	//SCHEMA time index location randtag location=coyote_creek randtag=1
	//2019-08-18T00:18:00Z 91 coyote_creek 1
	//2019-08-18T00:24:00Z 29 coyote_creek 1
	//SCHEMA time index location randtag location=coyote_creek randtag=3
	//2019-08-18T00:30:00Z 75 coyote_creek 3
	//SCHEMA time index location randtag location=santa_monica randtag=1
	//2019-08-18T00:18:00Z 14 santa_monica 1
	//SCHEMA time index location randtag location=santa_monica randtag=2
	//2019-08-18T00:30:00Z 79 santa_monica 2
	//SCHEMA time index location randtag location=santa_monica randtag=3
	//2019-08-18T00:24:00Z 44 santa_monica 3
	//end
	queryString3 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T01:31:00Z' AND time <= '2019-08-18T01:40:00Z' GROUP BY randtag,location"
	//SCHEMA time index location randtag location=coyote_creek randtag=1
	//2019-08-18T01:36:00Z 71 coyote_creek 1
	//SCHEMA time index location randtag location=santa_monica randtag=3
	//2019-08-18T01:36:00Z 75 santa_monica 3
	//end
	queryString4 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T03:31:00Z' AND time <= '2019-08-18T03:40:00Z' GROUP BY randtag,location"
	//SCHEMA time index location randtag location=coyote_creek randtag=2
	//2019-08-18T03:36:00Z 5 coyote_creek 2
	//SCHEMA time index location randtag location=santa_monica randtag=2
	//2019-08-18T03:36:00Z 66 santa_monica 2
	//end
	queryString5 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T03:40:00Z' AND time <= '2019-08-18T04:00:00Z' GROUP BY randtag,location"
	//SCHEMA time index location randtag location=coyote_creek randtag=1
	//2019-08-18T03:48:00Z 43 coyote_creek 1
	//SCHEMA time index location randtag location=coyote_creek randtag=2
	//2019-08-18T03:42:00Z 77 coyote_creek 2
	//SCHEMA time index location randtag location=coyote_creek randtag=3
	//2019-08-18T03:54:00Z 73 coyote_creek 3
	//2019-08-18T04:00:00Z 57 coyote_creek 3
	//SCHEMA time index location randtag location=santa_monica randtag=1
	//2019-08-18T03:48:00Z 62 santa_monica 1
	//2019-08-18T03:54:00Z 27 santa_monica 1
	//SCHEMA time index location randtag location=santa_monica randtag=3
	//2019-08-18T03:42:00Z 69 santa_monica 3
	//2019-08-18T04:00:00Z 22 santa_monica 3
	//end
	// redundant tag
	queryString6 := "SELECT index FROM h2o_quality WHERE location='coyote_creek' AND time >= '2019-08-18T02:40:00Z' AND time <= '2019-08-18T03:00:00Z' GROUP BY randtag"
	//name: h2o_quality
	//tags: randtag=1
	//time                index
	//----                -----
	//1566096480000000000 15
	//
	//name: h2o_quality
	//tags: randtag=3
	//time                index
	//----                -----
	//1566096120000000000 86
	//1566096840000000000 95
	//1566097200000000000 37
	queryString7 := "SELECT index FROM h2o_quality WHERE location='coyote_creek' AND time >= '2019-08-18T03:00:00Z' AND time <= '2019-08-18T04:00:00Z' GROUP BY location,randtag"
	//name: h2o_quality
	//tags: location=coyote_creek, randtag=1
	//time                index
	//----                -----
	//1566097920000000000 90
	//1566098280000000000 41
	//1566100080000000000 43
	//
	//name: h2o_quality
	//tags: location=coyote_creek, randtag=2
	//time                index
	//----                -----
	//1566099000000000000 70
	//1566099360000000000 5
	//1566099720000000000 77
	//
	//name: h2o_quality
	//tags: location=coyote_creek, randtag=3
	//time                index
	//----                -----
	//1566097200000000000 37
	//1566097560000000000 13
	//1566098640000000000 22
	//1566100440000000000 73
	//1566100800000000000 57

	queryString8 := "SELECT index FROM h2o_quality WHERE randtag='1' AND time >= '2019-08-18T02:00:00Z' AND time <= '2019-08-18T02:40:00Z' GROUP BY location"
	queryString9 := "SELECT index FROM h2o_quality WHERE randtag='1' AND time >= '2019-08-18T02:40:00Z' AND time <= '2019-08-18T03:00:00Z' GROUP BY location,randtag"

	tests := []struct {
		name     string
		querys   []string
		expected string
	}{
		{
			name:   " 1 2 ",
			querys: []string{queryString1, queryString2},
			expected: "location=coyote_creek randtag=1 \r\n" +
				"location=coyote_creek randtag=3 \r\n" +
				"location=santa_monica randtag=1 \r\n" +
				"location=santa_monica randtag=2 \r\n" +
				"location=santa_monica randtag=3 \r\n",
		},
		{
			name:   " 3 2 ",
			querys: []string{queryString3, queryString2},
			expected: "location=coyote_creek randtag=1 \r\n" +
				"location=coyote_creek randtag=3 \r\n" +
				"location=santa_monica randtag=1 \r\n" +
				"location=santa_monica randtag=2 \r\n" +
				"location=santa_monica randtag=3 \r\n",
		},
		{
			name:   " 1 4 ",
			querys: []string{queryString1, queryString4},
			expected: "location=coyote_creek randtag=1 \r\n" +
				"location=coyote_creek randtag=2 \r\n" +
				"location=coyote_creek randtag=3 \r\n" +
				"location=santa_monica randtag=1 \r\n" +
				"location=santa_monica randtag=2 \r\n",
		},
		{
			name:   " 3 4 ",
			querys: []string{queryString3, queryString4},
			expected: "location=coyote_creek randtag=1 \r\n" +
				"location=coyote_creek randtag=2 \r\n" +
				"location=santa_monica randtag=2 \r\n" +
				"location=santa_monica randtag=3 \r\n",
		},
		{
			name:   " 4 3 ",
			querys: []string{queryString4, queryString3},
			expected: "location=coyote_creek randtag=1 \r\n" +
				"location=coyote_creek randtag=2 \r\n" +
				"location=santa_monica randtag=2 \r\n" +
				"location=santa_monica randtag=3 \r\n",
		},
		{
			name:   " 4 5 ",
			querys: []string{queryString4, queryString5},
			expected: "location=coyote_creek randtag=1 \r\n" +
				"location=coyote_creek randtag=2 \r\n" +
				"location=coyote_creek randtag=3 \r\n" +
				"location=santa_monica randtag=1 \r\n" +
				"location=santa_monica randtag=2 \r\n" +
				"location=santa_monica randtag=3 \r\n",
		},
		{
			name:   " 5 2 ",
			querys: []string{queryString5, queryString2},
			expected: "location=coyote_creek randtag=1 \r\n" +
				"location=coyote_creek randtag=2 \r\n" +
				"location=coyote_creek randtag=3 \r\n" +
				"location=santa_monica randtag=1 \r\n" +
				"location=santa_monica randtag=2 \r\n" +
				"location=santa_monica randtag=3 \r\n",
		},
		{
			name:   " 6 7 ",
			querys: []string{queryString6, queryString7},
			expected: "randtag=1 \r\n" +
				"randtag=2 \r\n" +
				"randtag=3 \r\n",
		},
		{
			name:   " 8 9 ",
			querys: []string{queryString8, queryString9},
			expected: "location=coyote_creek \r\n" +
				"location=santa_monica \r\n",
		},
	}

	for _, tt := range tests {
		// 连接数据库
		var c, _ = NewHTTPClient(HTTPConfig{
			//Addr: "http://10.170.48.244:8086",
			Addr: "http://localhost:8086",
		})
		MyDB := "NOAA_water_database"
		t.Run(tt.name, func(t *testing.T) {
			q1 := NewQuery(tt.querys[0], MyDB, "s")
			q2 := NewQuery(tt.querys[1], MyDB, "s")
			resp1, _ := c.Query(q1)
			resp2, _ := c.Query(q2)

			seriesMerged := MergeSeries(resp1, resp2)
			var tagStr string
			for _, s := range seriesMerged {
				tagStr += TagsMapToString(s.Tags)
				tagStr += "\r\n"
			}

			if strings.Compare(tagStr, tt.expected) != 0 {
				t.Errorf("merged:\n%s", tagStr)
				t.Errorf("expected:\n%s", tt.expected)
			}

		})
	}
}
