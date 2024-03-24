package client

import (
	"encoding/json"
	"fmt"
	"github.com/influxdata/influxql"
	"log"
	"math"
	"regexp"
	"sort"
	"strings"
	"time"
)

// ResponseIsEmpty 判断结果是否为空
func ResponseIsEmpty(resp *Response) bool {
	/* 以下情况之一表示结果为空，返回 true */
	return resp == nil || len(resp.Results[0].Series) == 0 || len(resp.Results[0].Series[0].Values) == 0
}

// GetNumOfTable 获取查询结果中表的数量
func GetNumOfTable(resp *Response) int64 {
	return int64(len(resp.Results[0].Series))
}

// GetResponseTimeRange 获取查询结果的时间范围
// 从 response 中取数据，可以确保起止时间都有，只需要进行类型转换
func GetResponseTimeRange(resp *Response) (int64, int64) {
	var minStartTime int64
	var maxEndTime int64
	var ist int64
	var iet int64

	minStartTime = math.MaxInt64
	maxEndTime = 0
	for s := range resp.Results[0].Series {
		/* 获取一张表的起止时间（string） */
		length := len(resp.Results[0].Series[s].Values)      //一个结果表中有多少条记录
		start := resp.Results[0].Series[s].Values[0][0]      // 第一条记录的时间		第一个查询结果
		end := resp.Results[0].Series[s].Values[length-1][0] // 最后一条记录的时间

		if st, ok := start.(string); ok {
			et := end.(string)
			ist = TimeStringToInt64(st)
			iet = TimeStringToInt64(et)
		} else if st, ok := start.(json.Number); ok {
			et := end.(json.Number)
			ist, _ = st.Int64()
			iet, _ = et.Int64()
		}

		/* 更新起止时间范围 	两个时间可能不在一个表中 ? */
		if minStartTime > ist {
			minStartTime = ist
		}
		if maxEndTime < iet {
			maxEndTime = iet
		}
	}

	return minStartTime, maxEndTime
}

// GetQueryTimeRange 获取一条查询语句的时间范围	单位为秒 "s"
func GetQueryTimeRange(queryString string) (int64, int64) {
	matchStr := `(?i).+WHERE(.+)`
	conditionExpr := regexp.MustCompile(matchStr)
	if ok, _ := regexp.MatchString(matchStr, queryString); !ok {
		return -1, -1
	}
	condExprMatch := conditionExpr.FindStringSubmatch(queryString)
	parseExpr := condExprMatch[1]

	now := time.Now()
	valuer := influxql.NowValuer{Now: now}
	expr, _ := influxql.ParseExpr(parseExpr)
	_, timeRange, err := influxql.ConditionExpr(expr, &valuer)

	if err != nil {
		return -1, -1
	}

	start_time := timeRange.MinTime().Unix()
	end_time := timeRange.MaxTime().Unix()

	if start_time <= 0 || start_time*1000000000 < (math.MinInt64/2) { // 秒转化成纳秒，然后比较 	// todo 优化 判断时间合法性
		start_time = -1
	}
	if end_time <= 0 || end_time*1000000000 > (math.MaxInt64/2) {
		end_time = -1
	}

	// 调用了 influxql 解析查询时间范围的方法，对结束时间的边界条件敏感
	//if start_time != -1 && start_time != end_time && !strings.Contains(queryString, ">=") { // " > start_time "，返回值要减一
	//	start_time--
	//}
	if end_time != -1 && end_time != start_time && !strings.Contains(queryString, "<=") { // " < end_time "，返回值要加一
		end_time++
	}

	return start_time, end_time
}

// GetQueryTemplate 用 "?" 替换查询语句的时间范围，重新排列符号，重构为查询模版
func GetQueryTemplate(queryString string) string {
	/* 替换时间 */
	reg := regexp.MustCompile("\\'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z\\'")
	replacement := "?"
	result := reg.ReplaceAll([]byte(queryString), []byte(replacement))

	/* todo 替换符号，使模版的第一个时间判断符号是 >= , 第二个是 < */

	return string(result)
}

// GetFieldKeys 获取一个数据库中所有表的field name及其数据类型
func GetFieldKeys(c Client, database string) map[string]map[string]string {
	// 构建查询语句
	//query := fmt.Sprintf("SHOW FIELD KEYS on %s from %s", database, measurement)
	query := fmt.Sprintf("SHOW FIELD KEYS on %s", database)

	// 执行查询
	q := NewQuery(query, database, "")
	resp, err := c.Query(q)
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
		return nil
	}

	// 处理查询结果
	if resp.Error() != nil {
		fmt.Printf("Error: %s\n", resp.Error().Error())
		return nil
	}

	fieldMap := make(map[string]map[string]string)
	for _, series := range resp.Results[0].Series {
		fieldNames := make([]string, 0)
		datatypes := make([]string, 0)
		measurementName := series.Name
		for _, value := range series.Values {
			fieldName, ok := value[0].(string)
			datatype, ok := value[1].(string)
			if !ok {
				log.Fatal("field and datatype name fail to convert to string")
			}
			if datatype == "float" {
				datatype = "float64"
			}
			fieldNames = append(fieldNames, fieldName)
			datatypes = append(datatypes, datatype)
		}
		fldMap := make(map[string]string)
		for i := range fieldNames {
			fldMap[fieldNames[i]] = datatypes[i]
		}

		fieldMap[measurementName] = fldMap
	}

	return fieldMap
}

type TagValues struct {
	Values []string
}

type TagKeyMap struct {
	Tag map[string]TagValues
}

type MeasurementTagMap struct {
	Measurement map[string][]TagKeyMap
}

// GetTagKV 获取所有表的tag的key和value
func GetTagKV(c Client, database string) MeasurementTagMap {
	// 构建查询语句
	//query := fmt.Sprintf("SHOW FIELD KEYS on %s from %s", database, measurement)
	queryK := fmt.Sprintf("SHOW tag KEYS on %s", database)

	// 执行查询
	q := NewQuery(queryK, database, "")
	resp, err := c.Query(q)
	if err != nil {
		log.Fatal(err.Error())
	}

	// 处理查询结果
	if resp.Error() != nil {
		log.Fatal(resp.Error().Error())
	}

	tagMap := make(map[string][]string)
	//fmt.Println(resp)
	for _, series := range resp.Results[0].Series {
		measurementName := series.Name
		for _, value := range series.Values {
			tagKey, ok := value[0].(string)
			if !ok {
				log.Fatal("tag name fail to convert to string")
			}
			tagMap[measurementName] = append(tagMap[measurementName], tagKey)
		}
	}

	var measurementTagMap MeasurementTagMap
	measurementTagMap.Measurement = make(map[string][]TagKeyMap)
	for k, v := range tagMap {
		for _, tagKey := range v {
			queryV := fmt.Sprintf("SHOW tag VALUES on %s from %s with key=\"%s\"", database, k, tagKey)
			q := NewQuery(queryV, database, "")
			resp, err := c.Query(q)
			if err != nil {
				log.Fatal(err.Error())
			}
			if resp.Error() != nil {
				log.Fatal(resp.Error().Error())
			}

			var tagValues TagValues
			for _, value := range resp.Results[0].Series[0].Values {
				tagValues.Values = append(tagValues.Values, value[1].(string))
			}
			tmpKeyMap := make(map[string]TagValues, 0)
			tmpKeyMap[tagKey] = tagValues
			tagKeyMap := TagKeyMap{tmpKeyMap}
			measurementTagMap.Measurement[k] = append(measurementTagMap.Measurement[k], tagKeyMap)
		}
	}

	return measurementTagMap
}

// GetTagNameArr 判断结果是否为空，并从结果中取出tags数组，用于规范tag map的输出顺序
func GetTagNameArr(resp *Response) []string {
	tagArr := make([]string, 0)
	if resp == nil || len(resp.Results[0].Series) == 0 {
		return tagArr
	} else {
		if len(resp.Results[0].Series[0].Values) == 0 {
			return tagArr
		} else {
			for k, _ := range resp.Results[0].Series[0].Tags {
				tagArr = append(tagArr, k)
			}
		}
	}
	sort.Strings(tagArr) // 对tags排序
	return tagArr
}

// GetDataTypeArrayFromSF  从列名和数据类型组成的字符串中提取出每一列的数据类型
// time[int64],index[int64],location[string],randtag[string]
// 列名和数据类型都存放在数组中，顺序是固定的，不用手动排序，直接取出来就行
func GetDataTypeArrayFromSF(sfString string) []string {
	datatypes := make([]string, 0)
	columns := strings.Split(sfString, ",")

	for _, col := range columns {
		startIdx := strings.Index(col, "[") + 1
		endIdx := strings.Index(col, "]")
		datatypes = append(datatypes, col[startIdx:endIdx])
	}

	return datatypes
}

// GetDataTypeArrayFromResponse 从查寻结果中获取每一列的数据类型
func GetDataTypeArrayFromResponse(resp *Response) []string {
	fields := make([]string, 0)
	done := false
	able := false
	for _, s := range resp.Results[0].Series {
		if done {
			break
		}
		for _, v := range s.Values {
			if done {
				break
			}
			for _, vv := range v {
				if vv == nil {
					able = false
					break
				}
				able = true // 找到所有字段都不为空的一条数据
			}
			if able {
				for i, value := range v { // 根据具体数据推断该列的数据类型
					if i == 0 { // 根据查询条件不同，结果的时间戳可能是 string 或 int64，只使用 int64
						fields = append(fields, "int64")
					} else if _, ok := value.(string); ok {
						fields = append(fields, "string")
					} else if v, ok := value.(json.Number); ok {
						if _, err := v.Int64(); err == nil {
							fields = append(fields, "float64") // todo
						} else if _, err := v.Float64(); err == nil {
							fields = append(fields, "float64")
						} else {
							fields = append(fields, "string")
						}
					} else if _, ok := value.(bool); ok {
						fields = append(fields, "bool")
					}
					done = true
				}
			}

		}
	}

	return fields
}
