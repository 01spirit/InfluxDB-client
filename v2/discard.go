package client

import (
	"fmt"
	"github.com/influxdata/influxql"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

// 已被弃用的函数

// GetSM get measurement's name and tags
// func GetSM(queryString string, resp *Response) string {
func GetSM(resp *Response, tagPredicates []string) string {
	var result string
	var tagArr []string

	if ResponseIsEmpty(resp) {
		return "{empty}"
	}

	tagArr = GetTagNameArr(resp)
	tagPre := make([]string, 0)
	for i := range tagPredicates {
		var idx int
		if idx = strings.Index(tagPredicates[i], "!"); idx < 0 { // "!="
			idx = strings.Index(tagPredicates[i], "=") // "="
		}
		tagName := tagPredicates[i][:idx]
		if !slices.Contains(tagArr, tagName) {
			tagPre = append(tagPre, tagPredicates[i])
		}
	}

	// 格式： {(name.tag_key=tag_value)...}
	// 有查询结果 且 结果中有tag	当结果为空或某些使用聚合函数的情况都会输出 "empty tag"
	//if len(resp.Results[0].Series) > 0 && len(resp.Results[0].Series[0].Tags) > 0 {
	result += "{"
	tmpTags := make([]string, 0)
	if len(tagArr) > 0 {

		for _, s := range resp.Results[0].Series {
			//result += "("
			//measurementName := s.Name
			//for _, tagName := range tagArr {
			//	result += fmt.Sprintf("%s.%s=%s,", measurementName, tagName, s.Tags[tagName])
			//}
			//result = result[:len(result)-1]
			//result += ")"
			measurement := s.Name
			tmpTags = nil
			for _, tagName := range tagArr {
				tmpTag := fmt.Sprintf("%s=%s", tagName, s.Tags[tagName])
				tmpTags = append(tmpTags, tmpTag)
			}
			tmpTags = append(tmpTags, tagPre...)
			for i, tag := range tmpTags {
				tmpTags[i] = fmt.Sprintf("%s.%s", measurement, tag)
			}
			sort.Strings(tmpTags)
			tmpResult := strings.Join(tmpTags, ",")
			result += fmt.Sprintf("(%s)", tmpResult)
		}

	} else if len(tagPre) > 0 {
		measurement := resp.Results[0].Series[0].Name
		for i, tag := range tagPre {
			tagPre[i] = fmt.Sprintf("%s.%s", measurement, tag)
		}
		tmpResult := strings.Join(tagPre, ",")
		result += fmt.Sprintf("(%s)", tmpResult)
	} else {
		measurementName := resp.Results[0].Series[0].Name
		result = fmt.Sprintf("{(%s.empty)}", measurementName)
		return result
	}

	//result = result[:len(result)-1] //去掉最后的空格
	result += "}" //标志转换结束

	return result
}

/* 分别返回每张表的tag */
func GetSeperateSM(resp *Response, tagPredicates []string) []string {
	var result []string
	var tagArr []string

	if ResponseIsEmpty(resp) {
		return []string{"{empty}"}
	}

	measurement := resp.Results[0].Series[0].Name
	tagArr = GetTagNameArr(resp)

	tagPre := make([]string, 0)
	for i := range tagPredicates {
		var idx int
		if idx = strings.Index(tagPredicates[i], "!"); idx < 0 { // "!="
			idx = strings.Index(tagPredicates[i], "=")
		}
		tagName := tagPredicates[i][:idx]
		if !slices.Contains(tagArr, tagName) {
			tagPre = append(tagPre, tagPredicates[i])
		}
	}

	tmpTags := make([]string, 0)
	if len(tagArr) > 0 {
		for _, s := range resp.Results[0].Series {
			var tmp string
			tmpTags = nil
			for _, tagKey := range tagArr {
				tag := fmt.Sprintf("%s=%s", tagKey, s.Tags[tagKey])
				tmpTags = append(tmpTags, tag)
			}
			tmpTags = append(tmpTags, tagPre...)
			sort.Strings(tmpTags)

			tmp += "{("
			for _, t := range tmpTags {
				tmp += fmt.Sprintf("%s.%s,", measurement, t)
			}
			tmp = tmp[:len(tmp)-1]
			tmp += ")}"
			result = append(result, tmp)
		}
	} else if len(tagPre) > 0 {
		var tmp string
		tmp += "{("
		for _, t := range tagPre {
			tmp += fmt.Sprintf("%s.%s,", measurement, t)
		}
		tmp = tmp[:len(tmp)-1]
		tmp += ")}"
		result = append(result, tmp)
	} else {
		tmp := fmt.Sprintf("{(%s.empty)}", measurement)
		result = append(result, tmp)
		return result
	}
	return result
}

// GetAggregation  从查询语句中获取聚合函数
func GetAggregation(queryString string) string {
	/* 用正则匹配取出包含 列名 和 聚合函数 的字符串  */
	regStr := `(?i)SELECT\s*(.+)\s*FROM.+`
	regExpr := regexp.MustCompile(regStr)
	var FGstr string
	if ok, _ := regexp.MatchString(regStr, queryString); ok {
		match := regExpr.FindStringSubmatch(queryString)
		FGstr = match[1] // fields and aggr
	} else {
		return "error"
	}

	/* 从字符串中截取出聚合函数 */
	var aggr string
	if strings.IndexAny(FGstr, ")") > 0 {
		index := strings.IndexAny(FGstr, "(")
		aggr = FGstr[:index]
		aggr = strings.ToLower(aggr)
	} else {
		return "empty"
	}

	return aggr
}

// GetSFSGWithDataType  重写，包含数据类型和列名
func GetSFSGWithDataType(queryString string, resp *Response) (string, string) {
	var fields []string
	var FGstr string

	/* 用正则匹配从查询语句中取出包含聚合函数和列名的字符串  */
	regStr := `(?i)SELECT\s*(.+)\s*FROM.+`
	regExpr := regexp.MustCompile(regStr)

	if ok, _ := regexp.MatchString(regStr, queryString); ok {
		match := regExpr.FindStringSubmatch(queryString)
		FGstr = match[1] // fields and aggr
	} else {
		return "error", "error"
	}

	var aggr string
	singleField := strings.Split(FGstr, ",")
	if strings.IndexAny(singleField[0], "(") > 0 && strings.IndexAny(singleField[0], "*") < 0 { // 有一或多个聚合函数, 没有通配符 '*'
		/* 获取聚合函数名 */
		index := strings.IndexAny(singleField[0], "(")
		aggr = singleField[0][:index]
		aggr = strings.ToLower(aggr)

		/* 从查询语句获取field(实际的列名) */
		fields = append(fields, "time")
		var startIdx int
		var endIdx int
		for i := range singleField {
			for idx, ch := range singleField[i] { // 括号中间的部分是fields，默认没有双引号，不作处理
				if ch == '(' {
					startIdx = idx + 1
				}
				if ch == ')' {
					endIdx = idx
				}
			}
			tmpStr := singleField[i][startIdx:endIdx]
			tmpArr := strings.Split(tmpStr, ",")
			fields = append(fields, tmpArr...)
		}

	} else if strings.IndexAny(singleField[0], "(") > 0 && strings.IndexAny(singleField[0], "*") >= 0 { // 有聚合函数，有通配符 '*'
		/* 获取聚合函数名 */
		index := strings.IndexAny(singleField[0], "(")
		aggr = singleField[0][:index]
		aggr = strings.ToLower(aggr)

		/* 从Response获取列名 */
		for _, c := range resp.Results[0].Series[0].Columns {
			startIdx := strings.IndexAny(c, "_")
			if startIdx > 0 {
				tmpStr := c[startIdx+1:]
				fields = append(fields, tmpStr)
			} else {
				fields = append(fields, c)
			}
		}

	} else { // 没有聚合函数，通配符无所谓
		aggr = "empty"
		/* 从Response获取列名 */
		for _, c := range resp.Results[0].Series[0].Columns {
			fields = append(fields, c)
		}
	}

	/* 从查寻结果中获取每一列的数据类型 */
	dataTypes := GetDataTypeArrayFromResponse(resp)
	for i := range fields {
		fields[i] = fmt.Sprintf("%s[%s]", fields[i], dataTypes[i])
	}

	//去掉第一列中的 time[int64]
	fields = fields[1:]
	var fieldsStr string
	fieldsStr = strings.Join(fields, ",")

	return fieldsStr, aggr
}

func GetSFSG(query string) (string, string) {
	regStr := `(?i)SELECT\s*(.+)\s*FROM.+`
	regExpr := regexp.MustCompile(regStr)
	var FGstr string
	if ok, _ := regexp.MatchString(regStr, query); ok { // 取出 fields 和 聚合函数aggr
		match := regExpr.FindStringSubmatch(query)
		FGstr = match[1] // fields and aggr
	} else {
		return "err", "err"
	}

	var flds string
	var aggr string

	if strings.IndexAny(FGstr, ")") > 0 { // 如果这部分有括号，说明有聚合函数 ?
		/* get aggr */
		fields := influxql.Fields{}
		expr, err := influxql.NewParser(strings.NewReader(FGstr)).ParseExpr()
		if err != nil {
			panic(err)
		}
		fields = append(fields, &influxql.Field{Expr: expr})
		aggrs := fields.Names()
		aggr = strings.Join(aggrs, ",") //获取聚合函数

		/* get fields */
		//flds += "time,"
		var start_idx int
		var end_idx int
		for idx, ch := range FGstr { // 括号中间的部分是fields，默认没有双引号，不作处理
			if ch == '(' {
				start_idx = idx + 1
			}
			if ch == ')' {
				end_idx = idx
			}
		}
		flds += FGstr[start_idx:end_idx]

	} else { //没有聚合函数，直接从查询语句中解析出fields
		aggr = "empty"
		parser := influxql.NewParser(strings.NewReader(query))
		stmt, _ := parser.ParseStatement()
		s := stmt.(*influxql.SelectStatement)
		flds = strings.Join(s.ColumnNames()[1:], ",")
	}

	return flds, aggr
}

/* 只获取谓词，不要时间范围 */
func GetSP(query string, resp *Response, tagMap MeasurementTagMap) (string, []string) {
	//regStr := `(?i).+WHERE(.+)GROUP BY.`
	regStr := `(?i).+WHERE(.+)`
	conditionExpr := regexp.MustCompile(regStr)
	if ok, _ := regexp.MatchString(regStr, query); !ok {
		return "{empty}", nil
	}
	condExprMatch := conditionExpr.FindStringSubmatch(query) // 获取 WHERE 后面的所有表达式，包括谓词和时间范围
	parseExpr := condExprMatch[1]

	now := time.Now()
	valuer := influxql.NowValuer{Now: now}
	expr, _ := influxql.ParseExpr(parseExpr)
	cond, _, _ := influxql.ConditionExpr(expr, &valuer) //提取出谓词

	tagConds := make([]string, 0)
	var result string
	if cond == nil { //没有谓词
		result += fmt.Sprintf("{empty}")
	} else { //从语法树中找出由AND或OR连接的所有独立的谓词表达式
		var conds []string
		var tag []string
		binaryExpr := cond.(*influxql.BinaryExpr)
		var datatype []string
		var measurement string
		if !ResponseIsEmpty(resp) {
			measurement = resp.Results[0].Series[0].Name
		} else {
			return "{empty}", nil
		}

		tags, predicates, datatypes := preOrderTraverseBinaryExpr(binaryExpr, &tag, &conds, &datatype)
		result += "{"
		for i, p := range *predicates {
			isTag := false
			found := false
			for _, t := range tagMap.Measurement[measurement] {
				for tagkey, _ := range t.Tag {
					if (*tags)[i] == tagkey {
						isTag = true
						found = true
						break
					}
				}
				if found {
					break
				}
			}

			if !isTag {
				result += fmt.Sprintf("(%s[%s])", p, (*datatypes)[i])
			} else {
				p = strings.ReplaceAll(p, "'", "")
				tagConds = append(tagConds, p)
			}
		}
		result += "}"
	}

	if len(result) == 2 {
		result = "{empty}"
	}

	sort.Strings(tagConds)
	return result, tagConds
}

/*
SP 和 ST 都可以在这个函数中取到		条件判断谓词和查询时间范围
*/
func GetSPST(query string) string {
	//regStr := `(?i).+WHERE(.+)GROUP BY.`
	regStr := `(?i).+WHERE(.+)`
	conditionExpr := regexp.MustCompile(regStr)
	if ok, _ := regexp.MatchString(regStr, query); !ok {
		return "{empty}#{empty,empty}"
	}
	condExprMatch := conditionExpr.FindStringSubmatch(query) // 获取 WHERE 后面的所有表达式，包括谓词和时间范围
	parseExpr := condExprMatch[1]

	now := time.Now()
	valuer := influxql.NowValuer{Now: now}
	expr, _ := influxql.ParseExpr(parseExpr)
	cond, timeRange, _ := influxql.ConditionExpr(expr, &valuer) //提取出谓词和时间范围

	start_time := timeRange.MinTime() //获取起止时间
	end_time := timeRange.MaxTime()
	uint_start_time := start_time.UnixNano() //时间戳转换成 uint64 , 纳秒精度
	uint_end_time := end_time.UnixNano()
	string_start_time := strconv.FormatInt(uint_start_time, 10) // 转换成字符串
	string_end_time := strconv.FormatInt(uint_end_time, 10)

	// 判断时间戳合法性：19位数字，转换成字符串之后第一位是 1	时间范围是 2001-09-09 09:46:40 +0800 CST 到 2033-05-18 11:33:20 +0800 CST	（ 1 * 10^18 ~ 2 * 10^18 ns）
	if len(string_start_time) != 19 || string_start_time[0:1] != "1" {
		string_start_time = "empty"
	}
	if len(string_end_time) != 19 || string_end_time[0:1] != "1" {
		string_end_time = "empty"
	}

	var result string
	if cond == nil { //没有谓词
		result += fmt.Sprintf("{empty}#{%s,%s}", string_start_time, string_end_time)
	} else { //从语法树中找出由AND或OR连接的所有独立的谓词表达式
		var conds []string
		var tag []string
		binaryExpr := cond.(*influxql.BinaryExpr)
		var datatype []string
		_, predicates, datatypes := preOrderTraverseBinaryExpr(binaryExpr, &tag, &conds, &datatype)
		result += "{"
		for i, p := range *predicates {
			result += fmt.Sprintf("(%s[%s])", p, (*datatypes)[i])
		}
		result += "}"
		result += fmt.Sprintf("#{%s,%s}", string_start_time, string_end_time)
	}

	return result
}

/*
SemanticSegment 根据查询语句和数据库返回数据组成字段，用作存入cache的key
*/
func SemanticSegment(queryString string, response *Response) string {
	if ResponseIsEmpty(response) {
		return "{empty response}"
	}
	SP, tagPredicates := GetSP(queryString, response, TagKV)
	SM := GetSM(response, tagPredicates)
	Interval := GetInterval(queryString)
	SF, Aggr := GetSFSGWithDataType(queryString, response)

	var result string
	//result = fmt.Sprintf("%s#{%s}#%s#{%s,%s}", SM, SF, SPST, Aggr, Interval)
	result = fmt.Sprintf("%s#{%s}#%s#{%s,%s}", SM, SF, SP, Aggr, Interval)

	return result
}

// SeperateSemanticSegment 每个子表的语义段
func SeperateSemanticSegment(queryString string, response *Response) []string {

	SF, SG := GetSFSGWithDataType(queryString, response)
	SP, tagPredicates := GetSP(queryString, response, TagKV)
	SepSM := GetSeperateSM(response, tagPredicates)

	Interval := GetInterval(queryString)

	var resultArr []string
	for i := range SepSM {
		//str := fmt.Sprintf("%s#{%s}#%s#{%s,%s}", SepSM[i], SF, SPST, SG, Interval)
		str := fmt.Sprintf("%s#{%s}#%s#{%s,%s}", SepSM[i], SF, SP, SG, Interval)
		resultArr = append(resultArr, str)
	}

	return resultArr
}
