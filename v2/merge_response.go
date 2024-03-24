package client

import (
	"fmt"
	"github.com/InfluxDB-client/models"
	"log"

	//"github.com/influxdata/influxdb1-client/models"
	"slices"
	"strings"
	"time"
)

// Series 用于合并结果时暂时存储合并好的数据，合并完成后替换到结果中
type Series struct {
	Name    string            // measurement name
	Tags    map[string]string // GROUP BY tags
	Columns []string          // column name
	Values  [][]interface{}   // specific query results
	Partial bool              // useless (false)
}

// RespWithTimeRange 用于对结果排序的结构体
type RespWithTimeRange struct {
	resp      *Response
	startTime int64
	endTime   int64
}

/*
Merge
Lists:
todo	是否需要查询语句和结果的映射(?), 需要的话合并之后怎么映射(?)
done	传入的表是乱序的，需要 排序 或 两次遍历，让表按升序正确合并
done	按照升序，合并碎片化的查询结果，如 [1,25] , [27,50] 合并， 设置一个合理的时间误差，在误差内的表可以合并 ( 1ms ? )
done	传入参数是查询结果的表 (?) ,数量任意(?)或者是表的数组; 返回值是表的数组(?);
done	合并过程：需要比较每张表的起止时间，按照时间升序（越往下时间越大（越新）），如果两张表的起止时间在误差范围内，则合并;（不考虑时间范围重合的情况）
done	按照 GROUP BY tag value 区分同一个查询的不同的表，合并时两个查询的表分别合并；多 tag 如何处理(?)
done	表合并：能否直接从 Response 结构中合并(?)
done	查询结果中的表按照tag值划分，不同表的起止时间可能不同(?)
done	把两个查询结果的所有表合并，是否可以只比较第一张表的起止时间，如果这两张表可以合并，就认为两个查询的所有表都可以合并 (?)
*/
func Merge(precision string, resps ...*Response) []*Response {
	var results []*Response
	var resp1 *Response
	var resp2 *Response
	var respTmp *Response

	/* 没有两个及以上查询的结果，不需要合并 */
	if len(resps) <= 1 {
		return resps
	}

	/* 设置允许合并的时间误差范围 */
	duration, err := time.ParseDuration(precision)
	if err != nil {
		log.Fatalln(err)
	}
	timeRange := int64(duration.Seconds())

	/* 按时间排序，去除空的结果 */
	resps = SortResponses(resps)
	if len(resps) <= 1 {
		return resps
	}

	/* 合并 		经过排序处理后必定有两个以上的结果需要合并 */
	index := 0      // results 数组索引，指向最后一个元素
	merged := false // 标志是否成功合并
	results = append(results, resps[0])
	for _, resp := range resps[1:] {
		resp1 = results[index]
		resp2 = resp

		/* 获取结果的起止时间		结果不为空，则必定有起止时间，二者可能相等（只有一条数据时） */
		st1, et1 := GetResponseTimeRange(resp1)
		st2, et2 := GetResponseTimeRange(resp2)

		/* 判断是否可以合并，以及哪个在前面 */
		if et1 <= st2 { // 1在2前面
			if st2-et1 <= timeRange {
				respTmp = MergeResultTable(resp1, resp2)
				merged = true
				results[index] = respTmp // results中的1用合并后的1替换
			}
		} else if et2 <= st1 { // 2在1前面
			if st1-et2 <= timeRange {
				respTmp = MergeResultTable(resp2, resp1)
				merged = true
				results[index] = respTmp // 替换
			}
		}

		/* 不能合并，直接把2放入results数组，索引后移		由于结果提前排好序了，接下来用2比较能否合并 */
		if !merged {
			results = append(results, resp2)
			index++
		}

		merged = false

	}

	return results
}

// SortResponses 传入一组查询结果，构造成用于排序的结构体，对不为空的结果按时间升序进行排序，返回结果数组
func SortResponses(resps []*Response) []*Response {
	var results []*Response
	respArrTmp := make([]RespWithTimeRange, 0)

	/* 用不为空的结果构造用于排序的结构体数组 */
	for _, resp := range resps {
		if !ResponseIsEmpty(resp) {
			st, et := GetResponseTimeRange(resp)
			rwtr := RespWithTimeRange{resp, st, et}
			respArrTmp = append(respArrTmp, rwtr)
		}
	}

	/* 排序，提取出结果数组 */
	respArrTmp = SortResponseWithTimeRange(respArrTmp)
	for _, rt := range respArrTmp {
		results = append(results, rt.resp)
	}

	return results
}

// SortResponseWithTimeRange 用起止时间为一组查询结果排序 	冒泡排序
func SortResponseWithTimeRange(rwtr []RespWithTimeRange) []RespWithTimeRange {
	n := len(rwtr)
	for i := 0; i < n-1; i++ {
		for j := 0; j < n-i-1; j++ {
			if rwtr[j].startTime >= rwtr[j+1].endTime { // 大于等于
				rwtr[j], rwtr[j+1] = rwtr[j+1], rwtr[j]
			}
		}
	}
	return rwtr
}

// SeriesToRow 转换成可以替换到结果中的结构体
func SeriesToRow(ser Series) models.Row {
	return models.Row{
		Name:    ser.Name,
		Tags:    ser.Tags,
		Columns: ser.Columns,
		Values:  ser.Values,
		Partial: ser.Partial,
	}
}

// GetSeriesTagsMap （表按字典序排列）获取一个结果（Response）中的所有表（Series）的所有tag（ map[string]string ）
func GetSeriesTagsMap(resp *Response) map[int]map[string]string {
	tagsMap := make(map[int]map[string]string)

	/* 没用 GROUP BY 的话只会有一张表 */
	if !ResponseIsEmpty(resp) {
		/* 在结果中，多个表（[]Series）本身就是按照字典序排列的，直接读取就能保持顺序 */
		for i, ser := range resp.Results[0].Series {
			tagsMap[i] = ser.Tags
		}
	}

	return tagsMap
}

// TagsMapToString 按字典序把一张表中的所有tags组合成字符串
func TagsMapToString(tagsMap map[string]string) string {
	var str string
	tagKeyArr := make([]string, 0)

	if len(tagsMap) == 0 {
		return ""
	}
	/* 获取所有tag的key，按字典序排序 */
	for k, _ := range tagsMap {
		tagKeyArr = append(tagKeyArr, k)
	}
	slices.Sort(tagKeyArr)

	/* 根据排序好的key从map中获取value，组合成字符串 */
	for _, s := range tagKeyArr {
		str += fmt.Sprintf("%s=%s ", s, tagsMap[s])
	}

	/* 去掉末尾的空格，好像无所谓 */
	//str = str[:len(str)-1]

	return str
}

// MergeSeries 多表合并的关键部分，合并两个结果中的所有表的结构	有些表可能是某个结果独有的
func MergeSeries(resp1, resp2 *Response) []Series {
	resSeries := make([]Series, 0)
	tagsMapMerged := make(map[int]map[string]string)

	/* 分别获取所有表的所有tag，按字典序排列 */
	tagsMap1 := GetSeriesTagsMap(resp1)
	tagsMap2 := GetSeriesTagsMap(resp2)

	/* 考虑到cache的数据转换回来之后冗余tag的情况，获取结果中有效的tag */
	resTagArr := make([]string, 0)
	if len(tagsMap1[0]) <= len(tagsMap2[0]) {
		for k := range tagsMap1[0] {
			resTagArr = append(resTagArr, k)
		}
	} else {
		for k := range tagsMap2[0] {
			resTagArr = append(resTagArr, k)
		}
	}

	/* 没用 GROUP BY 时长度为 1，map[0:map[]] */
	len1 := len(tagsMap1)
	len2 := len(tagsMap2)

	/* 把一个结果的所有表的所有tag组合成一个字符串 */
	var str1 string
	var str2 string
	for _, v := range tagsMap1 {
		str1 += TagsMapToString(v) // 组合一张表的所有tags（字典序）
		str1 += ";"                // 区分不同的表
	}
	for _, v := range tagsMap2 {
		str2 += TagsMapToString(v)
		str2 += ";"
	}

	/* 合并两个查询结果的所有表	两个结果的表的数量未必相等，位置未必对应，需要判断是否是同一张表	同一张表的tag值都相等 */
	index1 := 0 // 标识表的索引
	index2 := 0
	indexAll := 0
	same := false                        // 判断两个结果中的两张表是否可以合并
	for index1 < len1 || index2 < len2 { // 遍历两个结果中的所有表
		/* 在其中一个结果的所有表都已经存入之前，需要判断两张表是否相同 */
		if index1 < len1 && index2 < len2 {
			same = true

			/* 分别把当前表的tags组合成字符串，若两字符串相同，说明是同一张表；否则说明有tag的值不同，不是同一张表（两种情况：结果1独有的表、结果2独有的表） */
			tagStr1 := TagsMapToString(tagsMap1[index1])
			tagStr2 := TagsMapToString(tagsMap2[index2])

			/* 不是同一张表 */
			//if strings.Compare(tagStr1, tagStr2) != 0 {
			if !strings.Contains(tagStr1, tagStr2) && !strings.Contains(tagStr2, tagStr1) { // 考虑从cache转换回来的结果可能会有多余的谓词tag，改用是不是子串判断
				if !strings.Contains(str2, tagStr1) { // 表示结果2中没有结果1的这张表，只把这张表添加到合并结果中
					tmpMap := make(map[string]string)
					for k, v := range tagsMap1[index1] { // 找出有效的tag
						if slices.Contains(resTagArr, k) {
							tmpMap[k] = v
						}
					}
					//tagsMapMerged[indexAll] = tagsMap1[index1]
					tagsMapMerged[indexAll] = tmpMap
					index1++
					indexAll++
				} else if !strings.Contains(str1, tagStr2) { // 结果2独有的表
					tmpMap := make(map[string]string)
					for k, v := range tagsMap2[index2] {
						if slices.Contains(resTagArr, k) {
							tmpMap[k] = v
						}
					}
					//tagsMapMerged[indexAll] = tagsMap2[index2]
					tagsMapMerged[indexAll] = tmpMap
					index2++
					indexAll++
				}
				same = false
			}

			/* 是同一张表 */
			/*if same {
				tagsMapMerged[indexAll] = tagsMap1[index1]
				index1++ // 两张表的索引都要后移
				index2++
				indexAll++
			}*/
			if same {
				if strings.Contains(tagStr1, tagStr2) {
					tagsMapMerged[indexAll] = tagsMap2[index2] // 考虑到多余的tag，选用tag数量少的作为合并结果的tag
					index1++
					index2++
					indexAll++
				} else {
					tagsMapMerged[indexAll] = tagsMap1[index1]
					index1++
					index2++
					indexAll++
				}
			}

		} else if index1 == len1 && index2 < len2 { // 只剩结果2的表了
			tmpMap := make(map[string]string)
			for k, v := range tagsMap2[index2] { // 找出有效的tag
				if slices.Contains(resTagArr, k) {
					tmpMap[k] = v
				}
			}
			//tagsMapMerged[indexAll] = tagsMap2[index2]
			tagsMapMerged[indexAll] = tmpMap
			index2++
			indexAll++
		} else if index1 < len1 && index2 == len2 { // 只剩结果1的表了
			tmpMap := make(map[string]string)
			for k, v := range tagsMap1[index1] {
				if slices.Contains(resTagArr, k) {
					tmpMap[k] = v
				}
			}
			//tagsMapMerged[indexAll] = tagsMap1[index1]
			tagsMapMerged[indexAll] = tmpMap
			index1++
			indexAll++
		}
	}

	/* 根据前面表合并的结果，构造Series结构 */
	var tagsStrArr []string // 所有表的tag字符串数组
	for i := 0; i < indexAll; i++ {
		tmpSeries := Series{
			Name:    resp1.Results[0].Series[0].Name, // 这些参数都从结果中获取
			Tags:    tagsMapMerged[i],                // 合并后的tag map
			Columns: resp1.Results[0].Series[0].Columns,
			Values:  make([][]interface{}, 0),
			Partial: resp1.Results[0].Series[0].Partial,
		}
		resSeries = append(resSeries, tmpSeries)
		tagsStrArr = append(tagsStrArr, TagsMapToString(tmpSeries.Tags))
	}
	slices.Sort(tagsStrArr) //对tag字符串数组按字典序排列

	/* 根据排序后的tag字符串数组对表结构排序 */
	sortedSeries := make([]Series, 0)
	for i := range tagsStrArr { // 即使tag是空串也是有长度的，不用担心数组越界
		for j := range resSeries { // 遍历所有表，找到应该在当前索引位置的
			s := TagsMapToString(resSeries[j].Tags)
			if strings.Compare(s, tagsStrArr[i]) == 0 {
				sortedSeries = append(sortedSeries, resSeries[j])
				break
			}
		}
	}

	return sortedSeries
}

// MergeResultTable 	2 合并到 1 后面，返回 1
func MergeResultTable(resp1, resp2 *Response) *Response {
	respRow := make([]models.Row, 0)

	/* 获取合并而且排序的表结构 */
	mergedSeries := MergeSeries(resp1, resp2)

	len1 := len(resp1.Results[0].Series)
	len2 := len(resp2.Results[0].Series)

	index1 := 0
	index2 := 0

	/* 对于没用 GROUP BY 的查询结果，直接把数据合并之后返回一张表 */
	/* 根据表结构向表中添加数据 	数据以数组形式存储，直接添加到数组末尾即可*/
	for _, ser := range mergedSeries {
		/* 先从结果1的相应表中存入数据 不是相同的表就直接跳过*/
		if index1 < len1 && strings.Compare(TagsMapToString(resp1.Results[0].Series[index1].Tags), TagsMapToString(ser.Tags)) == 0 {
			ser.Values = append(ser.Values, resp1.Results[0].Series[index1].Values...)
			index1++
		}
		/* 再从结果2的相应表中存入数据 */
		if index2 < len2 && strings.Compare(TagsMapToString(resp2.Results[0].Series[index2].Tags), TagsMapToString(ser.Tags)) == 0 {
			ser.Values = append(ser.Values, resp2.Results[0].Series[index2].Values...)
			index2++
		}
		// 转换成能替换到结果中的结构
		respRow = append(respRow, SeriesToRow(ser))
	}

	/* 合并结果替换到结果1中 */
	resp1.Results[0].Series = respRow

	return resp1
}
