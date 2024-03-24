package client

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/InfluxDB-client/models"

	//"github.com/influxdata/influxdb1-client/models"
	"log"
	"strconv"
	"strings"
	"time"
)

func (resp *Response) ToString() string {
	var result string
	var tags []string

	tags = GetTagNameArr(resp)
	if ResponseIsEmpty(resp) {
		return "empty response"
	}

	for r := range resp.Results { //包括 Statement_id , Series[] , Messages[] , Error	只用了 Series[]

		for s := range resp.Results[r].Series { //包括  measurement name, GROUP BY tags, Columns[] , Values[][], partial		只用了 Columns[]和 Values[][]

			result += "SCHEMA "
			// 列名	Columns[] 	[]string类型
			for c := range resp.Results[r].Series[s].Columns {
				result += resp.Results[r].Series[s].Columns[c]
				result += " " //用空格分隔列名
			}

			// tags		map元素顺序输出
			for t, tag := range tags {
				result += fmt.Sprintf("%s=%s ", tags[t], resp.Results[r].Series[s].Tags[tag])
			}
			result += "\r\n" // 列名和数据间换行  "\r\n" 还是 "\n" ?	用 "\r\n", 因为 memcache 读取换行符是 CRLF("\r\n")

			for v := range resp.Results[r].Series[s].Values {
				for vv := range resp.Results[r].Series[s].Values[v] { // 从JSON转换出来之后只有 string 和 json.Number 两种类型
					if resp.Results[r].Series[s].Values[v][vv] == nil { //值为空时输出一个占位标志
						result += "_"
					} else if str, ok := resp.Results[r].Series[s].Values[v][vv].(string); ok {
						result += str
					} else if jsonNumber, ok := resp.Results[r].Series[s].Values[v][vv].(json.Number); ok {
						str := jsonNumber.String()
						result += str
						//jsonNumber.String()
					} else {
						result += "#"
					}
					result += " " // 一行 Value 的数据之间用空格分隔
				}
				result += "\r\n" // Values 之间换行
			}
			//result += "\r\n" // Series 之间换行
		}
		//result += "\r\n" // Results 之间换行
	}
	result += "end" //标志响应转换结束
	return result
}

// ResponseToByteArray 把数据库的查询结果转换为字节流
func ResponseToByteArray(resp *Response, queryString string) []byte {
	result := make([]byte, 0)

	/* 结果为空 */
	if ResponseIsEmpty(resp) {
		return StringToByteArray("empty response")
	}

	/* 获取每一列的数据类型 */
	datatypes := GetDataTypeArrayFromResponse(resp)

	/* 获取每张表单独的语义段 */
	//seperateSemanticSegment := SeperateSemanticSegment(queryString, resp)	// 已弃用
	seperateSemanticSegment := GetSeperateSemanticSegment(queryString)

	/* 每行数据的字节数 */
	bytesPerLine := BytesPerLine(datatypes)

	for i, s := range resp.Results[0].Series {
		numOfValues := len(s.Values)                                             // 表中数据行数
		bytesPerSeries, _ := Int64ToByteArray(int64(bytesPerLine * numOfValues)) // 一张表的数据的总字节数：每行字节数 * 行数

		/* 存入一张表的 semantic segment 和表内所有数据的总字节数 */
		result = append(result, []byte(seperateSemanticSegment[i])...)
		result = append(result, []byte(" ")...)
		result = append(result, bytesPerSeries...)
		//result = append(result, []byte("\r\n")...) // 每个子表的字节数 和 数据 之间的换行符

		//fmt.Printf("%s %d\r\n", seperateSemanticSegment[i], bytesPerSeries)

		/* 数据转换成字节数组，存入 */
		for _, v := range s.Values {
			for j, vv := range v {
				datatype := datatypes[j]
				tmpBytes := InterfaceToByteArray(j, datatype, vv)
				result = append(result, tmpBytes...)

			}
			/* 如果传入cache的数据之间不需要换行，就把这一行注释掉；如果cache处理数据时也没加换行符，那么从字节数组转换成结果类型的部分也要修改 */
			//result = append(result, []byte("\r\n")...) // 每条数据之后换行
		}
		/* 如果表之间需要换行，在这里添加换行符，但是从字节数组转换成结果类型的部分也要修改 */
		//result = append(result, []byte("\r\n")...) // 每条数据之后换行
	}

	return result
}

/* todo	由字节数组转换成结果类型时，在查询语句的谓词中出现的tag不应该被添加到结果类型的 Tags 中，Tags中只有 GROUP BY tag：如何区分 谓词的tag 和 GROUP BY tag
 * 在生成语义段的过程中，关于tag的谓词会被添加到SM中，而不是留在SP；（用作为全局变量的TagKV（当前数据库的所有tag及其值）判断谓词是否是tag）
 * GROUP BY tag 会和 tag 谓词一起出现在SM中
 * 如何区分两种tag：当前条件下没办法，但是可以通过调整查询语句避免这一问题：把出现在 WHRER 中的 tag 也写进 GROUP BY，让转换前后的结果中都存在多余的谓词 tag
 */
// 字节数组转换成结果类型
func ByteArrayToResponse(byteArray []byte) *Response {

	/* 没有数据 */
	if len(byteArray) == 0 {
		return nil
	}

	valuess := make([][][]interface{}, 0) // 存放不同表(Series)的所有 values
	values := make([][]interface{}, 0)    // 存放一张表的 values
	value := make([]interface{}, 0)       // 存放 values 里的一行数据

	seprateSemanticSegments := make([]string, 0) // 存放所有表各自的SCHEMA
	seriesLength := make([]int64, 0)             // 每张表的数据的总字节数

	var curSeg string        // 当前表的语义段
	var curLen int64         // 当前表的数据的总字节数
	index := 0               // byteArray 数组的索引，指示当前要转换的字节的位置
	length := len(byteArray) // Get()获取的总字节数

	/* 转换 */
	for index < length {
		/* 结束转换 */
		if index == length-2 { // 索引指向数组的最后两字节
			if byteArray[index] == 13 && byteArray[index+1] == 10 { // "\r\n"，表示Get()返回的字节数组的末尾，结束转换		Get()除了返回查询数据之外，还会在数据末尾添加一个 "\r\n",如果读到这个组合，说明到达数组末尾
				break
			} else {
				log.Fatal(errors.New("expect CRLF in the end of []byte"))
			}
		}

		/* SCHEMA行 格式如下 	SSM:包含每张表单独的tags	len:一张表的数据的总字节数 */
		//  {SSM}#{SF}#{SP}#{SG} len\r\n
		if byteArray[index] == 123 && byteArray[index+1] == 40 { // "{(" ASCII码	表示语义段的开始位置
			ssStartIdx := index
			for byteArray[index] != 32 { // ' '空格，表示语义段的结束位置的后一位
				index++
			}
			ssEndIdx := index                               // 此时索引指向 len 前面的 空格
			curSeg = string(byteArray[ssStartIdx:ssEndIdx]) // 读取所有表示语义段的字节，直接转换为字符串
			seprateSemanticSegments = append(seprateSemanticSegments, curSeg)

			index++              // 空格后面的8字节是表示一张表中数据总字节数的int64
			lenStartIdx := index // 索引指向 len 的第一个字节
			index += 8
			lenEndIdx := index // 索引指向 len 后面一位的回车符 '\r' ，再后面一位是 '\n'
			tmpBytes := byteArray[lenStartIdx:lenEndIdx]
			serLen, err := ByteArrayToInt64(tmpBytes) // 读取 len ，转换为int64
			if err != nil {
				log.Fatal(err)
			}
			curLen = serLen
			seriesLength = append(seriesLength, curLen)

			/* 如果SCHEMA和数据之间不需要换行，把这一行注释掉 */
			//index += 2 // 索引指向换行符之后的第一个字节，开始读具体数据
		}

		/* 从 curSeg 取出包含每列的数据类型的字符串sf,获取数据类型数组 */
		// 所有数据和数据类型都存放在数组中，位置是对应的
		sf := "time[int64]," // sf中去掉了time，需要再添上time，让field数量和列数对应
		messages := strings.Split(curSeg, "#")
		sf += messages[1][1 : len(messages[1])-1] // 去掉大括号，包含列名和数据类型的字符串
		datatypes := GetDataTypeArrayFromSF(sf)   // 每列的数据类型

		/* 根据数据类型转换每行数据*/
		bytesPerLine := BytesPerLine(datatypes) // 每行字节数
		lines := int(curLen) / bytesPerLine     // 数据行数
		values = nil
		for len(values) < lines { // 按行读取一张表中的所有数据
			value = nil
			for _, d := range datatypes { // 每次处理一行, 遍历一行中的所有列
				switch d { // 根据每列的数据类型选择转换方法
				case "bool":
					bStartIdx := index
					index += 1 //	索引指向当前数据的后一个字节
					bEndIdx := index
					tmp, err := ByteArrayToBool(byteArray[bStartIdx:bEndIdx])
					if err != nil {
						log.Fatal(err)
					}
					value = append(value, tmp)
					break
				case "int64":
					iStartIdx := index
					index += 8 // 索引指向当前数据的后一个字节
					iEndIdx := index
					tmp, err := ByteArrayToInt64(byteArray[iStartIdx:iEndIdx])
					if err != nil {
						log.Fatal(err)
					}
					//if i == 0 { // 第一列是时间戳，存入Response时从int64转换成字符串
					//	ts := TimeInt64ToString(tmp)
					//	value = append(value, ts)
					//} else {
					//	str := strconv.FormatInt(tmp, 10)
					//	jNumber := json.Number(str) // int64 转换成 json.Number 类型	;Response中的数字类型只有json.Number	int64和float64都要转换成json.Number
					//	value = append(value, jNumber)
					//}

					// 根据查询时设置的参数不同，时间戳可能是字符串或int64，这里暂时当作int64处理
					str := strconv.FormatInt(tmp, 10)
					jNumber := json.Number(str) // int64 转换成 json.Number 类型	;Response中的数字类型只有json.Number	int64和float64都要转换成json.Number
					value = append(value, jNumber)
					break
				case "float64":
					fStartIdx := index
					index += 8 // 索引指向当前数据的后一个字节
					fEndIdx := index
					tmp, err := ByteArrayToFloat64(byteArray[fStartIdx:fEndIdx])
					if err != nil {
						log.Fatal(err)
					}
					str := strconv.FormatFloat(tmp, 'g', -1, 64)
					jNumber := json.Number(str) // 转换成json.Number
					value = append(value, jNumber)
					break
				default: // string
					sStartIdx := index
					index += STRINGBYTELENGTH // 索引指向当前数据的后一个字节
					sEndIdx := index
					tmp := ByteArrayToString(byteArray[sStartIdx:sEndIdx])
					value = append(value, tmp) // 存放一行数据中的每一列
					break
				}
			}
			values = append(values, value) // 存放一张表的每一行数据

			/* 如果cache传回的数据之间不需要换行符，把这一行注释掉 */
			//index += 2 // 跳过每行数据之间的换行符CRLF，处理下一行数据
		}
		valuess = append(valuess, values)
	}

	/* 用 semanticSegments数组 和 values数组 还原出表结构，构造成 Response 返回 */
	modelsRows := make([]models.Row, 0)

	// {SSM}#{SF}#{SP}#{SG}
	// 需要 SSM (name.tag=value) 中的 measurement name 和 tag value
	// 需要 SF 中的列名（考虑 SG 中的聚合函数）
	// values [][]interface{} 直接插入
	for i, s := range seprateSemanticSegments {
		messages := strings.Split(s, "#")
		/* 处理 ssm */
		ssm := messages[0][2 : len(messages[0])-2] // 去掉SM两侧的 大括号和小括号
		merged := strings.Split(ssm, ",")
		nameIndex := strings.Index(merged[0], ".") // 提取 measurement name
		name := merged[0][:nameIndex]
		tags := make(map[string]string)
		/* 取出所有tag */
		for _, m := range merged {
			tag := m[nameIndex+1 : len(m)]
			eqIdx := strings.Index(tag, "=") // tag 和 value 由  "=" 连接
			if eqIdx <= 0 {                  // 没有等号说明没有tag
				break
			}
			key := tag[:eqIdx] // Response 中的 tag 结构为 map[string]string
			val := tag[eqIdx+1 : len(tag)]
			tags[key] = val // 存入 tag map
		}

		/* 处理sf 如果有聚合函数，列名要用函数名，否则用sf中的列名*/
		columns := make([]string, 0)
		sf := "time[int64]," // sf中去掉了第一列的time，还原时要添上
		sf += messages[1][1 : len(messages[1])-1]
		sg := messages[3][1 : len(messages[3])-1]
		splitSg := strings.Split(sg, ",")
		aggr := splitSg[0]                       // 聚合函数名，小写的
		if strings.Compare(aggr, "empty") != 0 { // 聚合函数不为空，列名应该是聚合函数的名字
			columns = append(columns, "time")
			columns = append(columns, aggr)
		} else { // 没有聚合函数，用正常的列名
			fields := strings.Split(sf, ",") // time[int64],randtag[string]...
			for _, f := range fields {
				idx := strings.Index(f, "[") // "[" 前面的字符串是列名，后面的是数据类型
				columnName := f[:idx]
				columns = append(columns, columnName)
			}
		}

		/* 根据一条语义段构造一个 Series */
		seriesTmp := Series{
			Name:    name,
			Tags:    tags,
			Columns: columns,
			Values:  valuess[i],
			Partial: false,
		}

		/*  转换成 models.Row 数组 */
		row := SeriesToRow(seriesTmp)
		modelsRows = append(modelsRows, row)
	}

	/* 构造返回结果 */
	result := Result{
		StatementId: 0,
		Series:      modelsRows,
		Messages:    nil,
		Err:         "",
	}
	resp := Response{
		Results: []Result{result},
		Err:     "",
	}

	return &resp
}

// InterfaceToByteArray 把查询结果的 interface{} 类型转换为 []byte
/*
	index: 数据所在列的序号，第一列的时间戳如果是字符串要先转换成 int64
	datatype: 所在列的数据类型，决定转换的方法
	value: 待转换的数据
*/
func InterfaceToByteArray(index int, datatype string, value interface{}) []byte {
	result := make([]byte, 0)

	/* 根据所在列的数据类型处理数据 */
	switch datatype {
	case "bool":
		if value != nil { // 值不为空
			bv, ok := value.(bool)
			if !ok {
				log.Fatal(fmt.Errorf("{}interface fail to convert to bool"))
			} else {
				bBytes, err := BoolToByteArray(bv)
				if err != nil {
					log.Fatal(fmt.Errorf(err.Error()))
				} else {
					result = append(result, bBytes...)
				}
			}
		} else { // 值为空
			bBytes, _ := BoolToByteArray(false)
			result = append(result, bBytes...)
		}
		break
	case "int64":
		if value != nil {
			if index == 0 { // 第一列的时间戳
				if timestamp, ok := value.(string); ok {
					tsi := TimeStringToInt64(timestamp)
					iBytes, err := Int64ToByteArray(tsi)
					if err != nil {
						log.Fatal(fmt.Errorf(err.Error()))
					} else {
						result = append(result, iBytes...)
					}
				} else if timestamp, ok := value.(json.Number); ok {
					jvi, err := timestamp.Int64()
					if err != nil {
						log.Fatal(fmt.Errorf(err.Error()))
					} else {
						iBytes, err := Int64ToByteArray(jvi)
						if err != nil {
							log.Fatal(fmt.Errorf(err.Error()))
						} else {
							result = append(result, iBytes...)
						}
					}
				} else {
					log.Fatal("timestamp fail to convert to []byte")
				}

			} else { // 除第一列以外的所有列
				jv, ok := value.(json.Number)
				if !ok {
					log.Fatal(fmt.Errorf("{}interface fail to convert to json.Number"))
				} else {
					jvi, err := jv.Int64()
					if err != nil {
						log.Fatal(fmt.Errorf(err.Error()))
					} else {
						iBytes, err := Int64ToByteArray(jvi)
						if err != nil {
							log.Fatal(fmt.Errorf(err.Error()))
						} else {
							result = append(result, iBytes...)
						}
					}
				}
			}
		} else { // 值为空时设置默认值
			iBytes, _ := Int64ToByteArray(0)
			result = append(result, iBytes...)
		}
		break
	case "float64":
		if value != nil {
			jv, ok := value.(json.Number)
			if !ok {
				log.Fatal(fmt.Errorf("{}interface fail to convert to json.Number"))
			} else {
				jvf, err := jv.Float64()
				if err != nil {
					log.Fatal(fmt.Errorf(err.Error()))
				} else {
					fBytes, err := Float64ToByteArray(jvf)
					if err != nil {
						log.Fatal(fmt.Errorf(err.Error()))
					} else {
						result = append(result, fBytes...)
					}
				}
			}
		} else {
			fBytes, _ := Float64ToByteArray(0)
			result = append(result, fBytes...)
		}
		break
	default: // string
		if value != nil {
			sv, ok := value.(string)
			if !ok {
				log.Fatal(fmt.Errorf("{}interface fail to convert to string"))
			} else {
				sBytes := StringToByteArray(sv)
				result = append(result, sBytes...)
			}
		} else {
			sBytes := StringToByteArray(string(byte(0))) // 空字符串
			result = append(result, sBytes...)
		}
		break
	}

	return result
}

// BytesPerLine 根据一行中所有列的数据类型计算转换成字节数组后一行的总字节数
func BytesPerLine(datatypes []string) int {
	bytesPerLine := 0
	for _, d := range datatypes {
		switch d {
		case "bool":
			bytesPerLine += 1
			break
		case "int64":
			bytesPerLine += 8
			break
		case "float64":
			bytesPerLine += 8
			break
		default:
			bytesPerLine += STRINGBYTELENGTH
			break
		}
	}
	return bytesPerLine
}

// 所有转换都是 小端序
func BoolToByteArray(b bool) ([]byte, error) {
	bytesBuffer := bytes.NewBuffer([]byte{})
	err := binary.Write(bytesBuffer, binary.LittleEndian, &b)
	if err != nil {
		return nil, err
	}
	return bytesBuffer.Bytes(), nil
}

func ByteArrayToBool(byteArray []byte) (bool, error) {
	if len(byteArray) != 1 {
		return false, errors.New("incorrect length of byte array, can not convert []byte to bool\n")
	}
	var b bool
	byteBuffer := bytes.NewBuffer(byteArray)
	err := binary.Read(byteBuffer, binary.LittleEndian, &b)
	if err != nil {
		return false, err
	}
	return b, nil
}

func StringToByteArray(str string) []byte {
	byteArray := make([]byte, 0, STRINGBYTELENGTH)
	byteStr := []byte(str)
	if len(byteStr) > STRINGBYTELENGTH {
		return byteStr[:STRINGBYTELENGTH]
	}
	byteArray = append(byteArray, byteStr...)
	for i := 0; i < cap(byteArray)-len(byteStr); i++ {
		byteArray = append(byteArray, 0)
	}

	return byteArray
}

func ByteArrayToString(byteArray []byte) string {
	byteArray = bytes.Trim(byteArray, string(byte(0)))
	str := string(byteArray)
	return str
}

func Int64ToByteArray(number int64) ([]byte, error) {
	byteBuffer := bytes.NewBuffer([]byte{})
	err := binary.Write(byteBuffer, binary.LittleEndian, &number)
	if err != nil {
		return nil, err
	}
	return byteBuffer.Bytes(), nil
}

func ByteArrayToInt64(byteArray []byte) (int64, error) {
	if len(byteArray) != 8 {
		return 0, errors.New("incorrect length of byte array, can not convert []byte to int64\n")
	}
	var number int64
	byteBuffer := bytes.NewBuffer(byteArray)
	err := binary.Read(byteBuffer, binary.LittleEndian, &number)
	if err != nil {
		return 0, err
	}
	return number, nil
}

func Float64ToByteArray(number float64) ([]byte, error) {
	byteBuffer := bytes.NewBuffer([]byte{})
	err := binary.Write(byteBuffer, binary.LittleEndian, &number)
	if err != nil {
		return nil, err
	}
	return byteBuffer.Bytes(), nil
}

func ByteArrayToFloat64(byteArray []byte) (float64, error) {
	if len(byteArray) != 8 {
		return 0, errors.New("incorrect length of byte array, can not canvert []byte to float64\n")
	}
	var number float64
	byteBuffer := bytes.NewBuffer(byteArray)
	err := binary.Read(byteBuffer, binary.LittleEndian, &number)
	if err != nil {
		return 0.0, err
	}
	return number, nil
}

// RFC3339 字符串转换为 int64 时间戳
func TimeStringToInt64(timestamp string) int64 {
	timeT, _ := time.Parse(time.RFC3339, timestamp)
	//numberN := timeT.UnixNano()
	numberN := timeT.Unix()

	return numberN
}

// int64 时间戳转换为 RFC3339 格式字符串	"2019-08-18T00:00:00Z"
func TimeInt64ToString(number int64) string {
	t := time.Unix(number, 0).UTC()
	timestamp := t.Format(time.RFC3339)

	return timestamp
}
