# 说明

文件中的查询语句样例的测试代码在 TestSemanticSegmentInstance() 中

客户端分为两部分：

* memcache/memcache.go : 包含修改的 Set() 、Get() , 用于和cache系统交互，存入和读取字节数组

* v2/client.go : influxdb 1.x 客户端，包含查询等功能，添加了生成语义段（semantic segment）、合并多个查询结果、客户端和cache的数据交互（influx Response结果类型和memcache字节数组之间的类型转换）的功能



各函数的详细用法参考 v2/client_test.go



### 向Go项目中导入客户端包

```
import (
    "github.com/InfluxDB-client/memcache"
    "github.com/InfluxDB-client/v2"
)
```

两部分的包名分别为 `client` 和 `memcache`, 调用方法如下：

```
query := client.NewQuery("queryString", "databaseName", "ns")
```

```
mc := memcache.New("localhost:11213")
```



### 常量和全局变量修改

在 /v2/client.go 的开头 line-36

```
// 连接数据库
var c, err = NewHTTPClient(HTTPConfig{
    Addr: "http://10.170.48.244:8086",
    //Username: username,
    //Password: password,
})

// 连接cache
var mc = memcache.New("localhost:11213")

// 数据库中所有表的tag和field
var TagKV = GetTagKV(c, MyDB)
var Fields = GetFieldKeys(c, MyDB)

// 结果转换成字节数组时string类型占用字节数
const STRINGBYTELENGTH = 25

// 数据库名称
const (
    MyDB     = "NOAA_water_database"
    username = "root"
    password = "12345678"
)
```



### 连接数据库：

```
c, err := client.NewHTTPClient(client.HTTPConfig{
    Addr: "http://localhost:8086",
    //Username: username,
    //Password: password,
})
if err != nil {
    log.Fatal(err)
}
```

需要先在服务器启动InfluxDB，在终端执行以下命令

` $influxd`

如果想在终端进行查询，执行`influx`进入InfluxDB shell；输入`use databaseName`选择要用的数据库；输入SELECT查询语句



### 从InfluxDB查询

```
query := client.NewQuery(queryString, MyDB, "ns")	//构造查询
resp, err := c.Query(query)	//查询结果
if err != nil {
    t.Errorf(err.Error())
}
```

NewQuery()的三个参数分别为：查询语句字符串、数据库名称、时间精确度

时间精确度为空时返回结果的时间戳是字符串，指定为"ns" 或 "s" 等精度时，时间戳是int64



### 连接cache系统

```
mc := memcache.New("localhost:11213")
```

常用 11211 端口，不过测试代码里用的是 11213



### client的Set()

由memcache的Set()封装而来，传入查询语句和创建的连接，由Set()进行查询并把结果处理成字节数组存入cache	client.go line-806

```
func Set(queryString string, c Client, mc *memcache.Client) error 
```



### memcache 的 Set() 和 Get()

```
semanticSegment := SemanticSegment(queryString, resp)	//用作set key的语义段
startTime, endTime := GetResponseTimeRange(resp)	//查询结果的时间范围
respCacheByte := resp.ToByteArray(tt.queryString)	//数据库查询结果转化为字节数组
tableNumbers := int64(len(resp.Results[0].Series))	//表的数量

err = mc.Set(&memcache.Item{Key: semanticSegment, Value: respCacheByte, Time_start: startTime, Time_end: endTime, NumOfTables: tableNumbers})

valueBytes, _, err := mc.Get(semanticSegment, startTime, endTime) // 接收cache返回的字节数组
```



Get(key string, start_time int64, end_time int64) (itemValues []byte, item *Item, err error) : memcache.go line-355 -> getFromAddr line-411 -> parseGetResponse line-547  (从cache读取数据的核心部分)

```
for { //每次读入一行，直到 Reader 为空
    line, err := r.ReadBytes('\n') //从查寻结果中读取一行, 换行符对应的字节码是 10， 如果数据中有 int64 类型的 10，读取时会把数字错误当作换行符,不过对结果的字节数组没有影响，不用处理
    if err != nil {
       return err
    }
    if bytes.Equal(line, resultEnd) { // get 命令查询结束后会在数据末尾添加 "END", 如果读到 END，说明数据读取完毕
       return nil
    }
    it := new(Item)
    it.Value = line // 读取value的值
    if err != nil {
       it.Value = nil
       return err
    }
    cb(it)
    *itemValues = append(*itemValues, it.Value...)
}
```



Set(item \*Item) error : memcache.go line-638 -> (*Client).set line-642  -> populateOne() line-708

```
_, err = fmt.Fprintf(rw, "%s %s %d %d %d\r\n", //用item的基本信息构建命令的第一行，存入rw buffer 如：set user 0 0 3
    verb, item.Key, len(item.Value), item.Time_start, item.Time_end) // set key len st et  memcache的set格式暂时是这样
//_, err = fmt.Fprintf(rw, "%s %s %d %d %d\r\n",
//  verb, item.Key, item.Time_start, item.Time_end, item.NumOfTables)  //最终格式是这样的（大概），最后一个参数是结果中表的数量
```



### 客户端查询结果的结构（Response）

```
[	-	-	-	>	Response

​	Results[]	{	-	-	-	-	->	根据 statement_id 区别；InfluxDB不再支持一次执行多条查询，所以只有一个Results[0]

​							statement_id	int	（从 0 开始计数）

​							Series[]	[	-	-	->	series[0]	(查询结果的表，比如用 GROUP BY 时会有多个Series，根据 tags 值的数量)

​													name			string

​													tags			map[string]string	-	-	-	->	不用 GROUP BY 时为空

​													columns			[]string

​													Values{		  	[] [] interface{}

​																		field_value[0]

​																		......

​															}

​													partial			bool

​										]

​										[	-	-	->	series[1]
													......
​										]

​										.......

​							message[]	{	（正常为  “[]” ）

​													Level		string

​													Text	  	string		 

​										}

​							Error	( 正常没有 )

​				}

​	Error	（正常没有）	

]
```

示例：

```
[    
	{   0   -   -   -   -   -   -   -   -   -   ->  statement_id        
		[   -   -   -   -   -   -   -   -   -   ->  series            
			{   h2o_pH  -   -   -   -   -   -   ->  measurement name                
				map[location:coyote_creek]  -   ->  GROUPE BY tags                
				[time pH]   -   -   -   -   -   ->  columns                
				[   -   -   -   -   -   -   -   ->  values                    
					[2019-08-17T00:00:00Z 7]                     
					[2019-08-17T00:06:00Z 8]                     
					[2019-08-17T00:12:00Z 8]                     
					[2019-08-17T00:18:00Z 7]                     
					[2019-08-17T00:24:00Z 7]                
				]                 
				false   -   -   -   -   -   -   ->  partial             
			}             
			{   h2o_pH                  
				map[location:santa_monica]  -   ->  GROUPE BY tags                
				[time pH]                 
				[                    
					[2019-08-17T00:00:00Z 6]                     
                    [2019-08-17T00:06:00Z 6]                     
                    [2019-08-17T00:12:00Z 6]                     
                    [2019-08-17T00:18:00Z 7]                     
                    [2019-08-17T00:24:00Z 8]                
				]                 
				false            
			}       
		]         
		[]  -   -   -   -   -   -   -   -   -   ->  message    
	}
]
```



### 获取语义段(semantic segment)

```
// v2/client.go line-1166
func SemanticSegment(queryString string, response *Response) string 
```

根据查询语句和数据库返回数据组成字段，用作存入cache的key	{SM}#{SF}#{SP}#{SG}

```
ss := SemanticSegment(queryString, response)
```

结果：测试代码	 v2/client_test.go	line-1678

```
"{(h2o_quality.location=coyote_creek,h2o_quality.randtag=2)(h2o_quality.location=coyote_creek,h2o_quality.randtag=3)(h2o_quality.location=santa_monica,h2o_quality.randtag=2)(h2o_quality.location=santa_monica,h2o_quality.randtag=3)}#{time[int64],index[int64]}#{(index>=50[int64])}#{max,12m}"
```



### 合并查询结果

```
// v2/client.go line-796
func Merge(precision string, resps ...*Response) []*Response 
```

precision：允许合并的时间误差精度	"h"/"ns"/"us" 等

resps ... : 要合并的多个查询结果，可以整合成一个数组，也可以直接挨个传入

返回合并后的结果数组

先按照结果数据的起止时间排序，然后遍历合并

用法：详细过程看测试代码	 v2/client_test.go	line-2923

```
merged := Merge("h", resps...)
```



### Response转化为字节数组

```
// v2/client.go line-1714
func (resp *Response) ToByteArray(queryString string) []byte
```

格式：

```
"{(h2o_quality.location=coyote_creek,h2o_quality.randtag=1)}#{time[int64],index[int64],location[string],randtag[string]}#{(location='coyote_creek'[string])}#{empty,empty} [0 0 0 0 0 0 0 198]\r\n" +
    "[1566086760000000000 66 coyote_creek 1]\r\n" +
    "[1566087480000000000 91 coyote_creek 1]\r\n" +
    "[1566087840000000000 29 coyote_creek 1]\r\n" +
"{(h2o_quality.location=coyote_creek,h2o_quality.randtag=2)}#{time[int64],index[int64],location[string],randtag[string]}#{(location='coyote_creek'[string])}#{empty,empty} [0 0 0 0 0 0 0 66]\r\n" +
    "[1566087120000000000 78 coyote_creek 2]\r\n" +
"{(h2o_quality.location=coyote_creek,h2o_quality.randtag=3)}#{time[int64],index[int64],location[string],randtag[string]}#{(location='coyote_creek'[string])}#{empty,empty} [0 0 0 0 0 0 0 132]\r\n" +
    "[1566086400000000000 85 coyote_creek 3]\r\n" +
    "[1566088200000000000 75 coyote_creek 3]\r\n"
```

转换成字节数组的数据之间暂时加上了换行符，如果不用的话把下面一行代码注释掉：

```
/* 如果传入cache的数据之间不需要换行，就把这一行注释掉 */
result = append(result, []byte("\r\n")...) // 每条数据之后换行
```

测试代码	 v2/client_test.go	line-3561



### 字节数组转换成Response

```
// v2/client.go line-1765
func ByteArrayToResponse(byteArray []byte) *Response 
```

如果返回的字节数组的数据之间没有换行，把下面一行注释掉：

```
/* 如果cache传回的数据之间不需要换行符，把这一行注释掉 */
index += 2 // 跳过每行数据之间的换行符CRLF，处理下一行数据
```

测试代码	 v2/client_test.go	line-3636

