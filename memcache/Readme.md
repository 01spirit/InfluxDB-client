把 memcache go client 修改为适用于 改版fatcache 的客户端

大致修改：

memcache.go 中的几个函数



```
legalKey()	由Get()调用
取消了key的长度限制（原为250字节）
取消了对空格符的非法判定，使用时直接把多个key连同空格作为一个string一起传入

```



```
Get()
传入参数中增加了 start_time uint64, end_time uint64 ，用于查询时间范围
返回值增加了 itemValues []string ，把从fatcache终端读取到的结果直接按行存储在string数组中
```



```
getFromAddr()	由Get()调用
传入参数增加了 start_time uint64, end_time uint64, itemValues *[]string
```



```
parseGetResponse() 	由getFromAddr()调用
传入参数增加了itemValues *[]string
直接从终端根据换行符读取每一行数据，遇到 "END\r\n" 结束读取
结果存入string数组中返回到Get()
	*itemValues = append(*itemValues, string(it.Value))
```