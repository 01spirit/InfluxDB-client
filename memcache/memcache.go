/*
Copyright 2011 The gomemcache AUTHORS

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package memcache provides a client for the memcached cache server.		memcache包为缓存服务器提供客户端
package memcache

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Similar to:
// https://godoc.org/google.golang.org/appengine/memcache

var (
	// ErrCacheMiss means that a Get failed because the item wasn't present.	//未匹配
	ErrCacheMiss = errors.New("memcache: cache miss")

	// ErrCASConflict means that a CompareAndSwap call failed due to the
	// cached value being modified between the Get and the CompareAndSwap.
	// If the cached value was simply evicted rather than replaced,
	// ErrNotStored will be returned instead.
	ErrCASConflict = errors.New("memcache: compare-and-swap conflict")

	// ErrNotStored means that a conditional write operation (i.e. Add or
	// CompareAndSwap) failed because the condition was not satisfied.		//带条件的写，条件不符合
	ErrNotStored = errors.New("memcache: item not stored")

	// ErrServer means that a server error occurred.
	ErrServerError = errors.New("memcache: server error")

	// ErrNoStats means that no statistics were available.
	ErrNoStats = errors.New("memcache: no statistics available")

	// ErrMalformedKey is returned when an invalid key is used.
	// Keys must be at maximum 250 bytes long and not
	// contain whitespace or control characters.				// key的格式不对，过长或包含非法字符
	ErrMalformedKey = errors.New("malformed: key is too long or contains invalid characters")

	// ErrNoServers is returned when no servers are configured or available.	//没有可用的服务器
	ErrNoServers = errors.New("memcache: no servers configured or available")
)

const (
	// DefaultTimeout is the default socket read/write timeout.		默认超时时间
	DefaultTimeout = 500 * time.Millisecond

	// DefaultMaxIdleConns is the default maximum number of idle connections	默认最多有两个闲置的连接
	// kept for any single address.
	DefaultMaxIdleConns = 2
)

const buffered = 8 // arbitrary buffered channel size, for readability

// resumableError returns true if err is only a protocol-level cache error.
// This is used to determine whether a server connection should
// be re-used or not. If an error occurs, by default we don't reuse the
// connection, unless it was just a cache error.			连接可重新使用
func resumableError(err error) bool {
	switch err {
	case ErrCacheMiss, ErrCASConflict, ErrNotStored, ErrMalformedKey:
		return true
	}
	return false
}

// 检查 key 是否合法
func legalKey(key string) bool {
	//if len(key) > 250 { //长度小于等于 250 字节
	//	return false
	//}
	//for i := 0; i < len(key); i++ { //不能有空格符以及控制符	key[i] <= ' ' : ASCII码值小于等于32的控制字符     0x7f = 127  (删除符)
	//	if key[i] <= ' ' || key[i] == 0x7f { //key不能包含这 33 个控制字符中的任何一个
	//		return false
	//	}
	//}

	for i := 0; i < len(key); i++ { //不能有控制符	key[i] < ' ' : ASCII码值小于32的控制字符     0x7f = 127  (删除符)
		if key[i] < ' ' || key[i] == 0x7f { //key不能包含这 33 个控制字符中的任何一个
			return false
		}
	}
	return true
}

// memcache返回的结果中包含的的字符串
var (
	crlf            = []byte("\r\n")
	space           = []byte(" ")
	resultOK        = []byte("OK\r\n")
	resultStored    = []byte("STORED\r\n")
	resultNotStored = []byte("NOT_STORED\r\n")
	resultExists    = []byte("EXISTS\r\n")
	resultNotFound  = []byte("NOT_FOUND\r\n")
	resultDeleted   = []byte("DELETED\r\n")
	resultEnd       = []byte("END\r\n")
	resultOk        = []byte("OK\r\n")
	resultTouched   = []byte("TOUCHED\r\n")

	resultClientErrorPrefix = []byte("CLIENT_ERROR ")
	versionPrefix           = []byte("VERSION")
)

// New returns a memcache client using the provided server(s)
// with equal weight. If a server is listed multiple times,
// it gets a proportional amount of weight.
func New(server ...string) *Client { //允许传入任意数量的server ， string类型 ， 返回创建的Client的指针
	ss := new(ServerList)
	err := ss.SetServers(server...)
	if err != nil {
		return nil
	}
	return NewFromSelector(ss)
}

// NewFromSelector returns a new Client using the provided ServerSelector.
func NewFromSelector(ss ServerSelector) *Client {
	return &Client{selector: ss}
}

// Client is a memcache client.
// It is safe for unlocked use by multiple concurrent goroutines.
type Client struct {
	// DialContext connects to the address on the named network using the
	// provided context.
	//
	// To connect to servers using TLS (memcached running with "--enable-ssl"),
	// use a DialContext func that uses tls.Dialer.DialContext. See this
	// package's tests as an example.
	DialContext func(ctx context.Context, network, address string) (net.Conn, error)

	// Timeout specifies the socket read/write timeout.
	// If zero, DefaultTimeout is used.
	Timeout time.Duration

	// MaxIdleConns specifies the maximum number of idle connections that will
	// be maintained per address. If less than one, DefaultMaxIdleConns will be
	// used.
	//
	// Consider your expected traffic rates and latency carefully. This should
	// be set to a number higher than your peak parallel requests.
	MaxIdleConns int

	selector ServerSelector

	lk       sync.Mutex
	freeconn map[string][]*conn
}

// Item is an item to be got or stored in a memcached server.
type Item struct {
	// Key is the Item's key (250 bytes maximum).
	Key string

	// Value is the Item's value.
	Value []byte

	// Flags are server-opaque flags whose semantics are entirely
	// up to the app.
	Flags uint32

	// Expiration is the cache expiration time, in seconds: either a relative
	// time from now (up to 1 month), or an absolute Unix epoch time.
	// Zero means the Item has no expiration time.
	Expiration int32

	// CasID is the compare and swap ID.
	//
	// It's populated by get requests and then the same value is
	// required for a CompareAndSwap request to succeed.			它由get请求填充，然后CompareAndSwap请求需要相同的值才能成功。
	CasID uint64

	Time_start int64

	Time_end int64

	NumOfTables int64
}

// get/set key 1 2 3 12 34
//val

// conn is a connection to a server.
type conn struct {
	nc   net.Conn
	rw   *bufio.ReadWriter
	addr net.Addr
	c    *Client
}

// release returns this connection back to the client's free pool
func (cn *conn) release() {
	cn.c.putFreeConn(cn.addr, cn)
}

func (cn *conn) extendDeadline() {
	cn.nc.SetDeadline(time.Now().Add(cn.c.netTimeout()))
}

// condRelease releases this connection if the error pointed to by err
// is nil (not an error) or is only a protocol level error (e.g. a
// cache miss).  The purpose is to not recycle TCP connections that
// are bad.
func (cn *conn) condRelease(err *error) {
	if *err == nil || resumableError(*err) {
		cn.release()
	} else {
		cn.nc.Close()
	}
}

func (c *Client) putFreeConn(addr net.Addr, cn *conn) {
	c.lk.Lock()
	defer c.lk.Unlock()
	if c.freeconn == nil {
		c.freeconn = make(map[string][]*conn)
	}
	freelist := c.freeconn[addr.String()]
	if len(freelist) >= c.maxIdleConns() {
		cn.nc.Close()
		return
	}
	c.freeconn[addr.String()] = append(freelist, cn)
}

func (c *Client) getFreeConn(addr net.Addr) (cn *conn, ok bool) {
	c.lk.Lock()
	defer c.lk.Unlock()
	if c.freeconn == nil {
		return nil, false
	}
	freelist, ok := c.freeconn[addr.String()]
	if !ok || len(freelist) == 0 {
		return nil, false
	}
	cn = freelist[len(freelist)-1]
	c.freeconn[addr.String()] = freelist[:len(freelist)-1]
	return cn, true
}

func (c *Client) netTimeout() time.Duration {
	if c.Timeout != 0 {
		return c.Timeout
	}
	return DefaultTimeout
}

func (c *Client) maxIdleConns() int {
	if c.MaxIdleConns > 0 {
		return c.MaxIdleConns
	}
	return DefaultMaxIdleConns
}

// ConnectTimeoutError is the error type used when it takes
// too long to connect to the desired host. This level of
// detail can generally be ignored.
type ConnectTimeoutError struct {
	Addr net.Addr
}

func (cte *ConnectTimeoutError) Error() string {
	return "memcache: connect timeout to " + cte.Addr.String()
}

func (c *Client) dial(addr net.Addr) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.netTimeout())
	defer cancel()

	dialerContext := c.DialContext
	if dialerContext == nil {
		dialer := net.Dialer{
			Timeout: c.netTimeout(),
		}
		dialerContext = dialer.DialContext
	}

	nc, err := dialerContext(ctx, addr.Network(), addr.String())
	if err == nil {
		return nc, nil
	}

	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return nil, &ConnectTimeoutError{addr}
	}

	return nil, err
}

// 连接
func (c *Client) getConn(addr net.Addr) (*conn, error) {
	cn, ok := c.getFreeConn(addr)
	if ok {
		cn.extendDeadline()
		return cn, nil
	}
	nc, err := c.dial(addr)
	if err != nil {
		return nil, err
	}
	cn = &conn{
		nc:   nc,
		addr: addr,
		rw:   bufio.NewReadWriter(bufio.NewReader(nc), bufio.NewWriter(nc)),
		c:    c,
	}
	cn.extendDeadline()
	return cn, nil
}

// 第二个参数是传入的函数，例如写入Item时传入 (*Client).set 方法
func (c *Client) onItem(item *Item, fn func(*Client, *bufio.ReadWriter, *Item) error) error {
	addr, err := c.selector.PickServer(item.Key) //根据key选择服务器地址	ntw == "tcp"	addr == "127.0.0.1:11211"
	if err != nil {
		return err
	}
	cn, err := c.getConn(addr) //连接
	if err != nil {
		return err
	}
	defer cn.condRelease(&err)
	if err = fn(c, cn.rw, item); err != nil { //执行作为参数传入的函数
		return err
	}
	return nil
}

func (c *Client) FlushAll() error {
	return c.selector.Each(c.flushAllFromAddr)
}

// Get gets the item for the given key. ErrCacheMiss is returned for a
// memcache cache miss. The key must be at most 250 bytes in length.
func (c *Client) Get(key string, start_time int64, end_time int64) (itemValues []byte, item *Item, err error) {
	err = c.withKeyAddr(key, func(addr net.Addr) error {
		return c.getFromAddr(addr, key, start_time, end_time, &itemValues, func(it *Item) { item = it })
	})
	if err == nil && item == nil {
		err = ErrCacheMiss
	}
	return
}

// Touch updates the expiry for the given key. The seconds parameter is either
// a Unix timestamp or, if seconds is less than 1 month, the number of seconds
// into the future at which time the item will expire. Zero means the item has
// no expiration time. ErrCacheMiss is returned if the key is not in the cache.
// The key must be at most 250 bytes in length.
func (c *Client) Touch(key string, seconds int32) (err error) {
	return c.withKeyAddr(key, func(addr net.Addr) error {
		return c.touchFromAddr(addr, []string{key}, seconds)
	})
}

// 这几个方法都是用来选择服务器的，得到服务器地址addr之后执行作为参数传入的函数
func (c *Client) withKeyAddr(key string, fn func(net.Addr) error) (err error) {
	if !legalKey(key) {
		return ErrMalformedKey
	}
	addr, err := c.selector.PickServer(key) //addr == "tcp" "127.0.0.1:11211"
	if err != nil {
		return err
	}
	return fn(addr)
}

func (c *Client) withAddrRw(addr net.Addr, fn func(*bufio.ReadWriter) error) (err error) {
	cn, err := c.getConn(addr)
	if err != nil {
		return err
	}
	defer cn.condRelease(&err)
	return fn(cn.rw)
}

func (c *Client) withKeyRw(key string, fn func(*bufio.ReadWriter) error) error {
	return c.withKeyAddr(key, func(addr net.Addr) error {
		return c.withAddrRw(addr, fn)
	})
}

/*
memcache命令：
gets user
执行结果：
VALUE user 0 3 101
zyx
END
*/
func (c *Client) getFromAddr(addr net.Addr, key string, start_time int64, end_time int64, itemValues *[]byte, cb func(*Item)) error {
	return c.withAddrRw(addr, func(rw *bufio.ReadWriter) error {
		/*if _, err := fmt.Fprintf(rw, "get %s t %d %d\r\n", strings.Join(keys, " "), start_time, end_time); err != nil { //使用 gets 查询，除了获取key的flag及value以外，额外多获取一个cas unique id的值
			return err
		}*/
		if _, err := fmt.Fprintf(rw, "get %s %d %d\r\n", key, start_time, end_time); err != nil {
			return err
		}
		if err := rw.Flush(); err != nil {
			return err
		} //向memcache写入上面格式化得到的命令并执行
		if err := parseGetResponse(rw.Reader, itemValues, cb); err != nil { //解析查询结果，存入item
			return err
		}
		return nil
	})
}

// flushAllFromAddr send the flush_all command to the given addr
func (c *Client) flushAllFromAddr(addr net.Addr) error {
	return c.withAddrRw(addr, func(rw *bufio.ReadWriter) error {
		if _, err := fmt.Fprintf(rw, "flush_all\r\n"); err != nil { //向rw buffer中写入"flush_all"
			return err
		}
		if err := rw.Flush(); err != nil { //把rw缓存中的字符写到Memcache命令中
			return err
		}
		line, err := rw.ReadSlice('\n') //如果flush_all命令执行成功，Memcache会返回 OK，否则返回 ERROR ，用 line 接收返回信息
		if err != nil {
			return err
		}
		switch {
		case bytes.Equal(line, resultOk): //返回 OK
			break
		default:
			return fmt.Errorf("memcache: unexpected response line from flush_all: %q", string(line))
		}
		return nil
	})
}

// ping sends the version command to the given addr
func (c *Client) ping(addr net.Addr) error {
	return c.withAddrRw(addr, func(rw *bufio.ReadWriter) error {
		if _, err := fmt.Fprintf(rw, "version\r\n"); err != nil { //向rw buffer写入 version 命令，如果连接成功，会返回版本信息: VERSION 1.6.14
			return err
		}
		if err := rw.Flush(); err != nil {
			return err
		} //向Memcache写入命令
		line, err := rw.ReadSlice('\n') //读取命令执行结果	，如: VERSION 1.6.14
		if err != nil {
			return err
		}

		switch {
		case bytes.HasPrefix(line, versionPrefix): //若执行结果的前缀是 "VERSION"，说明连接成功
			break
		default:
			return fmt.Errorf("memcache: unexpected response line from ping: %q", string(line))
		}
		return nil
	})
}

func (c *Client) touchFromAddr(addr net.Addr, keys []string, expiration int32) error {
	return c.withAddrRw(addr, func(rw *bufio.ReadWriter) error {
		for _, key := range keys {
			if _, err := fmt.Fprintf(rw, "touch %s %d\r\n", key, expiration); err != nil {
				return err
			}
			if err := rw.Flush(); err != nil {
				return err
			}
			line, err := rw.ReadSlice('\n')
			if err != nil {
				return err
			}
			switch {
			case bytes.Equal(line, resultTouched):
				break
			case bytes.Equal(line, resultNotFound):
				return ErrCacheMiss
			default:
				return fmt.Errorf("memcache: unexpected response line from touch: %q", string(line))
			}
		}
		return nil
	})
}

// GetMulti is a batch version of Get. The returned map from keys to
// items may have fewer elements than the input slice, due to memcache
// cache misses. Each key must be at most 250 bytes in length.
// If no error is returned, the returned map will also be non-nil.
// GetMulti 从多个服务器上获取多个键对应的值，并将结果存储在一个 map 中返回	Get从单个服务器获取
func (c *Client) GetMulti(keys []string, start_time int64, end_time int64) (map[string]*Item, error) {
	var lk sync.Mutex
	m := make(map[string]*Item)
	addItemToMap := func(it *Item) {
		lk.Lock()
		defer lk.Unlock()
		m[it.Key] = it
	}

	keyMap := make(map[net.Addr][]string)
	for _, key := range keys {
		if !legalKey(key) {
			return nil, ErrMalformedKey
		}
		addr, err := c.selector.PickServer(key)
		if err != nil {
			return nil, err
		}
		keyMap[addr] = append(keyMap[addr], key)
	}

	var itemValues *[]byte
	ch := make(chan error, buffered)
	for addr, keys := range keyMap {
		go func(addr net.Addr, keys []string) {
			ch <- c.getFromAddr(addr, strings.Join(keys, " "), start_time, end_time, itemValues, addItemToMap)
		}(addr, keys)
	}

	var err error
	for _ = range keyMap {
		if ge := <-ch; ge != nil {
			err = ge
		}
	}
	return m, err
}

// parseGetResponse reads a GET response from r and calls cb for each		从Reader中读取一个 GET response，为每个读取到的并且分配好空间的Item调用函数cb
// read and allocated Item
func parseGetResponse(r *bufio.Reader, itemValues *[]byte, cb func(*Item)) (err error) {
	/*for { //每次读入一行，直到 Reader 为空
		line, err := r.ReadSlice('\n') //从查寻结果中读取一行
		if err != nil {
			return err
		}
		if bytes.Equal(line, resultEnd) { // get 命令查询结束后会在命令行输出 "END", 如果读到 END，说明数据读取完毕
			return nil
		}
		it := new(Item)
		size, err := scanGetResponseLine(line, it) // 从line中解析出item的属性值（除item.value以外），并得到item.value的长度（size）
		if err != nil {
			return err
		}
		it.Value = make([]byte, size+2)   // item.value中有效字符长度加上结尾的 "\r\n"
		_, err = io.ReadFull(r, it.Value) // ReadFull reads exactly len(it.Value) bytes from r into buf.	读取value的值
		if err != nil {
			it.Value = nil
			return err
		}
		if !bytes.HasSuffix(it.Value, crlf) { // HasSuffix tests whether the byte slice s ends with suffix(crlf).	一行数据的末尾必须是crlf( "\r\n" )
			it.Value = nil
			return fmt.Errorf("memcache: corrupt get result read")
		}
		it.Value = it.Value[:size] //在结果的item.value中去掉 "\r\n"
		cb(it)
	}*/

	for { //每次读入一行，直到 Reader 为空
		line, err := r.ReadBytes('\n') //从查寻结果中读取一行, 换行符对应的字节码是 10， 如果数据中有 int64 类型的 10，读取时会把数字错误当作换行符
		if err != nil {
			return err
		}
		//fmt.Printf("%s", line)
		if bytes.Equal(line, resultEnd) { // get 命令查询结束后会在数据末尾添加 "END", 如果读到 END，说明数据读取完毕
			return nil
		}
		it := new(Item)
		it.Value = line //	读取value的值
		if err != nil {
			it.Value = nil
			return err
		}
		//if !bytes.HasSuffix(it.Value, crlf) { // HasSuffix tests whether the byte slice s ends with suffix(crlf).	一行数据的末尾必须是crlf( "\r\n" )
		//	it.Value = nil
		//	return fmt.Errorf("memcache: corrupt get result read")
		//}
		cb(it)
		*itemValues = append(*itemValues, it.Value...)
	}
}

// scanGetResponseLine populates it and returns the declared size of the item.
// It does not read the bytes of the item.
/*
 通过对 get 的结果进行模式匹配，获取结果（item.value）的长度，解析并获取 item 的属性值（除 item.value 以外）
memcache命令1：
	get user
结果1：
	VALUE user 0 3
	zyx
	END

命令2：
	gets user
结果2：
	VALUE user 0 3 101
	zyx
	END
*/
//fatcache: get/set key 1 2 3 12 34
func scanGetResponseLine(line []byte, it *Item) (size int, err error) {
	//pattern := "VALUE %s %d %d %d %d %d\r\n"
	//dest := []interface{}{&it.Key, &it.Flags, &it.Expiration, &size, &it.Time_start, &it.Time_end}
	pattern := "VALUE %s %d %d\r\n"
	dest := []interface{}{&it.Key, &it.Expiration, &size}

	n, err := fmt.Sscanf(string(line), pattern, dest...) //从 line 中读取以空格分隔的字符串，与 pattern 进行模式匹配，读取结果存入 dest	返回值 n 是成功解析的项目个数
	/*
		如： line == "VALUE user 0 3 101"
			对应的pattern是 "VALUE %s %d %d %d\r\n"
			解析之后，dest中存入 user 0 3 101 ，分别对应dest声明中的四项，也就是读取到的这个item的属性值
	*/
	if err != nil || n != len(dest) {
		return -1, fmt.Errorf("memcache: unexpected line in get response: %q", line)
	}
	return size, nil
}

// Set writes the given item, unconditionally.
// 无条件写入给定的 item
func (c *Client) Set(item *Item) error {
	return c.onItem(item, (*Client).set)
}

func (c *Client) set(rw *bufio.ReadWriter, item *Item) error {
	return c.populateOne(rw, "set", item) //设置命令为 "set",传入要写入的item
}

// Add writes the given item, if no value already exists for its
// key. ErrNotStored is returned if that condition is not met.
func (c *Client) Add(item *Item) error {
	return c.onItem(item, (*Client).add)
}

func (c *Client) add(rw *bufio.ReadWriter, item *Item) error {
	return c.populateOne(rw, "add", item)
}

// Replace writes the given item, but only if the server *does*
// already hold data for this key
func (c *Client) Replace(item *Item) error {
	return c.onItem(item, (*Client).replace)
}

func (c *Client) replace(rw *bufio.ReadWriter, item *Item) error {
	return c.populateOne(rw, "replace", item)
}

// Append appends the given item to the existing item, if a value already
// exists for its key. ErrNotStored is returned if that condition is not met.
func (c *Client) Append(item *Item) error {
	return c.onItem(item, (*Client).append)
}

func (c *Client) append(rw *bufio.ReadWriter, item *Item) error {
	return c.populateOne(rw, "append", item)
}

// Prepend prepends the given item to the existing item, if a value already
// exists for its key. ErrNotStored is returned if that condition is not met.
func (c *Client) Prepend(item *Item) error {
	return c.onItem(item, (*Client).prepend)
}

func (c *Client) prepend(rw *bufio.ReadWriter, item *Item) error {
	return c.populateOne(rw, "prepend", item)
}

// CompareAndSwap writes the given item that was previously returned
// by Get, if the value was neither modified nor evicted between the
// Get and the CompareAndSwap calls. The item's Key should not change
// between calls but all other item fields may differ. ErrCASConflict
// is returned if the value was modified in between the
// calls. ErrNotStored is returned if the value was evicted in between
// the calls.
func (c *Client) CompareAndSwap(item *Item) error {
	return c.onItem(item, (*Client).cas)
}

func (c *Client) cas(rw *bufio.ReadWriter, item *Item) error {
	return c.populateOne(rw, "cas", item)
}

/*参数：
rw: 读写buffer，向Memcache写入命令，从Memcache接收执行结果
verb: 要执行的命令，如"set"、"add"等
item： 要处理的数据
*/
// 向Memcache添加item
//get/set key 1 2 3 12 34
func (c *Client) populateOne(rw *bufio.ReadWriter, verb string, item *Item) error {
	if !legalKey(item.Key) { //key的合法性
		return ErrMalformedKey
	}
	var err error
	if verb == "cas" { //"CompareAndSwap"  没用
		_, err = fmt.Fprintf(rw, "%s %s %d %d %d %d\r\n", //格式化的第一个元素表示命令（verb），后面的元素是命令中的参数
			verb, item.Key, item.Flags, item.Expiration, len(item.Value), item.CasID)
	} else { //   set

		//_, err = fmt.Fprintf(rw, "%s %s %d %d %d\r\n", //用item的基本信息构建命令的第一行，存入rw buffer	如：set user 0 0 3
		//	verb, item.Key, len(item.Value), item.Time_start, item.Time_end) // set key len st et	fatcache的set格式暂时是这样
		_, err = fmt.Fprintf(rw, "%s %s %d %d %d\r\n",
			verb, item.Key, item.Time_start, item.Time_end, item.NumOfTables) //最终格式是这样的（大概），最后一个参数是结果中表的数量

		//fmt.Printf("%s %s %d %d %d\r\n",
		//	verb, item.Key, item.Time_start, item.Time_end, item.NumOfTables)
		//fmt.Printf("%b", item.Value)
	}
	if err != nil {
		return err
	}
	if _, err = rw.Write(item.Value); err != nil { //命令的第二行，表示具体的数据	如：zyx
		return err
	}
	if _, err := rw.Write(crlf); err != nil { //向第二行写入换行符，表示命令结束
		return err
	}
	if err := rw.Flush(); err != nil { //向Memcache写入前面组装的两行命令	如： set user 0 0 3\r\n	zyx\r\n		表示把key=user、value=zyx的item存入Memcache	如果成功，会返回 STORED
		return err
	}
	line, err := rw.ReadSlice('\n') //读取命令在cache中的执行结果	如：STORED
	if err != nil {
		return err
	}
	switch {
	case bytes.Equal(line, resultStored): //bytes.Equal()	按字节比较，大小写敏感		代表成功执行
		return nil
	case bytes.Equal(line, resultNotStored): //add失败（key已存在），replace失败（key不存在）
		return ErrNotStored
	case bytes.Equal(line, resultExists): //cas失败
		return ErrCASConflict
	case bytes.Equal(line, resultNotFound):
		return ErrCacheMiss
	}
	return fmt.Errorf("memcache: unexpected response line from %q: %q", verb, string(line))
}

// 用于执行删除和自增操作
func writeReadLine(rw *bufio.ReadWriter, format string, args ...interface{}) ([]byte, error) {
	_, err := fmt.Fprintf(rw, format, args...) //写入格式化的参数
	if err != nil {
		return nil, err
	}
	if err := rw.Flush(); err != nil {
		return nil, err
	} //执行命令
	line, err := rw.ReadSlice('\n') //读取结果
	return line, err
}

// 用于执行删除操作
func writeExpectf(rw *bufio.ReadWriter, expect []byte, format string, args ...interface{}) error {
	line, err := writeReadLine(rw, format, args...)
	if err != nil {
		return err
	}
	switch {
	case bytes.Equal(line, resultOK):
		return nil
	case bytes.Equal(line, expect):
		return nil
	case bytes.Equal(line, resultNotStored):
		return ErrNotStored
	case bytes.Equal(line, resultExists):
		return ErrCASConflict
	case bytes.Equal(line, resultNotFound):
		return ErrCacheMiss
	}
	return fmt.Errorf("memcache: unexpected response line: %q", string(line))
}

// Delete deletes the item with the provided key. The error ErrCacheMiss is
// returned if the item didn't already exist in the cache.
// 用传入的key删除item
func (c *Client) Delete(key string) error {
	return c.withKeyRw(key, func(rw *bufio.ReadWriter) error {
		return writeExpectf(rw, resultDeleted, "delete %s\r\n", key)
	})
}

// DeleteAll deletes all items in the cache.
// 删除所有item 	用 flush_all 命令
func (c *Client) DeleteAll() error {
	return c.withKeyRw("", func(rw *bufio.ReadWriter) error {
		return writeExpectf(rw, resultDeleted, "flush_all\r\n")
	})
}

// Ping checks all instances if they are alive. Returns error if any
// of them is down.
func (c *Client) Ping() error {
	return c.selector.Each(c.ping)
}

// Increment atomically increments key by delta. The return value is
// the new value after being incremented or an error. If the value
// didn't exist in memcached the error is ErrCacheMiss. The value in
// memcached must be a decimal number, or an error will be returned.
// On 64-bit overflow, the new value wraps around.
func (c *Client) Increment(key string, delta uint64) (newValue uint64, err error) {
	return c.incrDecr("incr", key, delta)
}

// Decrement atomically decrements key by delta. The return value is
// the new value after being decremented or an error. If the value
// didn't exist in memcached the error is ErrCacheMiss. The value in
// memcached must be a decimal number, or an error will be returned.
// On underflow, the new value is capped at zero and does not wrap
// around.
func (c *Client) Decrement(key string, delta uint64) (newValue uint64, err error) {
	return c.incrDecr("decr", key, delta)
}

// 把数字类型的key增加或减少delta
func (c *Client) incrDecr(verb, key string, delta uint64) (uint64, error) {
	var val uint64
	err := c.withKeyRw(key, func(rw *bufio.ReadWriter) error {
		line, err := writeReadLine(rw, "%s %s %d\r\n", verb, key, delta)
		if err != nil {
			return err
		}
		switch {
		case bytes.Equal(line, resultNotFound):
			return ErrCacheMiss
		case bytes.HasPrefix(line, resultClientErrorPrefix): // 如： CLIENT_ERROR cannot increment or decrement non-numeric value
			errMsg := line[len(resultClientErrorPrefix) : len(line)-2]
			return errors.New("memcache: client error: " + string(errMsg))
		}
		val, err = strconv.ParseUint(string(line[:len(line)-2]), 10, 64) //把命令行返回的byte[]结果转换成uint64类型
		if err != nil {
			return err
		}
		return nil
	})
	return val, err
}

// Close closes any open connections.
//
// It returns the first error encountered closing connections, but always
// closes all connections.
//
// After Close, the Client may still be used.
func (c *Client) Close() error {
	c.lk.Lock()
	defer c.lk.Unlock()
	var ret error
	for _, conns := range c.freeconn {
		for _, c := range conns {
			if err := c.nc.Close(); err != nil && ret == nil {
				ret = err
			}
		}
	}
	c.freeconn = nil
	return ret
}
