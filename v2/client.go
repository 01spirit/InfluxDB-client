// Package client (v2) is the current official Go client for InfluxDB.
package client // import "github.com/influxdata/influxdb1-client/v2"

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/InfluxDB-client/memcache"
	"io"
	"io/ioutil"
	"log"
	"math"
	"mime"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/influxdata/influxdb1-client/models"
	"github.com/influxdata/influxql"
)

type ContentEncoding string

// 连接数据库
var c, err = NewHTTPClient(HTTPConfig{
	//Addr: "http://10.170.48.244:8086",
	Addr: "http://localhost:8086",
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
	MyDB = "NOAA_water_database"
	//MyDB     = "test"
	username = "root"
	password = "12345678"
)

// DB:	test	measurement:	cpu
/*
* 	field
 */
//usage_guest
//usage_guest_nice
//usage_idle
//usage_iowait
//usage_irq
//usage_nice
//usage_softirq
//usage_steal
//usage_system
//usage_user

/*
* 	tag
 */
//arch [x64 x86]
//datacenter [eu-central-1a us-west-2b us-west-2c]
//hostname [host_0 host_1 host_2 host_3]
//os [Ubuntu15.10 Ubuntu16.04LTS Ubuntu16.10]
//rack [4 41 61 84]
//region [eu-central-1 us-west-2]
//service [18 2 4 6]
//service_environment [production staging]
//service_version [0 1]
//team [CHI LON NYC]

const (
	DefaultEncoding ContentEncoding = ""
	GzipEncoding    ContentEncoding = "gzip"
)

// HTTPConfig is the config data needed to create an HTTP Client.
type HTTPConfig struct {
	// Addr should be of the form "http://host:port"
	// or "http://[ipv6-host%zone]:port".
	Addr string

	// Username is the influxdb username, optional.
	Username string

	// Password is the influxdb password, optional.
	Password string

	// UserAgent is the http User Agent, defaults to "InfluxDBClient".
	UserAgent string

	// Timeout for influxdb writes, defaults to no timeout.
	Timeout time.Duration

	// InsecureSkipVerify gets passed to the http client, if true, it will
	// skip https certificate verification. Defaults to false.
	InsecureSkipVerify bool

	// TLSConfig allows the user to set their own TLS config for the HTTP
	// Client. If set, this option overrides InsecureSkipVerify.
	TLSConfig *tls.Config

	// Proxy configures the Proxy function on the HTTP client.
	Proxy func(req *http.Request) (*url.URL, error)

	// WriteEncoding specifies the encoding of write request
	WriteEncoding ContentEncoding
}

// BatchPointsConfig is the config data needed to create an instance of the BatchPoints struct.
type BatchPointsConfig struct {
	// Precision is the write precision of the points, defaults to "ns".
	Precision string

	// Database is the database to write points to.
	Database string

	// RetentionPolicy is the retention policy of the points.
	RetentionPolicy string

	// Write consistency is the number of servers required to confirm write.
	WriteConsistency string
}

// Client is a client interface for writing & querying the database.
type Client interface {
	// Ping checks that status of cluster, and will always return 0 time and no
	// error for UDP clients.
	Ping(timeout time.Duration) (time.Duration, string, error)

	// Write takes a BatchPoints object and writes all Points to InfluxDB.
	Write(bp BatchPoints) error

	// Query makes an InfluxDB Query on the database. This will fail if using
	// the UDP client.
	Query(q Query) (*Response, error)

	// QueryAsChunk makes an InfluxDB Query on the database. This will fail if using
	// the UDP client.
	QueryAsChunk(q Query) (*ChunkedResponse, error)

	// Close releases any resources a Client may be using.
	Close() error
}

// NewHTTPClient returns a new Client from the provided config.
// Client is safe for concurrent use by multiple goroutines.
func NewHTTPClient(conf HTTPConfig) (Client, error) {
	if conf.UserAgent == "" {
		conf.UserAgent = "InfluxDBClient"
	}

	u, err := url.Parse(conf.Addr)
	if err != nil {
		return nil, err
	} else if u.Scheme != "http" && u.Scheme != "https" {
		m := fmt.Sprintf("Unsupported protocol scheme: %s, your address"+
			" must start with http:// or https://", u.Scheme)
		return nil, errors.New(m)
	}

	switch conf.WriteEncoding {
	case DefaultEncoding, GzipEncoding:
	default:
		return nil, fmt.Errorf("unsupported encoding %s", conf.WriteEncoding)
	}

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: conf.InsecureSkipVerify,
		},
		Proxy: conf.Proxy,
	}
	if conf.TLSConfig != nil {
		tr.TLSClientConfig = conf.TLSConfig
	}
	return &client{
		url:       *u,
		username:  conf.Username,
		password:  conf.Password,
		useragent: conf.UserAgent,
		httpClient: &http.Client{
			Timeout:   conf.Timeout,
			Transport: tr,
		},
		transport: tr,
		encoding:  conf.WriteEncoding,
	}, nil
}

// Ping will check to see if the server is up with an optional timeout on waiting for leader.
// Ping returns how long the request took, the version of the server it connected to, and an error if one occurred.
func (c *client) Ping(timeout time.Duration) (time.Duration, string, error) {
	now := time.Now()

	u := c.url
	u.Path = path.Join(u.Path, "ping")

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return 0, "", err
	}

	req.Header.Set("User-Agent", c.useragent)

	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	if timeout > 0 {
		params := req.URL.Query()
		params.Set("wait_for_leader", fmt.Sprintf("%.0fs", timeout.Seconds()))
		req.URL.RawQuery = params.Encode()
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()

	//body, err := ioutil.ReadAll(resp.Body)
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, "", err
	}

	if resp.StatusCode != http.StatusNoContent {
		var err = errors.New(string(body))
		return 0, "", err
	}

	version := resp.Header.Get("X-Influxdb-Version")
	return time.Since(now), version, nil
}

// Close releases the client's resources.
func (c *client) Close() error {
	c.transport.CloseIdleConnections()
	return nil
}

// client is safe for concurrent use as the fields are all read-only
// once the client is instantiated.
type client struct {
	// N.B - if url.UserInfo is accessed in future modifications to the
	// methods on client, you will need to synchronize access to url.
	url        url.URL
	username   string
	password   string
	useragent  string
	httpClient *http.Client
	transport  *http.Transport
	encoding   ContentEncoding
}

// BatchPoints is an interface into a batched grouping of points to write into
// InfluxDB together. BatchPoints is NOT thread-safe, you must create a separate
// batch for each goroutine.
type BatchPoints interface {
	// AddPoint adds the given point to the Batch of points.
	AddPoint(p *Point)
	// AddPoints adds the given points to the Batch of points.
	AddPoints(ps []*Point)
	// Points lists the points in the Batch.
	Points() []*Point

	// Precision returns the currently set precision of this Batch.
	Precision() string
	// SetPrecision sets the precision of this batch.
	SetPrecision(s string) error

	// Database returns the currently set database of this Batch.
	Database() string
	// SetDatabase sets the database of this Batch.
	SetDatabase(s string)

	// WriteConsistency returns the currently set write consistency of this Batch.
	WriteConsistency() string
	// SetWriteConsistency sets the write consistency of this Batch.
	SetWriteConsistency(s string)

	// RetentionPolicy returns the currently set retention policy of this Batch.
	RetentionPolicy() string
	// SetRetentionPolicy sets the retention policy of this Batch.
	SetRetentionPolicy(s string)
}

// NewBatchPoints returns a BatchPoints interface based on the given config.
func NewBatchPoints(conf BatchPointsConfig) (BatchPoints, error) {
	if conf.Precision == "" {
		conf.Precision = "ns"
	}
	if _, err := time.ParseDuration("1" + conf.Precision); err != nil {
		return nil, err
	}
	bp := &batchpoints{
		database:         conf.Database,
		precision:        conf.Precision,
		retentionPolicy:  conf.RetentionPolicy,
		writeConsistency: conf.WriteConsistency,
	}
	return bp, nil
}

type batchpoints struct {
	points           []*Point
	database         string
	precision        string
	retentionPolicy  string
	writeConsistency string
}

func (bp *batchpoints) AddPoint(p *Point) {
	bp.points = append(bp.points, p)
}

func (bp *batchpoints) AddPoints(ps []*Point) {
	bp.points = append(bp.points, ps...)
}

func (bp *batchpoints) Points() []*Point {
	return bp.points
}

func (bp *batchpoints) Precision() string {
	return bp.precision
}

func (bp *batchpoints) Database() string {
	return bp.database
}

func (bp *batchpoints) WriteConsistency() string {
	return bp.writeConsistency
}

func (bp *batchpoints) RetentionPolicy() string {
	return bp.retentionPolicy
}

func (bp *batchpoints) SetPrecision(p string) error {
	if _, err := time.ParseDuration("1" + p); err != nil {
		return err
	}
	bp.precision = p
	return nil
}

func (bp *batchpoints) SetDatabase(db string) {
	bp.database = db
}

func (bp *batchpoints) SetWriteConsistency(wc string) {
	bp.writeConsistency = wc
}

func (bp *batchpoints) SetRetentionPolicy(rp string) {
	bp.retentionPolicy = rp
}

// Point represents a single data point.
type Point struct {
	pt models.Point
}

// NewPoint returns a point with the given timestamp. If a timestamp is not
// given, then data is sent to the database without a timestamp, in which case
// the server will assign local time upon reception. NOTE: it is recommended to
// send data with a timestamp.
func NewPoint(
	name string,
	tags map[string]string,
	fields map[string]interface{},
	t ...time.Time,
) (*Point, error) {
	var T time.Time
	if len(t) > 0 {
		T = t[0]
	}

	pt, err := models.NewPoint(name, models.NewTags(tags), fields, T)
	if err != nil {
		return nil, err
	}
	return &Point{
		pt: pt,
	}, nil
}

// String returns a line-protocol string of the Point.
func (p *Point) String() string {
	return p.pt.String()
}

// PrecisionString returns a line-protocol string of the Point,
// with the timestamp formatted for the given precision.
func (p *Point) PrecisionString(precision string) string {
	return p.pt.PrecisionString(precision)
}

// Name returns the measurement name of the point.
func (p *Point) Name() string {
	return string(p.pt.Name())
}

// Tags returns the tags associated with the point.
func (p *Point) Tags() map[string]string {
	return p.pt.Tags().Map()
}

// Time return the timestamp for the point.
func (p *Point) Time() time.Time {
	return p.pt.Time()
}

// UnixNano returns timestamp of the point in nanoseconds since Unix epoch.
func (p *Point) UnixNano() int64 {
	return p.pt.UnixNano()
}

// Fields returns the fields for the point.
func (p *Point) Fields() (map[string]interface{}, error) {
	return p.pt.Fields()
}

// NewPointFrom returns a point from the provided models.Point.
func NewPointFrom(pt models.Point) *Point {
	return &Point{pt: pt}
}

func (c *client) Write(bp BatchPoints) error {
	var b bytes.Buffer

	var w io.Writer
	if c.encoding == GzipEncoding {
		w = gzip.NewWriter(&b)
	} else {
		w = &b
	}

	for _, p := range bp.Points() { //数据点批量写入
		if p == nil {
			continue
		}
		if _, err := io.WriteString(w, p.pt.PrecisionString(bp.Precision())); err != nil { //向 writer 写入一条数据(sring)
			return err
		}

		if _, err := w.Write([]byte{'\n'}); err != nil { //每条数据换一行
			return err
		}
	}

	// gzip writer should be closed to flush data into underlying buffer
	if c, ok := w.(io.Closer); ok {
		if err := c.Close(); err != nil {
			return err
		}
	}

	//组合一个写入请求
	u := c.url
	u.Path = path.Join(u.Path, "write")

	req, err := http.NewRequest("POST", u.String(), &b)
	if err != nil {
		return err
	}
	if c.encoding != DefaultEncoding {
		req.Header.Set("Content-Encoding", string(c.encoding))
	}
	req.Header.Set("Content-Type", "")
	req.Header.Set("User-Agent", c.useragent)
	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	params := req.URL.Query()
	params.Set("db", bp.Database())
	params.Set("rp", bp.RetentionPolicy())
	params.Set("precision", bp.Precision())
	params.Set("consistency", bp.WriteConsistency())
	req.URL.RawQuery = params.Encode()

	//发送请求，接受响应
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	//body, err := ioutil.ReadAll(resp.Body)
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		var err = errors.New(string(body))
		return err
	}

	return nil
}

// Query defines a query to send to the server.
type Query struct {
	Command         string
	Database        string
	RetentionPolicy string
	Precision       string
	Chunked         bool // chunked是数据存储和查询的方式，用于大量数据的读写操作，把数据划分成较小的块存储，而不是单条记录	，块内数据点数量固定
	ChunkSize       int
	Parameters      map[string]interface{}
}

// Params is a type alias to the query parameters.
type Params map[string]interface{}

// NewQuery returns a query object.
// The database and precision arguments can be empty strings if they are not needed for the query.
func NewQuery(command, database, precision string) Query {
	return Query{
		Command:    command,
		Database:   database,
		Precision:  precision,                    // autogen
		Parameters: make(map[string]interface{}), // 参数化查询 ?
	}
}

// NewQueryWithRP returns a query object.
// The database, retention policy, and precision arguments can be empty strings if they are not needed
// for the query. Setting the retention policy only works on InfluxDB versions 1.6 or greater.
func NewQueryWithRP(command, database, retentionPolicy, precision string) Query {
	return Query{
		Command:         command,
		Database:        database,
		RetentionPolicy: retentionPolicy,
		Precision:       precision,
		Parameters:      make(map[string]interface{}),
	}
}

// NewQueryWithParameters returns a query object.
// The database and precision arguments can be empty strings if they are not needed for the query.
// parameters is a map of the parameter names used in the command to their values.
func NewQueryWithParameters(command, database, precision string, parameters map[string]interface{}) Query {
	return Query{
		Command:    command,
		Database:   database,
		Precision:  precision,
		Parameters: parameters,
	}
}

// Response represents a list of statement results.
type Response struct {
	Results []Result
	Err     string `json:"error,omitempty"`
}

// Error returns the first error from any statement.
// It returns nil if no errors occurred on any statements.
func (r *Response) Error() error {
	if r.Err != "" {
		return errors.New(r.Err)
	}
	for _, result := range r.Results {
		if result.Err != "" {
			return errors.New(result.Err)
		}
	}
	return nil
}

// Message represents a user message.
type Message struct {
	Level string
	Text  string
}

// Result represents a resultset returned from a single statement.
type Result struct {
	StatementId int `json:"statement_id"`
	Series      []models.Row
	Messages    []*Message
	Err         string `json:"error,omitempty"`
}

// Query sends a command to the server and returns the Response.
func (c *client) Query(q Query) (*Response, error) {
	req, err := c.createDefaultRequest(q)
	if err != nil {
		return nil, err
	}
	params := req.URL.Query()
	if q.Chunked { //查询结果是否分块
		params.Set("chunked", "true")
		if q.ChunkSize > 0 {
			params.Set("chunk_size", strconv.Itoa(q.ChunkSize))
		}
		req.URL.RawQuery = params.Encode()
	}
	resp, err := c.httpClient.Do(req) // 发送请求
	if err != nil {
		return nil, err
	}
	defer func() {
		io.Copy(ioutil.Discard, resp.Body) // https://github.com/influxdata/influxdb1-client/issues/58
		resp.Body.Close()
	}()

	if err := checkResponse(resp); err != nil {
		return nil, err
	}

	var response Response
	if q.Chunked { // 分块
		cr := NewChunkedResponse(resp.Body)
		for {
			r, err := cr.NextResponse()
			if err != nil {
				if err == io.EOF { // 结束
					break
				}
				// If we got an error while decoding the response, send that back.
				return nil, err
			}

			if r == nil {
				break
			}

			response.Results = append(response.Results, r.Results...) // 把所有结果添加到 response.Results 数组中
			if r.Err != "" {
				response.Err = r.Err
				break
			}
		}
	} else { // 不分块，普通查询
		dec := json.NewDecoder(resp.Body) // 响应是 json 格式，需要进行解码，创建一个 Decoder，参数是 JSON 的 Reader
		dec.UseNumber()                   // 解码时把数字字符串转换成 Number 的字面值
		decErr := dec.Decode(&response)   // 解码，结果存入自定义的 Response, Response结构体和 json 的字段对应

		// ignore this error if we got an invalid status code
		if decErr != nil && decErr.Error() == "EOF" && resp.StatusCode != http.StatusOK {
			decErr = nil
		}
		// If we got a valid decode error, send that back
		if decErr != nil {
			return nil, fmt.Errorf("unable to decode json: received status code %d err: %s", resp.StatusCode, decErr)
		}
	}

	// If we don't have an error in our json response, and didn't get statusOK
	// then send back an error
	if resp.StatusCode != http.StatusOK && response.Error() == nil {
		return &response, fmt.Errorf("received status code %d from server", resp.StatusCode)
	}
	return &response, nil
}

// QueryAsChunk sends a command to the server and returns the Response.
func (c *client) QueryAsChunk(q Query) (*ChunkedResponse, error) {
	req, err := c.createDefaultRequest(q)
	if err != nil {
		return nil, err
	}
	params := req.URL.Query()
	params.Set("chunked", "true")
	if q.ChunkSize > 0 {
		params.Set("chunk_size", strconv.Itoa(q.ChunkSize))
	}
	req.URL.RawQuery = params.Encode()
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	if err := checkResponse(resp); err != nil {
		return nil, err
	}
	return NewChunkedResponse(resp.Body), nil // 把HTTP响应的 reader 传入，进行解码
}

// 检验响应合法性
func checkResponse(resp *http.Response) error {
	// If we lack a X-Influxdb-Version header, then we didn't get a response from influxdb
	// but instead some other service. If the error code is also a 500+ code, then some
	// downstream loadbalancer/proxy/etc had an issue and we should report that.
	if resp.Header.Get("X-Influxdb-Version") == "" && resp.StatusCode >= http.StatusInternalServerError {
		body, err := io.ReadAll(resp.Body)
		if err != nil || len(body) == 0 {
			return fmt.Errorf("received status code %d from downstream server", resp.StatusCode)
		}

		return fmt.Errorf("received status code %d from downstream server, with response body: %q", resp.StatusCode, body)
	}

	// If we get an unexpected content type, then it is also not from influx direct and therefore
	// we want to know what we received and what status code was returned for debugging purposes.
	if cType, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type")); cType != "application/json" {
		// Read up to 1kb of the body to help identify downstream errors and limit the impact of things
		// like downstream serving a large file
		body, err := ioutil.ReadAll(io.LimitReader(resp.Body, 1024))
		if err != nil || len(body) == 0 {
			return fmt.Errorf("expected json response, got empty body, with status: %v", resp.StatusCode)
		}

		return fmt.Errorf("expected json response, got %q, with status: %v and response body: %q", cType, resp.StatusCode, body)
	}
	return nil
}

// 创造默认查询请求
func (c *client) createDefaultRequest(q Query) (*http.Request, error) {
	u := c.url
	u.Path = path.Join(u.Path, "query")

	jsonParameters, err := json.Marshal(q.Parameters)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", u.String(), nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "")
	req.Header.Set("User-Agent", c.useragent)

	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	params := req.URL.Query()
	params.Set("q", q.Command)
	params.Set("db", q.Database)
	if q.RetentionPolicy != "" {
		params.Set("rp", q.RetentionPolicy)
	}
	params.Set("params", string(jsonParameters))

	if q.Precision != "" {
		params.Set("epoch", q.Precision)
	}
	req.URL.RawQuery = params.Encode()

	return req, nil

}

// duplexReader reads responses and writes it to another writer while
// satisfying the reader interface.
type duplexReader struct {
	r io.ReadCloser
	w io.Writer
}

func (r *duplexReader) Read(p []byte) (n int, err error) {
	n, err = r.r.Read(p)
	if err == nil {
		r.w.Write(p[:n])
	}
	return n, err
}

// Close closes the response.
func (r *duplexReader) Close() error {
	return r.r.Close()
}

// ChunkedResponse represents a response from the server that
// uses chunking to stream the output.
type ChunkedResponse struct {
	dec    *json.Decoder
	duplex *duplexReader
	buf    bytes.Buffer
}

// NewChunkedResponse reads a stream and produces responses from the stream.
func NewChunkedResponse(r io.Reader) *ChunkedResponse {
	rc, ok := r.(io.ReadCloser)
	if !ok {
		rc = ioutil.NopCloser(r)
	}
	resp := &ChunkedResponse{}
	resp.duplex = &duplexReader{r: rc, w: &resp.buf} //把 reader 中的数据写入 buffer
	resp.dec = json.NewDecoder(resp.duplex)          //解码
	resp.dec.UseNumber()
	return resp
}

// NextResponse reads the next line of the stream and returns a response.
func (r *ChunkedResponse) NextResponse() (*Response, error) {
	var response Response
	if err := r.dec.Decode(&response); err != nil {
		if err == io.EOF {
			return nil, err
		}
		// A decoding error happened. This probably means the server crashed
		// and sent a last-ditch error message to us. Ensure we have read the
		// entirety of the connection to get any remaining error text.
		io.Copy(ioutil.Discard, r.duplex)
		return nil, errors.New(strings.TrimSpace(r.buf.String()))
	}

	r.buf.Reset()
	return &response, nil
}

// Close closes the response.
func (r *ChunkedResponse) Close() error {
	return r.duplex.Close()
}

func Set(queryString string, c Client, mc *memcache.Client) error {
	query := NewQuery(queryString, MyDB, "ns")
	resp, err := c.Query(query)
	if err != nil {
		return err
	}

	semanticSegment := SemanticSegment(queryString, resp)
	startTime, endTime := GetResponseTimeRange(resp)
	respCacheByte := resp.ToByteArray(queryString)
	tableNumbers := int64(len(resp.Results[0].Series))

	item := memcache.Item{
		Key:         semanticSegment,
		Value:       respCacheByte,
		Flags:       0,
		Expiration:  0,
		CasID:       0,
		Time_start:  startTime,
		Time_end:    endTime,
		NumOfTables: tableNumbers,
	}

	err = mc.Set(&item)

	if err != nil {
		return err
	}

	return nil
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
	timePrecision := time.Hour
	switch precision {
	case "ns":
		timePrecision = time.Nanosecond
		break
	case "us":
		timePrecision = time.Microsecond
		break
	case "ms":
		timePrecision = time.Millisecond
		break
	case "s":
		timePrecision = time.Second
		break
	case "m":
		timePrecision = time.Minute
		break
	case "h":
		timePrecision = time.Hour
		break
	default:
		timePrecision = time.Hour
	}
	timeRange := timePrecision.Nanoseconds()

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

// 用于对结果排序的结构体
type RespWithTimeRange struct {
	resp      *Response
	startTime int64
	endTime   int64
}

/* 传入一组查询结果，构造成用于排序的结构体，对不为空的结果按时间升序进行排序，返回结果数组 */
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

/* 用起止时间为一组查询结果排序 	冒泡排序 */
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

/* 判断结果是否为空 */
func ResponseIsEmpty(resp *Response) bool {
	/* 以下情况之一表示结果为空，返回 true */
	return resp == nil || len(resp.Results[0].Series) == 0 || len(resp.Results[0].Series[0].Values) == 0
}

/* 用于合并结果时暂时存储合并好的数据，合并完成后替换到结果中 */
type Series struct {
	Name    string            // measurement name
	Tags    map[string]string // GROUP BY tags
	Columns []string          // column name
	Values  [][]interface{}   // specific query results
	Partial bool              // useless (false)
}

/* 转换成可以替换到结果中的结构体 */
func SeriesToRow(ser Series) models.Row {
	return models.Row{
		Name:    ser.Name,
		Tags:    ser.Tags,
		Columns: ser.Columns,
		Values:  ser.Values,
		Partial: ser.Partial,
	}
}

/* （表按字典序排列）获取一个结果（Response）中的所有表（Series）的所有tag（ map[string]string ） */
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

/* 按字典序把一张表中的所有tags组合成字符串 */
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

/* 多表合并的关键部分，合并两个结果中的所有表的结构	有些表可能是某个结果独有的 */
func MergeSeries(resp1, resp2 *Response) []Series {
	resSeries := make([]Series, 0)
	tagsMapMerged := make(map[int]map[string]string)

	/* 分别获取所有表的所有tag，按字典序排列 */
	tagsMap1 := GetSeriesTagsMap(resp1)
	tagsMap2 := GetSeriesTagsMap(resp2)

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
			if strings.Compare(tagStr1, tagStr2) != 0 {
				if !strings.Contains(str2, tagStr1) { // 表示结果2中没有结果1的这张表，只把这张表添加到合并结果中
					tagsMapMerged[indexAll] = tagsMap1[index1]
					index1++
					indexAll++
				} else if !strings.Contains(str1, tagStr2) { // 结果2独有的表
					tagsMapMerged[indexAll] = tagsMap2[index2]
					index2++
					indexAll++
				}
				same = false
			}

			/* 是同一张表 */
			if same {
				tagsMapMerged[indexAll] = tagsMap1[index1]
				index1++ // 两张表的索引都要后移
				index2++
				indexAll++
			}
		} else if index1 == len1 && index2 < len2 { // 只剩结果2的表了
			tagsMapMerged[indexAll] = tagsMap2[index2]
			index2++
			indexAll++
		} else if index1 < len1 && index2 == len2 { // 只剩结果1的表了
			tagsMapMerged[indexAll] = tagsMap1[index1]
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

// 获取一个数据库中所有表的field name，每张表存为一个map，其中的fields存为一个string数组
func GetFieldKeys(c Client, database string) map[string][]string {
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

	fieldMap := make(map[string][]string)
	for _, series := range resp.Results[0].Series {
		fieldNames := make([]string, 0)
		measurementName := series.Name
		for _, value := range series.Values {
			fieldName, ok := value[0].(string)
			if !ok {
				log.Fatal("field name fail to convert to string")
			}
			fieldNames = append(fieldNames, fieldName)
		}

		fieldMap[measurementName] = fieldNames
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

// 获取所有表的tag的key和value
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

// GetTagNameArr /* 判断结果是否为空，并从结果中取出tags数组，用于规范tag map的输出顺序 */
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
			idx = strings.Index(tagPredicates[i], "=")
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

	//if strings.IndexAny(FGstr, ")") > 0 {
	//	/* 获取聚合函数 */
	//	index := strings.IndexAny(FGstr, "(")
	//	aggr = FGstr[:index]
	//	aggr = strings.ToLower(aggr)
	//
	//	/* 从查询语句获取field(实际的列名) */
	//	fields = append(fields, "time")
	//	var startIdx int
	//	var endIdx int
	//	for idx, ch := range FGstr { // 括号中间的部分是fields，默认没有双引号，不作处理
	//		if ch == '(' {
	//			startIdx = idx + 1
	//		}
	//		if ch == ')' {
	//			endIdx = idx
	//		}
	//	}
	//	tmpStr := FGstr[startIdx:endIdx]
	//	tmpArr := strings.Split(tmpStr, ",")
	//	fields = append(fields, tmpArr...)
	//} else {
	//	aggr = "empty"
	//	/* 从Response获取列名 */
	//	for _, c := range resp.Results[0].Series[0].Columns {
	//		fields = append(fields, c)
	//	}
	//}

	/* 从查寻结果中获取每一列的数据类型 */
	dataTypes := DataTypeArrayFromResponse(resp)
	for i := range fields {
		fields[i] = fmt.Sprintf("%s[%s]", fields[i], dataTypes[i])
	}

	//去掉第一列中的 time[int64]
	fields = fields[1:]
	var fieldsStr string
	fieldsStr = strings.Join(fields, ",")

	return fieldsStr, aggr
}

// DataTypeArrayFromResponse 从查寻结果中获取每一列的数据类型
func DataTypeArrayFromResponse(resp *Response) []string {
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
					if i == 0 { // 时间戳可能是string或int64，只使用int64
						fields = append(fields, "int64")
					} else if _, ok := value.(string); ok {
						fields = append(fields, "string")
					} else if v, ok := value.(json.Number); ok {
						if _, err := v.Int64(); err == nil {
							fields = append(fields, "int64")
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

// DataTypeArrayFromSF  从列名和数据类型组成的字符串中提取出每一列的数据类型
// time[int64],index[int64],location[string],randtag[string]
// 列名和数据类型都存放在数组中，顺序是固定的，不用手动排序，直接取出来就行
func DataTypeArrayFromSF(sfString string) []string {
	datatypes := make([]string, 0)
	columns := strings.Split(sfString, ",")

	for _, col := range columns {
		startIdx := strings.Index(col, "[") + 1
		endIdx := strings.Index(col, "]")
		datatypes = append(datatypes, col[startIdx:endIdx])
	}

	return datatypes
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

		tags, predicates, datatypes := PreOrderTraverseBinaryExpr(binaryExpr, &tag, &conds, &datatype)
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
		_, predicates, datatypes := PreOrderTraverseBinaryExpr(binaryExpr, &tag, &conds, &datatype)
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
遍历语法树，找出所有谓词表达式，去掉多余的空格，存入字符串数组
*/
func PreOrderTraverseBinaryExpr(node *influxql.BinaryExpr, tags *[]string, predicates *[]string, datatypes *[]string) (*[]string, *[]string, *[]string) {
	if node.Op != influxql.AND && node.Op != influxql.OR { // 不是由AND或OR连接的，说明表达式不可再分，存入结果数组
		str := node.String()
		//fmt.Println(node.LHS.String())
		// 用字符串获取每个二元表达式的数据类型	可能有问题，具体看怎么用
		if strings.Contains(str, "'") { // 有单引号的都是字符串
			*datatypes = append(*datatypes, "string")
		} else if strings.EqualFold(node.RHS.String(), "true") || strings.EqualFold(node.RHS.String(), "false") { // 忽略大小写，相等就是 bool
			*datatypes = append(*datatypes, "bool")
		} else if strings.Contains(str, ".") { // 带小数点就是 double
			*datatypes = append(*datatypes, "float64")
		} else { // 什么都没有是 int
			*datatypes = append(*datatypes, "int64")
		}

		*tags = append(*tags, node.LHS.String())
		str = strings.ReplaceAll(str, " ", "") //去掉空格
		*predicates = append(*predicates, str)
		return tags, predicates, datatypes
	}

	if node.LHS != nil { //遍历左子树
		binaryExprL := GetBinaryExpr(node.LHS.String())
		PreOrderTraverseBinaryExpr(binaryExprL, tags, predicates, datatypes)
	} else {
		return tags, predicates, datatypes
	}

	if node.RHS != nil { //遍历右子树
		binaryExprR := GetBinaryExpr(node.RHS.String())
		PreOrderTraverseBinaryExpr(binaryExprR, tags, predicates, datatypes)
	} else {
		return tags, predicates, datatypes
	}

	return tags, predicates, datatypes
}

/*
字符串转化成二元表达式，用作遍历二叉树的节点
*/
func GetBinaryExpr(str string) *influxql.BinaryExpr {
	now := time.Now()
	valuer := influxql.NowValuer{Now: now}
	parsedExpr, _ := influxql.ParseExpr(str)
	condExpr, _, _ := influxql.ConditionExpr(parsedExpr, &valuer)
	binaryExpr := condExpr.(*influxql.BinaryExpr)

	return binaryExpr
}

/*
获取 GROUP BY interval
*/
func GetInterval(query string) string {
	parser := influxql.NewParser(strings.NewReader(query))
	stmt, _ := parser.ParseStatement()

	/* 获取 GROUP BY interval */
	s := stmt.(*influxql.SelectStatement)
	interval, err := s.GroupByInterval()
	if err != nil {
		log.Fatalln("GROUP BY INTERVAL ERROR")
	}

	//fmt.Println("GROUP BY interval:\t", interval.String()) // 12m0s

	if interval == 0 {
		return "empty"
	} else {
		//result := fmt.Sprintf("%dm", int(interval.Minutes()))
		//return result
		result := interval.String()
		for idx, ch := range result {
			if unicode.IsLetter(ch) {
				if (idx+1) < len(result) && result[idx+1] == '0' {
					return result[0 : idx+1]
				}
			}
		}

		return result
	}

}

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

func (resp *Response) ToByteArray(queryString string) []byte {
	result := make([]byte, 0)

	/* 结果为空 */
	if ResponseIsEmpty(resp) {
		return StringToByteArray("empty response")
	}

	/* 获取每一列的数据类型 */
	datatypes := DataTypeArrayFromResponse(resp)

	/* 获取每张表单独的语义段 */
	seprateSemanticSegment := SeperateSemanticSegment(queryString, resp)

	/* 每行数据的字节数 */
	bytesPerLine := BytesPerLine(datatypes)

	for i, s := range resp.Results[0].Series {
		numOfValues := len(s.Values)                                             // 表中数据行数
		bytesPerSeries, _ := Int64ToByteArray(int64(bytesPerLine * numOfValues)) // 一张表的数据的总字节数：每行字节数 * 行数

		/* 存入一张表的 semantic segment 和表内所有数据的总字节数 */
		result = append(result, []byte(seprateSemanticSegment[i])...)
		result = append(result, []byte(" ")...)
		result = append(result, bytesPerSeries...)
		result = append(result, []byte("\r\n")...) // 是否需要换行

		//fmt.Printf("%s %d\r\n", seprateSemanticSegment[i], bytesPerSeries)

		/* 数据转换成字节数组，存入 */
		for _, v := range s.Values {
			for j, vv := range v {
				datatype := datatypes[j]
				tmpBytes := InterfaceToByteArray(j, datatype, vv)
				result = append(result, tmpBytes...)

			}
			//fmt.Println(v)
			//fmt.Print(v)

			/* 如果传入cache的数据之间不需要换行，就把这一行注释掉 */
			result = append(result, []byte("\r\n")...) // 每条数据之后换行
		}
		/* 如果表之间需要换行，在这里添加换行符，但是从字节数组转换成结果类型的部分也要修改 */
		//result = append(result, []byte("\r\n")...) // 每条数据之后换行
	}

	return result
}

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
			index += 2 // 索引指向换行符之后的第一个字节，开始读具体数据
		}

		/* 从 curSeg 取出包含每列的数据类型的字符串sf,获取数据类型数组 */
		// 所有数据和数据类型都存放在数组中，位置是对应的
		messages := strings.Split(curSeg, "#")
		sf := messages[1][1 : len(messages[1])-1] // 去掉大括号，包含列名和数据类型的字符串
		datatypes := DataTypeArrayFromSF(sf)      // 每列的数据类型

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
					index += 2 //	索引指向当前数据的后一个字节
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
			index += 2 // 跳过每行数据之间的换行符CRLF，处理下一行数据
		}
		valuess = append(valuess, values)
	}

	/* 用 semanticSegments数组 和 values数组 还原出表结构，构造成 Response 返回 */
	modelsRows := make([]models.Row, 0)

	// {SSM}#{SF}#{SP}#{SG}
	// 需要 SSM (name.tag=value) 中的 measurement name 和 tag value	可能是 empty_tag
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
		sf := messages[1][1 : len(messages[1])-1]
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

func BoolToByteArray(b bool) ([]byte, error) {
	bytesBuffer := bytes.NewBuffer([]byte{})
	err := binary.Write(bytesBuffer, binary.BigEndian, &b)
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
	err := binary.Read(byteBuffer, binary.BigEndian, &b)
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
	err := binary.Write(byteBuffer, binary.BigEndian, &number)
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
	err := binary.Read(byteBuffer, binary.BigEndian, &number)
	if err != nil {
		return 0, err
	}
	return number, nil
}

func Float64ToByteArray(number float64) ([]byte, error) {
	byteBuffer := bytes.NewBuffer([]byte{})
	err := binary.Write(byteBuffer, binary.BigEndian, &number)
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
	err := binary.Read(byteBuffer, binary.BigEndian, &number)
	if err != nil {
		return 0.0, err
	}
	return number, nil
}

func TimeStringToInt64(timestamp string) int64 {
	timeT, _ := time.Parse(time.RFC3339, timestamp)
	numberN := timeT.UnixNano()

	return numberN
}

// 从字节数组转换回来的时间戳是 int64 ,Response 结构中存的是 string	time.RFC3339
func TimeInt64ToString(number int64) string {
	t := time.Unix(0, number).UTC()
	timestamp := t.Format(time.RFC3339)

	return timestamp
}

// todo :
// lists:

// todo 整合Set()函数

// todo 在工作站上安装InfluxDB1.8，下载样例数据库

/* 详见 client_test.go 最后的说明 */
// done : 修改 semantic segment ,去掉所有的时间范围 ST	，修改测试代码中所有包含时间范围的部分
// done : 找到Get()方法的限制和什么因素有关，为什么会是读取64条数据，数据之间即使去掉换行符也不能读取更多，
// done : key 的长度限制暂时设置为 450

// done 1.把数据转为对应数量的byte
// done 2.根据series确定SF的数据类型。
// done 3.把转化好的byte传入fatcache中再取出，转为result
/*
	1.数据类型有4种：string/int64/float64/bool
		字节数：		25/8/8/1
	2.SF保存查寻结果中的所有列的列名和数据类型
		根据这里的数据类型决定数据占用的字节数，以及把字节数组转换成什么数据类型
		SF有两种可能：不使用聚合函数时，包含所有 SELECT 的 tag 和 field，
					使用聚合函数时，列名可能是 MAX、MEAN 之类的，需要从 SELECT 语句中取出字段名
		数据类型根据从结果中取的第一行数据进行判断，数据有 string、bool、json.Number 三种类型
			需要把 json.Number 转换成 int64 或 float64
			暂时方法：看能否进行类型转换，能转换成 int64 就是 int64， 否则是 float64 （?）	// 验证该方法是否可行 	可行

			json.Number的 int64 可以转换成 float64； float64 不能转成  int64
			底层调用了 strconv.ParseInt() 和 strconv.ParseFloat()

	3.暂时把所有表的数据看成一张表的，测试能否成功转换
		然后处理成和cache交互的具体格式
		交互：
			set key(semantic segment) start_time end_time SLen(num of series)
			SM1(first series' tags) VLen1(num of the series' values' byte)
			values([]byte)	(自己测试暂时加上换行符，之后交互时只传数据)
			SM2(second series) VLen2
			values([byte])
	数据转换时根据SF的数据类型读取相应数量的字节，然后转换

	result结构是数组嵌套，根据 semantic segment 获取其他元数据之后，通过从字节数组中解析出具体数据完成结果类型转换
	memcache Get() 返回 byte array ，存放在 []byte(itemValues) 中，把字节数组转换成字符串和数字类型，组合成Response结构

	Get()会在查询结果的末尾添加 "\r\nEND",根据"END"停止从cache读取,不会把"END"存入Get()结果，但是"\r\n"会留在结果中，处理时要去掉末尾的"\r\n"
		bytes := itemValues[:len(itemValues)-2]

*/
