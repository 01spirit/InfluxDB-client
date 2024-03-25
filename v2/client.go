// Package client (v2) is the current official Go client for InfluxDB.
package client // import "github.com/influxdata/influxdb1-client/v2"

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	stscache "github.com/InfluxDB-client/memcache"
	"github.com/InfluxDB-client/models"
	fatcache "github.com/bradfitz/gomemcache/memcache"
	//"github.com/influxdata/influxdb1-client/models"
	"io"
	"io/ioutil"
	"log"
	"mime"
	"net/http"
	"net/url"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"
)

type ContentEncoding string

// 连接数据库
var c, err = NewHTTPClient(HTTPConfig{
	Addr: "http://10.170.48.244:8086",
	//Addr: "http://localhost:8086",
})

// 连接cache
var stscacheConn = stscache.New("localhost:11214")

var fatcacheConn = fatcache.New("localhost:11213")

// 数据库中所有表的tag和field
var TagKV = GetTagKV(c, MyDB)
var Fields = GetFieldKeys(c, MyDB)
var QueryTemplates = make(map[string]string) // 存放查询模版及其语义段；查询模板只替换了时间范围，语义段没变

// 结果转换成字节数组时string类型占用字节数
const STRINGBYTELENGTH = 25

// fatcache 设置存入的时间间隔		"1.5h"	"15m"
const TimeSize = "8m"

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

//func Set(queryString string, c Client, mc *memcache.Client) error {
//	query := NewQuery(queryString, MyDB, "ns")
//	resp, err := c.Query(query)
//	if err != nil {
//		return err
//	}
//
//	semanticSegment := SemanticSegment(queryString, resp)
//	startTime, endTime := GetResponseTimeRange(resp)
//	respCacheByte := resp.ToByteArray(queryString)
//	tableNumbers := int64(len(resp.Results[0].Series))
//
//	item := memcache.Item{
//		Key:         semanticSegment,
//		Value:       respCacheByte,
//		Flags:       0,
//		Expiration:  0,
//		CasID:       0,
//		Time_start:  startTime,
//		Time_end:    endTime,
//		NumOfTables: tableNumbers,
//	}
//
//	err = mc.Set(&item)
//
//	if err != nil {
//		return err
//	}
//
//	return nil
//}

/* TSCache */
// 把字节流转换成查询结果
func TSCacheByteToValue(byteArray []byte) *Response {
	/* 没有数据 */
	if len(byteArray) == 0 {
		return nil
	}

	data_len := make(map[string]int) // 每列的数据类型对应的字节长度
	data_len = map[string]int{"int64": 8, "float64": 8, "string": 25, "bool": 1}

	measurement_name := ""
	cur_datatype := ""
	cur_col := ""
	tag_field := make([]string, 0)              // 返回的第一个参数，由 measurement_name, tag, field, datatype 连接成的字符串，直接从字节数组中转换出来，暂时不处理
	field_total_length := make([]int64, 0)      // 返回的第二个参数，每列数据的总长度
	datatypes := make([]string, 0)              // 包含在 tag_field 中的，每列的数据类型
	columns := make([]string, 0)                // 列名
	value_all_field := make([][]interface{}, 0) // 返回的第三个参数，所有列的所有数据
	value_per_field := make([]interface{}, 0)   // 每列的所有数据
	tags := make(map[string]string)             // 每张子表的所有 tag

	index := 0               // byteArray 数组的索引，指示当前要转换的字节的位置
	length := len(byteArray) // Get()获取的总字节数
	var cur_tag_field string // 当前处理的一个 tag_field 字符串
	var cur_field_len int64  // 当前列的数据总长度
	var cur_data_len int     // 当前列的单个数据的长度

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

		/* 第一个参数 tag_field */
		if byteArray[index] == byte('(') { // 以 '(' 开始
			tfStartIndex := index
			for byteArray[index] != byte(' ') { // 以 ' ' 结束
				index++
			}
			tfEndIndex := index
			cur_tag_field = string(byteArray[tfStartIndex:tfEndIndex]) // 截取并转换成字符串
			tag_field = append(tag_field, cur_tag_field)

			/* 第二个参数 field_total_len */
			index++                // 两个参数间由一个空格分隔，跳过空格
			lenStartIndex := index // 索引指向 field length 的第一个字节
			index += 8
			lenEndIndex := index // 索引指向 field length 的最后一个字节
			cur_field_len, err = ByteArrayToInt64(byteArray[lenStartIndex:lenEndIndex])
			if err != nil {
				log.Fatal(err)
			}
			field_total_length = append(field_total_length, cur_field_len)

			/* 从 tag_field 中取出每列的数据类型 */
			dtStartIndex := strings.Index(cur_tag_field, "[") + 1
			dtEndIndex := len(cur_tag_field) - 1
			cur_datatype = cur_tag_field[dtStartIndex:dtEndIndex]
			datatypes = append(datatypes, cur_datatype)
			// 每列的列名
			colStartIndex := strings.LastIndex(cur_tag_field, ".") + 1
			colEndIndex := dtStartIndex - 1
			cur_col = cur_tag_field[colStartIndex:colEndIndex]
			columns = append(columns, cur_col)
		}

		/* 每列的具体数据 */
		cur_data_len = data_len[cur_datatype]        // 单个数据的长度
		row_num := int(cur_field_len) / cur_data_len // 数据总行数
		value_per_field = nil
		for len(value_per_field) < row_num {
			switch cur_datatype {
			case "bool":
				bStartIdx := index
				index += 1 //	索引指向当前数据的后一个字节
				bEndIdx := index
				tmp, err := ByteArrayToBool(byteArray[bStartIdx:bEndIdx])
				if err != nil {
					log.Fatal(err)
				}
				value_per_field = append(value_per_field, tmp)
				break
			case "int64":
				iStartIdx := index
				index += 8 // 索引指向当前数据的后一个字节
				iEndIdx := index
				tmp, err := ByteArrayToInt64(byteArray[iStartIdx:iEndIdx])
				if err != nil {
					log.Fatal(err)
				}
				str := strconv.FormatInt(tmp, 10)
				jNumber := json.Number(str) // int64 转换成 json.Number 类型	;Response中的数字类型只有json.Number	int64和float64都要转换成json.Number
				value_per_field = append(value_per_field, jNumber)
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
				value_per_field = append(value_per_field, jNumber)
				break
			default: // string
				sStartIdx := index
				index += STRINGBYTELENGTH // 索引指向当前数据的后一个字节
				sEndIdx := index
				tmp := ByteArrayToString(byteArray[sStartIdx:sEndIdx])
				value_per_field = append(value_per_field, tmp) // 存放一行数据中的每一列
				break
			}

		}
		value_all_field = append(value_all_field, value_per_field)
	}

	/* 还原表结构，构造成 Response 返回 */
	modelsRows := make([]models.Row, 0)

	// tag_field : (measurement.tag1=name1,tag2=name2).field_name[datatype]
	// value_all_field [][]interface{} 插入
	mnStartIndex := strings.Index(cur_tag_field, "(") + 1
	mnEndIndex := strings.Index(cur_tag_field, ".")
	measurement_name = cur_tag_field[mnStartIndex:mnEndIndex]

	/* tags */
	series_num := 0 // 子表数量
	col_num := 0    // 每张表的列数
	tag_str_map := make(map[string]int)
	for i := range tag_field {
		tagStartIndex := strings.Index(tag_field[i], ".") + 1
		tagEndIndex := strings.Index(tag_field[i], ")")
		tmp_tags := tag_field[i][tagStartIndex:tagEndIndex]
		tag_str_map[tmp_tags]++
	}
	series_num = len(tag_str_map)
	col_num = len(columns) / len(tag_str_map) // 每张子表的 tag_str 相同	，总列数 / tag_str 总数 即为每张表的列数

	/* values */
	valuess := make([][][]interface{}, 0)
	values := make([][]interface{}, 0)
	value := make([]interface{}, 0)
	for i := 0; i < series_num; i++ { // 第 i 张子表
		values = nil
		for j := range value_all_field[i*col_num] { // 一张子表的数据行数
			value = nil
			for k := i * col_num; k < (i+1)*col_num; k++ {
				value = append(value, value_all_field[k][j])
			}
			values = append(values, value)
		}
		valuess = append(valuess, values)
	}

	tag_str_arr := make([]string, 0)
	for key := range tag_str_map {
		tag_str_arr = append(tag_str_arr, key)
	}
	slices.Sort(tag_str_arr)

	for i, key := range tag_str_arr {
		split_tags := strings.Split(key, ",")
		for _, tag := range split_tags {
			eqIndex := strings.Index(tag, "=")
			if eqIndex <= 0 {
				break
			}
			k := tag[:eqIndex]
			v := tag[eqIndex+1:]
			tags[k] = v
		}

		tmpTags := make(map[string]string)
		for key, value := range tags {
			tmpTags[key] = value
		}
		/* 构造一个子表的结构 Series */
		seriesTmp := Series{
			Name:    measurement_name,
			Tags:    tmpTags,
			Columns: columns[:col_num],
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

/* 用于 TSCache， 按列存储的形式 */
// 把查询结果转换成字节流
func TSCacheValueToByte(resp *Response) []byte {
	result := make([]byte, 0)

	/* 结果为空 */
	if ResponseIsEmpty(resp) {
		return StringToByteArray("empty response")
	}

	//datatypes := GetDataTypeArrayFromResponse(resp)

	/* 列名、数据长度、具体数据 */
	tag_field, field_len, field_value := TSCacheParameter(resp)

	// 子表数量、列的数量
	//table_num := len(tag_field)
	column_num := len(tag_field[0])

	// 存入字节数组
	for i := range field_value {
		col_len, _ := Int64ToByteArray(field_len[i/column_num][i%column_num])
		result = append(result, []byte(tag_field[i/column_num][i%column_num])...) // 列名
		result = append(result, []byte(" ")...)                                   // 空格
		result = append(result, col_len...)                                       // 数据长度

		/* 数据 */
		for j := range field_value[i] {
			result = append(result, field_value[i][j]...)
		}

	}

	return result
}

// 获取把查询结果转换成字节流所需的数据，包括 列名、每列数据的总长度、每列的具体数据
func TSCacheParameter(resp *Response) ([][]string, [][]int64, [][][]byte) {
	var tag_value []string     // 每张子表的所有 tag 的值连接成字符串
	var tag_field [][]string   // 每张子表的多个列，每个列的列名和 tag 连接成字符串
	var field_len [][]int64    // 每张子表的每个列的数据的长度
	var field_value [][][]byte // 每张子表的每个列的数据的数组

	if ResponseIsEmpty(resp) {
		return nil, nil, nil
	}

	data_len := make(map[string]int) // 每列的数据类型对应的字节长度
	data_len = map[string]int{"int64": 8, "float64": 8, "string": 25, "bool": 1}
	datatype := GetDataTypeArrayFromResponse(resp) // 每一列的数据类型
	tags := GetTagNameArr(resp)                    // 结果中的所有 tag 的名称
	measurement_name := resp.Results[0].Series[0].Name
	for r := range resp.Results {
		for s := range resp.Results[r].Series { // 结果中的每张子表
			// 一张子表的所有 tag 的值
			tmp_tag_value := fmt.Sprintf("%s.", measurement_name)
			for t, tag := range tags { // 一张子表的所有 tag 的值
				tmp_tag_value += fmt.Sprintf("%s=%s,", tags[t], resp.Results[r].Series[s].Tags[tag])
			}
			tag_value = append(tag_value, tmp_tag_value[:len(tmp_tag_value)-1])

			tfld := make([]string, 0)
			fldl := make([]int64, 0)

			row_num := len(resp.Results[r].Series[s].Values)   // 每张子表的数据行数
			for c := range resp.Results[r].Series[s].Columns { // 每张子表的每个列
				// 每列的列名连接成字符串
				tmp_tag_field := ""
				tmp_tag_field = fmt.Sprintf("(%s).%s[%s]", tag_value[s], resp.Results[r].Series[s].Columns[c], datatype[c])
				tfld = append(tfld, tmp_tag_field)

				// 每张子表的每个列的数据的总字节数
				tmp_field_len := 0
				tmp_field_len = data_len[datatype[c]] * row_num
				fldl = append(fldl, int64(tmp_field_len))

				// 每张子表的每个列的数据的数组
				fldv := make([][]byte, 0)
				for _, values := range resp.Results[r].Series[s].Values {
					tmp_value_byte := InterfaceToByteArray(c, datatype[c], values[c])
					fldv = append(fldv, tmp_value_byte)
				}
				field_value = append(field_value, fldv)
			}
			tag_field = append(tag_field, tfld)
			field_len = append(field_len, fldl)
		}
	}

	return tag_field, field_len, field_value
}

/* Fatcache */
// 根据时间尺度分割查询结果	返回分块后的数据，以及每块对应的查询语句时间字符串
func SplitResponseValuesByTime(queryString string, resp *Response, timeSize string) ([][][][]interface{}, []int64, []int64) {
	result := make([][][][]interface{}, 0)
	starts := make([]int64, 0)
	ends := make([]int64, 0)

	if ResponseIsEmpty(resp) {
		return nil, nil, nil
	}

	// 获取划分查询结果所用的时间间隔
	duration, err := time.ParseDuration(timeSize)
	if err != nil {
		log.Fatalln(err)
	}
	interval := int64(duration.Seconds())

	// 查询结果的起止时间，时间跨度
	startTime, endTime := GetQueryTimeRange(queryString)
	allTime := endTime - startTime

	/* 结果的时间跨度小于要分割的时间间隔，直接返回结果 */
	if allTime <= interval {
		values := make([][][]interface{}, 0)
		for _, series := range resp.Results[0].Series { // 每个子表的结果分开存
			values = append(values, series.Values)
		}
		result = append(result, values)
		starts = append(starts, startTime)
		ends = append(ends, endTime)
		return result, starts, ends
	}

	// 查询结果要划分成的块数，向上取整
	splitNum := int((allTime + interval - 1) / interval)
	var idx int64 = 0
	for i := 0; i < splitNum; i++ {
		valuess := make([][][]interface{}, 0)
		tmpStartTime := startTime + interval*idx // 每块对应查询语句的起始时间
		idx++
		tmpEndTime := startTime + interval*idx // 每块对应的查询语句的结束时间
		if tmpEndTime > endTime {              // 最后一块的结束时间
			tmpEndTime = endTime
		}

		starts = append(starts, tmpStartTime)
		ends = append(ends, tmpEndTime)

		for _, series := range resp.Results[0].Series {
			values := make([][]interface{}, 0)
			for _, vals := range series.Values {
				timestamp := vals[0].(json.Number)
				timestampInt, err := timestamp.Int64()
				if err != nil {
					log.Fatalf("%s\n%s", err.Error(), queryString)
				}
				if timestampInt >= tmpStartTime && timestampInt < tmpEndTime { // 比较每条数据的时间戳是否在分块的查询时间范围内，在的话就取出
					values = append(values, vals)
				} else if timestampInt < tmpStartTime {
					continue
				} else {
					break
				}
			}
			valuess = append(valuess, values)
		}
		result = append(result, valuess)
	}

	return result, starts, ends
}

// 按时间尺度分块，存入 cache
func SetToFatache(queryString string, timeSize string) {
	semanticSegment := GetSemanticSegment(queryString)
	qs := NewQuery(queryString, MyDB, "s")
	resp, _ := c.Query(qs)
	datatype := GetDataTypeArrayFromResponse(resp)

	/* 把查询模版存入全局 map */
	queryTemplate := GetQueryTemplate(queryString)
	QueryTemplates[queryTemplate] = semanticSegment

	valuess, starts, ends := SplitResponseValuesByTime(queryString, resp, timeSize)

	for i, values := range valuess { // 查询结果分块
		//  set 分块数据	set seg[timerange] vlen	value
		byteVal := make([]byte, 0)
		for _, value := range values { // 每个分块的子表
			for _, val := range value { // 每个子表的行
				for t, v := range val { // 每个行的列
					tmp := InterfaceToByteArray(t, datatype[t], v)
					byteVal = append(byteVal, tmp...)
				}
			}
		}
		stStr := starts[i]
		etStr := ends[i]
		ss := fmt.Sprintf("%s[%d,%d]", semanticSegment, stStr, etStr)

		err := fatcacheConn.Set(&fatcache.Item{
			Key:        ss,
			Value:      byteVal,
			Flags:      0,
			Expiration: 0,
			CasID:      0,
		})
		if err != nil {
			//log.Fatal(err)
			//log.Println("NOT STORED.")
		} else {
			log.Printf("store:%s\n", ss)
			log.Println("STORED.")
			log.Println("set byte length:", len(byteVal))
		}
	}

}

// 按时间尺度分块，向 Fatcache 查询，返回结果的原始字节流
func GetFromFatcache(queryString string, timeSize string) [][]byte {
	results := make([][]byte, 0)

	queryTemplate := GetQueryTemplate(queryString)
	if semanticSegment, ok := QueryTemplates[queryTemplate]; !ok { // 不存在该查询的模版，说明之前没存过
		return nil
	} else { // 存在该查询模版，尝试 Get
		// 获取划分查询结果所用的时间间隔
		duration, err := time.ParseDuration(timeSize)
		if err != nil {
			log.Fatalln(err)
		}
		interval := int64(duration.Seconds())

		// 查询结果的起止时间，时间跨度
		startTime, endTime := GetQueryTimeRange(queryString)
		allTime := endTime - startTime

		// 查询时间范围小于分割的时间尺度，直接查询
		if allTime <= interval {
			// 直接向 cache Get
			startTime, endTime := GetQueryTimeRange(queryString)
			ss := fmt.Sprintf("%s[%d,%d]", semanticSegment, startTime, endTime)

			/* 向 cache Get */
			items, err := fatcacheConn.Get(ss)
			if err != nil {
				log.Println(err)
			} else {
				log.Printf("\tget:%s\n", ss)
				log.Println("\tGET.")
				log.Println("\tget byte length:", len(items.Value))

				results = append(results, items.Value)
				return results
			}

		} else {
			/* 按照时间尺度分割查询，分别向cache Get */

			// 分割的块数
			splitNum := int((allTime + interval - 1) / interval)
			var idx int64 = 0
			for i := 0; i < splitNum; i++ {
				tmpStartTime := startTime + interval*idx // 每块对应查询语句的起始时间
				idx++
				tmpEndTime := startTime + interval*idx // 每块对应的查询语句的结束时间
				if tmpEndTime > endTime {              // 最后一块的结束时间
					tmpEndTime = endTime
				}

				/* 向 cache Get */
				ss := fmt.Sprintf("%s[%d,%d]", semanticSegment, tmpStartTime, tmpEndTime)
				items, err := fatcacheConn.Get(ss)
				if err != nil {
					log.Println(err)
				} else {
					log.Printf("\tget:%s\n", ss)
					log.Println("\tGET.")
					log.Println("\tget byte length:", len(items.Value))
					results = append(results, items.Value)
				}

			}

			return results
		}
	}
	return results
}

/* STsCache */
/*
	1. 客户端接收查询语句
	2. 客户端向 cache 系统查询，得到部分结果
	3. 生成这条查询语句的模版，把时间范围用占位符替换
	4. 得到要向数据库查询的时间范围，带入模版，向数据库查询剩余数据
	5. 客户端把剩余数据存入 cache
*/
func IntegratedClient(queryString string) {
	/* 原始查询语句的时间范围 */
	startTime, endTime := GetQueryTimeRange(queryString) // 当查询的时间只有一半时，另一个值为 -1; 当查询时间为 "=" 时，两值相等

	/* 原始查询语句替换掉时间之后的的模版 */
	queryTemplate := GetQueryTemplate(queryString) // 时间用 '?' 代替

	/* 从查询模版获取语义段，或构造语义段并存入查询模版 */
	semanticSegment := ""
	if ss, ok := QueryTemplates[queryTemplate]; !ok { // 查询模版中不存在该查询
		semanticSegment = GetSemanticSegment(queryString)
		/* 存入全局 map */
		QueryTemplates[queryTemplate] = semanticSegment
	} else {
		semanticSegment = ss
	}

	/* 向 cache 查询数据 */
	values, _, err := stscacheConn.Get(semanticSegment, startTime, endTime)
	if err == stscache.ErrCacheMiss { // 缓存未命中
		log.Printf("Key not found in cache")

		/* 向数据库查询全部数据，存入 cache */
		q := NewQuery(queryString, MyDB, "s")
		resp, _ := c.Query(q)

		if resp != nil {
			//ss := GetSemanticSegment(queryString)
			st, et := GetResponseTimeRange(resp)
			numOfTab := GetNumOfTable(resp)
			remainValues := ResponseToByteArray(resp, queryString)

			/* 存入 cache */
			err = stscacheConn.Set(&stscache.Item{Key: semanticSegment, Value: remainValues, Time_start: st, Time_end: et, NumOfTables: numOfTab})
			if err != nil {
				log.Fatalf("Error setting value: %v", err)
			} else {
				log.Printf("STORED.")
			}
		}
	} else if err != nil { // 异常
		log.Fatalf("Error getting value: %v", err)
	} else { // 缓存部分命中或完全命中
		log.Printf("GET.")

		/* 把查询结果从字节流转换成 Response 结构 */
		convertedResponse := ByteArrayToResponse(values)

		/* 从cache返回的数据的时间范围 */
		recv_start_time, recv_end_time := GetResponseTimeRange(convertedResponse)

		/* 向数据库查询剩余数据 	暂时只考虑一侧未命中的情况，不考虑两个时间范围都未命中的情况*/
		var remainResponse *Response
		var remainQuery string
		if recv_start_time > startTime { // 起始时间未命中，结束时间命中
			/* 把 int64 类型的时间转换成 RFC3339 格式的时间戳，替换到查询模版上 */
			remain_start_time_string := TimeInt64ToString(startTime)
			remain_end_time_string := TimeInt64ToString(recv_start_time)
			remainQuery = strings.Replace(queryTemplate, "?", remain_start_time_string, 1)
			remainQuery = strings.Replace(remainQuery, "?", remain_end_time_string, 1)

			q := NewQuery(remainQuery, MyDB, "s")
			remainResponse, _ = c.Query(q)
		} else if recv_end_time < endTime { // 起始时间命中，结束时间未命中
			remain_start_time_string := TimeInt64ToString(recv_end_time)
			remain_end_time_string := TimeInt64ToString(endTime)

			/* 替换到查询模版中 */
			remainQuery = strings.Replace(queryTemplate, "?", "'"+remain_start_time_string+"'", 1)
			remainQuery = strings.Replace(remainQuery, "?", "'"+remain_end_time_string+"'", 1)

			/* 向数据库查询剩余数据 */
			q := NewQuery(remainQuery, MyDB, "s")
			remainResponse, _ = c.Query(q)
		}

		/* 把剩余数据存入 cache */
		if remainResponse != nil {
			//remainSemanticSegment := GetSemanticSegment(remainQuery)
			remainSemanticSegment := semanticSegment
			remain_start_time, remain_end_time := GetResponseTimeRange(remainResponse)
			numOfTab := GetNumOfTable(remainResponse)
			remainValues := ResponseToByteArray(remainResponse, remainQuery)

			err = stscacheConn.Set(&stscache.Item{Key: remainSemanticSegment, Value: remainValues, Time_start: remain_start_time, Time_end: remain_end_time, NumOfTables: numOfTab})
			if err != nil {
				log.Fatalf("Error setting value: %v", err)
			} else {
				log.Printf("STORED.")
			}
		}

	}

}

// todo :
// lists:

// done 整合Set()函数

// done 在工作站上安装InfluxDB1.8，下载样例数据库

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
