// Package client (v2) is the current official Go client for InfluxDB.
package client // import "github.com/influxdata/influxdb1-client/v2"

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
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

/*
Merge
todo	合并不同查询结果的表	假设查询只有时间范围不同，其余如 GROUP BY tag value 和 columns 都相同
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
	timeRange := uint64(timePrecision)

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
	startTime uint64
	endTime   uint64
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

// done todo 两个结果的表的数量未必一致，需要保留所有表的数据，现在只有 1 中的所有表 和 2 中的与 1 对应的表， 只在 2 中的表被跳过了
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

/* 获取查询结果的时间范围 */
// 从 response 中取数据，可以确保起止时间都有，只需要进行类型转换
// done todo 一个查询结果的多张表的起止时间可能是不同的，是否需要找到所有表的最小和最大时间
func GetResponseTimeRange(resp *Response) (uint64, uint64) {
	var minStartTime uint64
	var maxEndTime uint64

	minStartTime = math.MaxInt64
	maxEndTime = 0
	for s := range resp.Results[0].Series {
		/* 获取一张表的起止时间（string） */
		length := len(resp.Results[0].Series[s].Values)      //一个结果表中有多少条记录
		start := resp.Results[0].Series[s].Values[0][0]      // 第一条记录的时间		第一个查询结果
		end := resp.Results[0].Series[s].Values[length-1][0] // 最后一条记录的时间
		st := start.(string)
		et := end.(string)

		/* 转换成 uint64 计算时间误差 */
		tsst, _ := time.Parse(time.RFC3339, st) //字符串转换成时间戳	转换的格式layout必须是这个时间，不能改
		tset, _ := time.Parse(time.RFC3339, et)
		uist := uint64(tsst.UnixNano()) //时间戳转换成 uint64 , 纳秒精度
		uiet := uint64(tset.UnixNano())

		/* 更新起止时间范围 	两个时间可能不在一个表中 ? */
		if minStartTime > uist {
			minStartTime = uist
		}
		if maxEndTime < uiet {
			maxEndTime = uiet
		}
	}

	return minStartTime, maxEndTime
}

/*
SemanticSegment 根据查询语句和数据库返回数据组成字段，用作存入cache的key
*/
func SemanticSegment(queryString string, response *Response) []string {
	SM := GetSM(response)
	SPST := GetSPST(queryString)
	Interval := GetInterval(queryString)
	SF, Aggr := GetSFSG(queryString)

	//var result string
	//result = fmt.Sprintf("%s#{%s}#%s#{%s,%s}", SM, SF, SPST, Aggr, Interval)

	var resultArr []string
	for i := range SM {
		str := fmt.Sprintf("{%s}#{%s}#%s#{%s,%s}", SM[i], SF, SPST, Aggr, Interval)
		resultArr = append(resultArr, str)
	}

	return resultArr
}

/* 判断结果是否为空，并提取出tags数组，用于规范tag map的输出顺序 */
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
func GetSM(resp *Response) []string {
	var result string
	var tagArr []string

	/* 从查询语句中提取出tag名称，刨除时间间隔，用tag名称规范map的输出，避免乱序 */
	//parser := influxql.NewParser(strings.NewReader(queryString))
	//stmt, _ := parser.ParseStatement()
	//s := stmt.(*influxql.SelectStatement)
	//_, tagArr = s.Dimensions.Normalize()

	tagArr = GetTagNameArr(resp)

	// 格式： {(name.tag_key=tag_value)...}
	// 有查询结果 且 结果中有tag	当结果为空或某些使用聚合函数的情况都会输出 "empty tag"
	//if len(resp.Results[0].Series) > 0 && len(resp.Results[0].Series[0].Tags) > 0 {
	if len(tagArr) > 0 {
		//result += "{"
		for r := range resp.Results { //包括 Statement_id , Series[] , Messages[] , Error	只用了 Series[]

			/* 遍历查询结果的所有表 */
			for s := range resp.Results[r].Series { //包括  measurement name, GROUP BY tags, Columns[] , Values[][], partial		只用了 Columns[]和 Values[][]

				measurementName := resp.Results[r].Series[s].Name

				for _, tagName := range tagArr {
					result += fmt.Sprintf("(%s.%s=%s)", measurementName, tagName, resp.Results[r].Series[s].Tags[tagName])
				}
				result += " " //不同表之间用空格分隔

			}

		}
	} else {
		return []string{"empty tag"}
	}

	result = result[:len(result)-1] //去掉最后的空格
	//result += "}"                   //标志转换结束

	resArr := strings.Split(result, " ")

	return resArr
}

/* 从查询语句中获取fields和聚合函数 */
func GetSFSG(query string) (string, string) {
	regStr := `(?i)SELECT (.+) FROM.+`
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

	if strings.HasSuffix(FGstr, ")") { // 如果这部分有括号，说明有聚合函数 ?
		/* get aggr */
		fields := influxql.Fields{}
		expr, err := influxql.NewParser(strings.NewReader(FGstr)).ParseExpr()
		if err != nil {
			panic(err)
		}
		fields = append(fields, &influxql.Field{Expr: expr})
		aggrs := fields.Names()
		aggr = strings.Join(aggrs, ",") //获取聚合函数

		/*
			//用字符串处理聚合函数
				var index int
				for i, s := range FGstr {
					if reflect.DeepEqual(s, "(") {	// 括号前面的是聚合函数
						index = i
						break
					}
				}
				aggr = FGstr[ : index]
				aggr = strings.ToLower(aggr)
		*/

		/* get fields */
		flds += "time,"
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
		flds = strings.Join(s.ColumnNames(), ",")
	}

	return flds, aggr
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
	uint_start_time := uint64(start_time.UnixNano()) //时间戳转换成 uint64 , 纳秒精度
	uint_end_time := uint64(end_time.UnixNano())
	string_start_time := strconv.FormatUint(uint_start_time, 10) // 转换成字符串
	string_end_time := strconv.FormatUint(uint_end_time, 10)

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
		binaryExpr := cond.(*influxql.BinaryExpr)
		var datatype []string
		predicates, datatypes := PreOrderTraverseBinaryExpr(binaryExpr, &conds, &datatype)
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
func PreOrderTraverseBinaryExpr(node *influxql.BinaryExpr, predicates *[]string, datatypes *[]string) (*[]string, *[]string) {
	if node.Op != influxql.AND && node.Op != influxql.OR { // 不是由AND或OR连接的，说明表达式不可再分，存入结果数组
		str := node.String()

		// 用字符串获取每个二元表达式的数据类型	可能有问题，具体看怎么用
		if strings.Contains(str, "'") { // 有单引号的都是字符串
			*datatypes = append(*datatypes, "string")
		} else if strings.EqualFold(node.RHS.String(), "true") || strings.EqualFold(node.RHS.String(), "false") { // 忽略大小写，相等就是 bool
			*datatypes = append(*datatypes, "bool")
		} else if strings.Contains(str, ".") { // 带小数点就是 double
			*datatypes = append(*datatypes, "double")
		} else { // 什么都没有是 int
			*datatypes = append(*datatypes, "int")
		}

		str = strings.ReplaceAll(str, " ", "") //去掉空格
		*predicates = append(*predicates, str)
		return predicates, datatypes
	}

	if node.LHS != nil { //遍历左子树
		binaryExprL := GetBinaryExpr(node.LHS.String())
		PreOrderTraverseBinaryExpr(binaryExprL, predicates, datatypes)
	} else {
		return predicates, datatypes
	}

	if node.RHS != nil { //遍历右子树
		binaryExprR := GetBinaryExpr(node.RHS.String())
		PreOrderTraverseBinaryExpr(binaryExprR, predicates, datatypes)
	} else {
		return predicates, datatypes
	}

	return predicates, datatypes
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
						//str := jsonNumber.String()
						//result += str
						jsonNumber.String()
					} else {
						result += "N"
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

func (resp *Response) ToByteArray() []byte {
	result := make([]byte, 0)

	tags := GetTagNameArr(resp)
	if ResponseIsEmpty(resp) {
		return []byte("empty response")
	}

	for s := range resp.Results[0].Series { //包括  measurement name, GROUP BY tags, Columns[] , Values[][], partial		只用了 Columns[]和 Values[][]

		result = append(result, []byte("SCHEMA ")...)

		// 列名	Columns[] 	[]string类型
		for c := range resp.Results[0].Series[s].Columns {
			result = append(result, []byte(resp.Results[0].Series[s].Columns[c])...)
			result = append(result, []byte(" ")...) //用空格分隔列名
		}

		// tags		map元素顺序输出
		for t, tag := range tags {
			result = append(result, []byte(fmt.Sprintf("%s=%s ", tags[t], resp.Results[0].Series[s].Tags[tag]))...)
		}
		result = append(result, []byte("\r\n")...) // 列名和数据间换行  "\r\n" 还是 "\n" ?	用 "\r\n", 因为 memcache 读取换行符是 CRLF("\r\n")

		for v := range resp.Results[0].Series[s].Values {
			for vv := range resp.Results[0].Series[s].Values[v] { // 从JSON转换出来之后只有 string 和 json.Number 两种类型
				if resp.Results[0].Series[s].Values[v][vv] == nil { //值为空时输出一个占位标志
					result = append(result, []byte("_")...)
				} else if str, ok := resp.Results[0].Series[s].Values[v][vv].(string); ok { // 字符串类型
					result = append(result, []byte(str)...)
				} else if jsonNumber, ok := resp.Results[0].Series[s].Values[v][vv].(json.Number); ok { // 数字类型
					numberByteArray, _ := json.Marshal(jsonNumber)
					result = append(result, numberByteArray...)
				} else {
					result = append(result, []byte("_")...)
				}
				result = append(result, []byte(" ")...) // 一行 Value 的数据之间用空格分隔
			}
			result = append(result, []byte("\r\n")...) // Values 之间换行
		}
		//result = append(result, []byte("\r\n")...) // Series 之间换行
	}

	result = append(result, []byte("end")...)
	return result
}