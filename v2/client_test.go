package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	stscache "github.com/InfluxDB-client/memcache"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestUDPClient_Query(t *testing.T) {
	config := UDPConfig{Addr: "localhost:8089"}
	c, err := NewUDPClient(config)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
	defer c.Close()
	query := Query{}
	_, err = c.Query(query)
	if err == nil {
		t.Error("Querying UDP client should fail")
	}
}

func TestUDPClient_Ping(t *testing.T) {
	config := UDPConfig{Addr: "localhost:8089"}
	c, err := NewUDPClient(config)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
	defer c.Close()

	rtt, version, err := c.Ping(0)
	if rtt != 0 || version != "" || err != nil {
		t.Errorf("unexpected error.  expected (%v, '%v', %v), actual (%v, '%v', %v)", 0, "", nil, rtt, version, err)
	}
}

func TestUDPClient_Write(t *testing.T) {
	config := UDPConfig{Addr: "localhost:8089"}
	c, err := NewUDPClient(config)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
	defer c.Close()

	bp, err := NewBatchPoints(BatchPointsConfig{})
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}

	fields := make(map[string]interface{})
	fields["value"] = 1.0
	pt, _ := NewPoint("cpu", make(map[string]string), fields)
	bp.AddPoint(pt)

	err = c.Write(bp)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
}

func TestUDPClient_BadAddr(t *testing.T) {
	config := UDPConfig{Addr: "foobar@wahoo"}
	c, err := NewUDPClient(config)
	if err == nil {
		defer c.Close()
		t.Error("Expected resolve error")
	}
}

func TestUDPClient_Batches(t *testing.T) {
	var logger writeLogger
	var cl udpclient

	cl.conn = &logger
	cl.payloadSize = 20 // should allow for two points per batch

	// expected point should look like this: "cpu a=1i"
	fields := map[string]interface{}{"a": 1}

	p, _ := NewPoint("cpu", nil, fields, time.Time{})

	bp, _ := NewBatchPoints(BatchPointsConfig{})

	for i := 0; i < 9; i++ {
		bp.AddPoint(p)
	}

	if err := cl.Write(bp); err != nil {
		t.Fatalf("Unexpected error during Write: %v", err)
	}

	if len(logger.writes) != 5 {
		t.Errorf("Mismatched write count: got %v, exp %v", len(logger.writes), 5)
	}
}

func TestUDPClient_Split(t *testing.T) {
	var logger writeLogger
	var cl udpclient

	cl.conn = &logger
	cl.payloadSize = 1 // force one field per point

	fields := map[string]interface{}{"a": 1, "b": 2, "c": 3, "d": 4}

	p, _ := NewPoint("cpu", nil, fields, time.Unix(1, 0))

	bp, _ := NewBatchPoints(BatchPointsConfig{})

	bp.AddPoint(p)

	if err := cl.Write(bp); err != nil {
		t.Fatalf("Unexpected error during Write: %v", err)
	}

	if len(logger.writes) != len(fields) {
		t.Errorf("Mismatched write count: got %v, exp %v", len(logger.writes), len(fields))
	}
}

type writeLogger struct {
	writes [][]byte
}

func (w *writeLogger) Write(b []byte) (int, error) {
	w.writes = append(w.writes, append([]byte(nil), b...))
	return len(b), nil
}

func (w *writeLogger) Close() error { return nil }

func TestClient_Query(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data Response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(data)
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, _ := NewHTTPClient(config)
	defer c.Close()

	query := Query{}
	_, err := c.Query(query)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
}

func TestClient_QueryWithRP(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		params := r.URL.Query()
		if got, exp := params.Get("db"), "db0"; got != exp {
			t.Errorf("unexpected db query parameter: %s != %s", exp, got)
		}
		if got, exp := params.Get("rp"), "rp0"; got != exp {
			t.Errorf("unexpected rp query parameter: %s != %s", exp, got)
		}
		var data Response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(data)
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, _ := NewHTTPClient(config)
	defer c.Close()

	query := NewQueryWithRP("SELECT * FROM m0", "db0", "rp0", "")
	_, err := c.Query(query)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
}

func TestClientDownstream500WithBody_Query(t *testing.T) {
	const err500page = `<html>
	<head>
		<title>500 Internal Server Error</title>
	</head>
	<body>Internal Server Error</body>
</html>`
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err500page))
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, _ := NewHTTPClient(config)
	defer c.Close()

	query := Query{}
	_, err := c.Query(query)

	expected := fmt.Sprintf("received status code 500 from downstream server, with response body: %q", err500page)
	if err.Error() != expected {
		t.Errorf("unexpected error.  expected %v, actual %v", expected, err)
	}
}

func TestClientDownstream500_Query(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, _ := NewHTTPClient(config)
	defer c.Close()

	query := Query{}
	_, err := c.Query(query)

	expected := "received status code 500 from downstream server"
	if err.Error() != expected {
		t.Errorf("unexpected error.  expected %v, actual %v", expected, err)
	}
}

func TestClientDownstream400WithBody_Query(t *testing.T) {
	const err403page = `<html>
	<head>
		<title>403 Forbidden</title>
	</head>
	<body>Forbidden</body>
</html>`
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(err403page))
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, _ := NewHTTPClient(config)
	defer c.Close()

	query := Query{}
	_, err := c.Query(query)

	expected := fmt.Sprintf(`expected json response, got "text/html", with status: %v and response body: %q`, http.StatusForbidden, err403page)
	if err.Error() != expected {
		t.Errorf("unexpected error.  expected %v, actual %v", expected, err)
	}
}

func TestClientDownstream400_Query(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, _ := NewHTTPClient(config)
	defer c.Close()

	query := Query{}
	_, err := c.Query(query)

	expected := fmt.Sprintf(`expected json response, got empty body, with status: %v`, http.StatusForbidden)
	if err.Error() != expected {
		t.Errorf("unexpected error.  expected %v, actual %v", expected, err)
	}
}

func TestClient500_Query(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Influxdb-Version", "1.3.1")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"test"}`))
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, _ := NewHTTPClient(config)
	defer c.Close()

	query := Query{}
	resp, err := c.Query(query)

	if err != nil {
		t.Errorf("unexpected error.  expected nothing, actual %v", err)
	}

	if resp.Err != "test" {
		t.Errorf(`unexpected response error.  expected "test", actual %v`, resp.Err)
	}
}

func TestClient_ChunkedQuery(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data Response
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Influxdb-Version", "1.3.1")
		w.WriteHeader(http.StatusOK)
		enc := json.NewEncoder(w)
		_ = enc.Encode(data)
		_ = enc.Encode(data)
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, err := NewHTTPClient(config)
	if err != nil {
		t.Fatalf("unexpected error.  expected %v, actual %v", nil, err)
	}

	query := Query{Chunked: true}
	_, err = c.Query(query)
	if err != nil {
		t.Fatalf("unexpected error.  expected %v, actual %v", nil, err)
	}
}

func TestClientDownstream500WithBody_ChunkedQuery(t *testing.T) {
	const err500page = `<html>
	<head>
		<title>500 Internal Server Error</title>
	</head>
	<body>Internal Server Error</body>
</html>`
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err500page))
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, err := NewHTTPClient(config)
	if err != nil {
		t.Fatalf("unexpected error.  expected %v, actual %v", nil, err)
	}

	query := Query{Chunked: true}
	_, err = c.Query(query)

	expected := fmt.Sprintf("received status code 500 from downstream server, with response body: %q", err500page)
	if err.Error() != expected {
		t.Errorf("unexpected error.  expected %v, actual %v", expected, err)
	}
}

func TestClientDownstream500_ChunkedQuery(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, _ := NewHTTPClient(config)
	defer c.Close()

	query := Query{Chunked: true}
	_, err := c.Query(query)

	expected := "received status code 500 from downstream server"
	if err.Error() != expected {
		t.Errorf("unexpected error.  expected %v, actual %v", expected, err)
	}
}

func TestClient500_ChunkedQuery(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Influxdb-Version", "1.3.1")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"test"}`))
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, _ := NewHTTPClient(config)
	defer c.Close()

	query := Query{Chunked: true}
	resp, err := c.Query(query)

	if err != nil {
		t.Errorf("unexpected error.  expected nothing, actual %v", err)
	}

	if resp.Err != "test" {
		t.Errorf(`unexpected response error.  expected "test", actual %v`, resp.Err)
	}
}

func TestClientDownstream400WithBody_ChunkedQuery(t *testing.T) {
	const err403page = `<html>
	<head>
		<title>403 Forbidden</title>
	</head>
	<body>Forbidden</body>
</html>`
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(err403page))
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, _ := NewHTTPClient(config)
	defer c.Close()

	query := Query{Chunked: true}
	_, err := c.Query(query)

	expected := fmt.Sprintf(`expected json response, got "text/html", with status: %v and response body: %q`, http.StatusForbidden, err403page)
	if err.Error() != expected {
		t.Errorf("unexpected error.  expected %v, actual %v", expected, err)
	}
}

func TestClientDownstream400_ChunkedQuery(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, _ := NewHTTPClient(config)
	defer c.Close()

	query := Query{Chunked: true}
	_, err := c.Query(query)

	expected := fmt.Sprintf(`expected json response, got empty body, with status: %v`, http.StatusForbidden)
	if err.Error() != expected {
		t.Errorf("unexpected error.  expected %v, actual %v", expected, err)
	}
}

func TestClient_BoundParameters(t *testing.T) {
	var parameterString string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data Response
		r.ParseForm()
		parameterString = r.FormValue("params")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(data)
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, _ := NewHTTPClient(config)
	defer c.Close()

	expectedParameters := map[string]interface{}{
		"testStringParameter": "testStringValue",
		"testNumberParameter": 12.3,
	}

	query := Query{
		Parameters: expectedParameters,
	}

	_, err := c.Query(query)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}

	var actualParameters map[string]interface{}

	err = json.Unmarshal([]byte(parameterString), &actualParameters)
	if err != nil {
		t.Errorf("unexpected error. expected %v, actual %v", nil, err)
	}

	if !reflect.DeepEqual(expectedParameters, actualParameters) {
		t.Errorf("unexpected parameters. expected %v, actual %v", expectedParameters, actualParameters)
	}
}

func TestClient_BasicAuth(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()

		if !ok {
			t.Errorf("basic auth error")
		}
		if u != "username" {
			t.Errorf("unexpected username, expected %q, actual %q", "username", u)
		}
		if p != "password" {
			t.Errorf("unexpected password, expected %q, actual %q", "password", p)
		}
		var data Response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(data)
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL, Username: "username", Password: "password"}
	c, _ := NewHTTPClient(config)
	defer c.Close()

	query := Query{}
	_, err := c.Query(query)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
}

func TestClient_Ping(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data Response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNoContent)
		_ = json.NewEncoder(w).Encode(data)
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, _ := NewHTTPClient(config)
	defer c.Close()

	_, _, err := c.Ping(0)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
}

func TestClient_Concurrent_Use(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, _ := NewHTTPClient(config)
	defer c.Close()

	var wg sync.WaitGroup
	wg.Add(3)
	n := 1000

	errC := make(chan error)
	go func() {
		defer wg.Done()
		bp, err := NewBatchPoints(BatchPointsConfig{})
		if err != nil {
			errC <- fmt.Errorf("got error %v", err)
			return
		}

		for i := 0; i < n; i++ {
			if err = c.Write(bp); err != nil {
				errC <- fmt.Errorf("got error %v", err)
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		var q Query
		for i := 0; i < n; i++ {
			if _, err := c.Query(q); err != nil {
				errC <- fmt.Errorf("got error %v", err)
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			c.Ping(time.Second)
		}
	}()

	go func() {
		wg.Wait()
		close(errC)
	}()

	for err := range errC {
		if err != nil {
			t.Error(err)
		}
	}
}

func TestClient_Write(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		in, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		} else if have, want := strings.TrimSpace(string(in)), `m0,host=server01 v1=2,v2=2i,v3=2u,v4="foobar",v5=true 0`; have != want {
			t.Errorf("unexpected write protocol: %s != %s", have, want)
		}
		var data Response
		w.WriteHeader(http.StatusNoContent)
		_ = json.NewEncoder(w).Encode(data)
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, _ := NewHTTPClient(config)
	defer c.Close()

	bp, err := NewBatchPoints(BatchPointsConfig{})
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
	pt, err := NewPoint(
		"m0",
		map[string]string{
			"host": "server01",
		},
		map[string]interface{}{
			"v1": float64(2),
			"v2": int64(2),
			"v3": uint64(2),
			"v4": "foobar",
			"v5": true,
		},
		time.Unix(0, 0).UTC(),
	)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
	bp.AddPoint(pt)
	err = c.Write(bp)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
}

func TestClient_UserAgent(t *testing.T) {
	receivedUserAgent := ""
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedUserAgent = r.UserAgent()

		var data Response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(data)
	}))
	defer ts.Close()

	_, err := http.Get(ts.URL)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}

	tests := []struct {
		name      string
		userAgent string
		expected  string
	}{
		{
			name:      "Empty user agent",
			userAgent: "",
			expected:  "InfluxDBClient",
		},
		{
			name:      "Custom user agent",
			userAgent: "Test Influx Client",
			expected:  "Test Influx Client",
		},
	}

	for _, test := range tests {

		config := HTTPConfig{Addr: ts.URL, UserAgent: test.userAgent}
		c, _ := NewHTTPClient(config)
		defer c.Close()

		receivedUserAgent = ""
		query := Query{}
		_, err = c.Query(query)
		if err != nil {
			t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
		}
		if !strings.HasPrefix(receivedUserAgent, test.expected) {
			t.Errorf("Unexpected user agent. expected %v, actual %v", test.expected, receivedUserAgent)
		}

		receivedUserAgent = ""
		bp, _ := NewBatchPoints(BatchPointsConfig{})
		err = c.Write(bp)
		if err != nil {
			t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
		}
		if !strings.HasPrefix(receivedUserAgent, test.expected) {
			t.Errorf("Unexpected user agent. expected %v, actual %v", test.expected, receivedUserAgent)
		}

		receivedUserAgent = ""
		_, err := c.Query(query)
		if err != nil {
			t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
		}
		if receivedUserAgent != test.expected {
			t.Errorf("Unexpected user agent. expected %v, actual %v", test.expected, receivedUserAgent)
		}
	}
}

func TestClient_PointString(t *testing.T) {
	const shortForm = "2006-Jan-02"
	time1, _ := time.Parse(shortForm, "2013-Feb-03")
	tags := map[string]string{"cpu": "cpu-total"}
	fields := map[string]interface{}{"idle": 10.1, "system": 50.9, "user": 39.0}
	p, _ := NewPoint("cpu_usage", tags, fields, time1)

	s := "cpu_usage,cpu=cpu-total idle=10.1,system=50.9,user=39 1359849600000000000"
	if p.String() != s {
		t.Errorf("Point String Error, got %s, expected %s", p.String(), s)
	}

	s = "cpu_usage,cpu=cpu-total idle=10.1,system=50.9,user=39 1359849600000"
	if p.PrecisionString("ms") != s {
		t.Errorf("Point String Error, got %s, expected %s",
			p.PrecisionString("ms"), s)
	}
}

func TestClient_PointWithoutTimeString(t *testing.T) {
	tags := map[string]string{"cpu": "cpu-total"}
	fields := map[string]interface{}{"idle": 10.1, "system": 50.9, "user": 39.0}
	p, _ := NewPoint("cpu_usage", tags, fields)

	s := "cpu_usage,cpu=cpu-total idle=10.1,system=50.9,user=39"
	if p.String() != s {
		t.Errorf("Point String Error, got %s, expected %s", p.String(), s)
	}

	if p.PrecisionString("ms") != s {
		t.Errorf("Point String Error, got %s, expected %s",
			p.PrecisionString("ms"), s)
	}
}

func TestClient_PointName(t *testing.T) {
	tags := map[string]string{"cpu": "cpu-total"}
	fields := map[string]interface{}{"idle": 10.1, "system": 50.9, "user": 39.0}
	p, _ := NewPoint("cpu_usage", tags, fields)

	exp := "cpu_usage"
	if p.Name() != exp {
		t.Errorf("Error, got %s, expected %s",
			p.Name(), exp)
	}
}

func TestClient_PointTags(t *testing.T) {
	tags := map[string]string{"cpu": "cpu-total"}
	fields := map[string]interface{}{"idle": 10.1, "system": 50.9, "user": 39.0}
	p, _ := NewPoint("cpu_usage", tags, fields)

	if !reflect.DeepEqual(tags, p.Tags()) {
		t.Errorf("Error, got %v, expected %v",
			p.Tags(), tags)
	}
}

func TestClient_PointUnixNano(t *testing.T) {
	const shortForm = "2006-Jan-02"
	time1, _ := time.Parse(shortForm, "2013-Feb-03")
	tags := map[string]string{"cpu": "cpu-total"}
	fields := map[string]interface{}{"idle": 10.1, "system": 50.9, "user": 39.0}
	p, _ := NewPoint("cpu_usage", tags, fields, time1)

	exp := int64(1359849600000000000)
	if p.UnixNano() != exp {
		t.Errorf("Error, got %d, expected %d",
			p.UnixNano(), exp)
	}
}

func TestClient_PointFields(t *testing.T) {
	tags := map[string]string{"cpu": "cpu-total"}
	fields := map[string]interface{}{"idle": 10.1, "system": 50.9, "user": 39.0}
	p, _ := NewPoint("cpu_usage", tags, fields)

	pfields, err := p.Fields()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fields, pfields) {
		t.Errorf("Error, got %v, expected %v",
			pfields, fields)
	}
}

func TestBatchPoints_PrecisionError(t *testing.T) {
	_, err := NewBatchPoints(BatchPointsConfig{Precision: "foobar"})
	if err == nil {
		t.Errorf("Precision: foobar should have errored")
	}

	bp, _ := NewBatchPoints(BatchPointsConfig{Precision: "ns"})
	err = bp.SetPrecision("foobar")
	if err == nil {
		t.Errorf("Precision: foobar should have errored")
	}
}

func TestBatchPoints_SettersGetters(t *testing.T) {
	bp, _ := NewBatchPoints(BatchPointsConfig{
		Precision:        "ns",
		Database:         "db",
		RetentionPolicy:  "rp",
		WriteConsistency: "wc",
	})
	if bp.Precision() != "ns" {
		t.Errorf("Expected: %s, got %s", bp.Precision(), "ns")
	}
	if bp.Database() != "db" {
		t.Errorf("Expected: %s, got %s", bp.Database(), "db")
	}
	if bp.RetentionPolicy() != "rp" {
		t.Errorf("Expected: %s, got %s", bp.RetentionPolicy(), "rp")
	}
	if bp.WriteConsistency() != "wc" {
		t.Errorf("Expected: %s, got %s", bp.WriteConsistency(), "wc")
	}

	bp.SetDatabase("db2")
	bp.SetRetentionPolicy("rp2")
	bp.SetWriteConsistency("wc2")
	err := bp.SetPrecision("s")
	if err != nil {
		t.Errorf("Did not expect error: %s", err.Error())
	}

	if bp.Precision() != "s" {
		t.Errorf("Expected: %s, got %s", bp.Precision(), "s")
	}
	if bp.Database() != "db2" {
		t.Errorf("Expected: %s, got %s", bp.Database(), "db2")
	}
	if bp.RetentionPolicy() != "rp2" {
		t.Errorf("Expected: %s, got %s", bp.RetentionPolicy(), "rp2")
	}
	if bp.WriteConsistency() != "wc2" {
		t.Errorf("Expected: %s, got %s", bp.WriteConsistency(), "wc2")
	}
}

func TestClientConcatURLPath(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.String(), "/influxdbproxy/ping") || strings.Contains(r.URL.String(), "/ping/ping") {
			t.Errorf("unexpected error.  expected %v contains in %v", "/influxdbproxy/ping", r.URL)
		}
		var data Response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNoContent)
		_ = json.NewEncoder(w).Encode(data)
	}))
	defer ts.Close()

	url, _ := url.Parse(ts.URL)
	url.Path = path.Join(url.Path, "influxdbproxy")

	fmt.Println("TestClientConcatURLPath: concat with path 'influxdbproxy' result ", url.String())

	c, _ := NewHTTPClient(HTTPConfig{Addr: url.String()})
	defer c.Close()

	_, _, err := c.Ping(0)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}

	_, _, err = c.Ping(0)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
}

func TestClientProxy(t *testing.T) {
	pinged := false
	ts := httptest.NewServer(http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
		if got, want := req.URL.String(), "http://example.com:8086/ping"; got != want {
			t.Errorf("invalid url in request: got=%s want=%s", got, want)
		}
		resp.WriteHeader(http.StatusNoContent)
		pinged = true
	}))
	defer ts.Close()

	proxyURL, _ := url.Parse(ts.URL)
	c, _ := NewHTTPClient(HTTPConfig{
		Addr:  "http://example.com:8086",
		Proxy: http.ProxyURL(proxyURL),
	})
	if _, _, err := c.Ping(0); err != nil {
		t.Fatalf("could not ping server: %s", err)
	}

	if !pinged {
		t.Fatalf("no http request was received")
	}
}

func TestClient_QueryAsChunk(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data Response
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Influxdb-Version", "1.3.1")
		w.WriteHeader(http.StatusOK)
		enc := json.NewEncoder(w)
		_ = enc.Encode(data)
		_ = enc.Encode(data)
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, err := NewHTTPClient(config)
	if err != nil {
		t.Fatalf("unexpected error.  expected %v, actual %v", nil, err)
	}

	query := Query{Chunked: true}
	resp, err := c.QueryAsChunk(query)
	defer resp.Close()
	if err != nil {
		t.Fatalf("unexpected error.  expected %v, actual %v", nil, err)
	}
}

func TestClient_ReadStatementId(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data := Response{
			Results: []Result{{
				StatementId: 1,
				Series:      nil,
				Messages:    nil,
				Err:         "",
			}},
			Err: "",
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Influxdb-Version", "1.3.1")
		w.WriteHeader(http.StatusOK)
		enc := json.NewEncoder(w)
		_ = enc.Encode(data)
		_ = enc.Encode(data)
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, err := NewHTTPClient(config)
	if err != nil {
		t.Fatalf("unexpected error.  expected %v, actual %v", nil, err)
	}

	query := Query{Chunked: true}
	resp, err := c.QueryAsChunk(query)
	defer resp.Close()
	if err != nil {
		t.Fatalf("unexpected error.  expected %v, actual %v", nil, err)
	}

	r, err := resp.NextResponse()

	if err != nil {
		t.Fatalf("expected success, got %s", err)
	}

	if r.Results[0].StatementId != 1 {
		t.Fatalf("expected statement_id = 1, got %d", r.Results[0].StatementId)
	}
}

func TestGetSM(t *testing.T) {

	tests := []struct {
		name        string
		queryString string
		expected    string
	}{
		{
			name:        "empty tag caused by having query results but no tags",
			queryString: "SELECT water_level FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z'",
			expected:    "{(h2o_feet.empty)}",
		},
		{
			name:        "empty tag caused by no query results",
			queryString: "SELECT water_level FROM h2o_feet WHERE time >= '2024-08-18T00:00:00Z' AND time <= '2024-08-18T00:30:00Z'",
			expected:    "{empty}",
		},
		{
			name:        "one tag with two tables",
			queryString: "SELECT water_level FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY location",
			expected:    "{(h2o_feet.location=coyote_creek)(h2o_feet.location=santa_monica)}",
		},
		{
			name:        "two tags with six tables",
			queryString: "SELECT index FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location",
			expected:    "{(h2o_quality.location=coyote_creek,h2o_quality.randtag=1)(h2o_quality.location=coyote_creek,h2o_quality.randtag=2)(h2o_quality.location=coyote_creek,h2o_quality.randtag=3)(h2o_quality.location=santa_monica,h2o_quality.randtag=1)(h2o_quality.location=santa_monica,h2o_quality.randtag=2)(h2o_quality.location=santa_monica,h2o_quality.randtag=3)}",
		},
		{
			name:        "only time interval without tags",
			queryString: "SELECT COUNT(water_level) FROM h2o_feet WHERE location='coyote_creek' AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY time(12m)",
			expected:    "{(h2o_feet.location=coyote_creek)}",
		},
		{
			name:        "one specific tag with time interval",
			queryString: "SELECT COUNT(water_level) FROM h2o_feet WHERE location='coyote_creek' AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY time(12m),location",
			expected:    "{(h2o_feet.location=coyote_creek)}",
		},
		{
			name:        "one tag with time interval",
			queryString: "SELECT COUNT(water_level) FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY time(12m),location",
			expected:    "{(h2o_feet.location=coyote_creek)(h2o_feet.location=santa_monica)}",
		},
		{
			name:        "two tags with time interval",
			queryString: "SELECT COUNT(index) FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY location,time(12m),randtag",
			expected:    "{(h2o_quality.location=coyote_creek,h2o_quality.randtag=1)(h2o_quality.location=coyote_creek,h2o_quality.randtag=2)(h2o_quality.location=coyote_creek,h2o_quality.randtag=3)(h2o_quality.location=santa_monica,h2o_quality.randtag=1)(h2o_quality.location=santa_monica,h2o_quality.randtag=2)(h2o_quality.location=santa_monica,h2o_quality.randtag=3)}",
		},
		{
			name:        "one tag with one predicate",
			queryString: "SELECT index FROM h2o_quality WHERE randtag='2' AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY location",
			expected:    "{(h2o_quality.location=coyote_creek,h2o_quality.randtag=2)(h2o_quality.location=santa_monica,h2o_quality.randtag=2)}",
		},
		{
			name:        "one tag with one predicate, without GROUP BY",
			queryString: "SELECT index FROM h2o_quality WHERE randtag='2' AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z'",
			expected:    "{(h2o_quality.randtag=2)}",
		},
		{
			name:        "one tag with two predicates",
			queryString: "SELECT index,randtag,location FROM h2o_quality WHERE randtag='2' AND location='santa_monica' AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z'",
			expected:    "{(h2o_quality.location=santa_monica,h2o_quality.randtag=2)}",
		},
		{
			name:        "one tag with two predicates",
			queryString: "SELECT index,randtag,location FROM h2o_quality WHERE randtag='2' AND location='santa_monica' AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z'GROUP BY randtag",
			expected:    "{(h2o_quality.location=santa_monica,h2o_quality.randtag=2)}",
		},
		{
			name:        "one tag with two predicates",
			queryString: "SELECT index,randtag,location FROM h2o_quality WHERE randtag='2' AND location='santa_monica' AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z'GROUP BY randtag,location",
			expected:    "{(h2o_quality.location=santa_monica,h2o_quality.randtag=2)}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := NewQuery(tt.queryString, MyDB, "")
			response, err := c.Query(q)

			if err != nil {
				log.Println(err)
			}

			_, tagPredicates := GetSP(tt.queryString, response, TagKV)
			SM := GetSM(response, tagPredicates)

			if strings.Compare(SM, tt.expected) != 0 {
				t.Errorf("\nSM=%s\nexpected:%s", SM, tt.expected)
			}

		})
	}

}

func TestGetSeperateSM(t *testing.T) {

	tests := []struct {
		name        string
		queryString string
		expected    []string
	}{
		{
			name:        "empty Result",
			queryString: "SELECT index FROM h2o_quality WHERE time >= '2029-08-18T00:00:00Z' AND time <= '2029-08-18T00:30:00Z' GROUP BY randtag,location",
			expected:    []string{"{empty}"},
		},
		{
			name:        "empty tag",
			queryString: "SELECT index FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z'",
			expected:    []string{"{(h2o_quality.empty)}"},
		},
		{
			name:        "one table one tag",
			queryString: "SELECT COUNT(water_level) FROM h2o_feet WHERE location='coyote_creek' AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY time(12m),location",
			expected: []string{
				"{(h2o_feet.location=coyote_creek)}",
			},
		},
		{
			name:        "six tables two tags",
			queryString: "SELECT COUNT(index) FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY location,time(12m),randtag",
			expected: []string{
				"{(h2o_quality.location=coyote_creek,h2o_quality.randtag=1)}",
				"{(h2o_quality.location=coyote_creek,h2o_quality.randtag=2)}",
				"{(h2o_quality.location=coyote_creek,h2o_quality.randtag=3)}",
				"{(h2o_quality.location=santa_monica,h2o_quality.randtag=1)}",
				"{(h2o_quality.location=santa_monica,h2o_quality.randtag=2)}",
				"{(h2o_quality.location=santa_monica,h2o_quality.randtag=3)}",
			},
		},
		{
			name:        "one tag with one predicate",
			queryString: "SELECT index FROM h2o_quality WHERE randtag='2' AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY location",
			expected: []string{
				"{(h2o_quality.location=coyote_creek,h2o_quality.randtag=2)}",
				"{(h2o_quality.location=santa_monica,h2o_quality.randtag=2)}",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := NewQuery(tt.queryString, MyDB, "")
			resp, _ := c.Query(q)
			_, tagPredicates := GetSP(tt.queryString, resp, TagKV)

			sepSM := GetSeperateSM(resp, tagPredicates)

			for i, s := range sepSM {
				if strings.Compare(s, tt.expected[i]) != 0 {
					t.Errorf("seperate SM:%s", s)
					t.Errorf("expected:%s", tt.expected[i])
				}
			}
		})
	}

}

func TestGetAggregation(t *testing.T) {
	tests := []struct {
		name        string
		queryString string
		expected    string
	}{
		{
			name:        "error",
			queryString: "SELECT ",
			expected:    "error",
		},
		{
			name:        "empty",
			queryString: "SELECT     index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z'",
			expected:    "empty",
		},
		{
			name:        "count",
			queryString: "SELECT   COUNT(water_level)      FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY time(12m)",
			expected:    "count",
		},
		{
			name:        "max",
			queryString: "SELECT  MAX(water_level)   FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY time(12m)",
			expected:    "max",
		},
		{
			name:        "mean",
			queryString: "SELECT MEAN(water_level) FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY time(12m)",
			expected:    "mean",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			aggregation := GetAggregation(tt.queryString)
			if strings.Compare(aggregation, tt.expected) != 0 {
				t.Errorf("aggregation:%s", aggregation)
				t.Errorf("expected:%s", tt.expected)
			}
		})
	}

}

func TestGetSFSG(t *testing.T) {
	tests := []struct {
		name        string
		queryString string
		expected    []string
	}{
		{
			name:        "one field without aggr",
			queryString: "SELECT water_level FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z'",
			expected:    []string{"water_level", "empty"},
		},
		{
			name:        "two fields without aggr",
			queryString: "SELECT water_level,location FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z'",
			expected:    []string{"water_level,location", "empty"},
		},
		{
			name:        "three fields without aggr",
			queryString: "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z'",
			expected:    []string{"index,location,randtag", "empty"},
		},
		{
			name:        "one field with aggr count",
			queryString: "SELECT COUNT(water_level) FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY time(12m)",
			expected:    []string{"water_level", "count"},
		},
		{
			name:        "one field with aggr max",
			queryString: "SELECT MAX(water_level) FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY time(12m)",
			expected:    []string{"water_level", "max"},
		},
		{
			name:        "one field with aggr mean",
			queryString: "SELECT MEAN(water_level) FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY time(12m)",
			expected:    []string{"water_level", "mean"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SF, SG := GetSFSG(tt.queryString)
			if !reflect.DeepEqual(SF, tt.expected[0]) {
				t.Errorf("Fields:\t%s\nexpected:%s", SF, tt.expected[0])
			}
			if !reflect.DeepEqual(SG, tt.expected[1]) {
				t.Errorf("Aggr:\t%s\nexpected:%s", SG, tt.expected[1])
			}

		})
	}

}

func TestGetSFSGWithDataType(t *testing.T) {

	tests := []struct {
		name        string
		queryString string
		expected    []string
	}{
		{
			name:        "one field without aggr",
			queryString: "SELECT water_level FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z'",
			expected:    []string{"water_level[float64]", "empty"},
		},
		{
			name:        "two fields without aggr",
			queryString: "SELECT water_level,location FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z'",
			expected:    []string{"water_level[float64],location[string]", "empty"},
		},
		{
			name:        "three fields without aggr",
			queryString: "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z'",
			expected:    []string{"index[int64],location[string],randtag[string]", "empty"},
		},
		{
			name:        "three fields without aggr",
			queryString: "SELECT location,index,randtag,index FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z'",
			expected:    []string{"location[string],index[int64],randtag[string],index_1[int64]", "empty"},
		},
		{
			name:        "one field with aggr count",
			queryString: "SELECT COUNT(water_level) FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY time(12m)",
			expected:    []string{"water_level[int64]", "count"},
		},
		{
			name:        "one field with aggr max",
			queryString: "SELECT MAX(water_level) FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY time(12m)",
			expected:    []string{"water_level[float64]", "max"},
		},
		{
			name:        "one field with aggr mean",
			queryString: "SELECT MEAN(water_level) FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY time(12m)",
			expected:    []string{"water_level[float64]", "mean"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := NewQuery(tt.queryString, MyDB, "s")
			resp, err := c.Query(q)
			if err != nil {
				t.Fatalf(err.Error())
			}

			sf, aggr := GetSFSGWithDataType(tt.queryString, resp)
			if sf != tt.expected[0] {
				t.Errorf("fields:%s", sf)
				t.Errorf("expected:%s", tt.expected[0])
			}
			if aggr != tt.expected[1] {
				t.Errorf("aggregation:%s", aggr)
				t.Errorf("expected:%s", tt.expected[1])
			}

		})
	}

}

func TestGetInterval(t *testing.T) {
	tests := []struct {
		name        string
		queryString string
		expected    string
	}{

		{
			name:        "without GROUP BY",
			queryString: "SELECT water_level FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z'",
			expected:    "empty",
		},
		{
			name:        "without time()",
			queryString: "SELECT MAX(water_level) FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY location",
			expected:    "empty",
		},
		{
			name:        "only time()",
			queryString: "SELECT MAX(water_level) FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY time(12m)",
			expected:    "12m",
		},
		{
			name:        "only time()",
			queryString: "SELECT MAX(water_level) FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY time(12h)",
			expected:    "12h",
		},
		{
			name:        "only time()",
			queryString: "SELECT MAX(water_level) FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY time(12s)",
			expected:    "12s",
		},
		{
			name:        "only time()",
			queryString: "SELECT MAX(water_level) FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY time(12ns)",
			expected:    "12ns",
		},
		{
			name:        "with time() and one tag",
			queryString: "SELECT MAX(water_level) FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY location,time(12m)",
			expected:    "12m",
		},
		{
			name:        "with time() and two tags",
			queryString: "SELECT MAX(water_level) FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY location,time(12m),randtag",
			expected:    "12m",
		},
		{
			name:        "different time()",
			queryString: "SELECT MAX(water_level) FROM h2o_feet WHERE location='coyote_creek' AND time >= '2015-09-18T16:00:00Z' AND time <= '2015-09-18T16:42:00Z' GROUP BY time(12h)",
			expected:    "12h",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			interval := GetInterval(tt.queryString)
			if !reflect.DeepEqual(interval, tt.expected) {
				t.Errorf("interval:\t%s\nexpected:\t%s", interval, tt.expected)
			}
		})
	}
}

func TestGetBinaryExpr(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		expected   string
	}{
		{
			name:       "binary expr",
			expression: "location='coyote_creek'",
			expected:   "location = 'coyote_creek'",
		},
		{
			name:       "binary expr",
			expression: "location='coyote creek'",
			expected:   "location = 'coyote creek'",
		},
		{
			name:       "multiple binary exprs",
			expression: "location='coyote_creek' AND randtag='2' AND index>=50",
			expected:   "location = 'coyote_creek' AND randtag = '2' AND index >= 50",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			binaryExpr := getBinaryExpr(tt.expression)
			if !reflect.DeepEqual(binaryExpr.String(), tt.expected) {
				t.Errorf("binary expression:\t%s\nexpected:\t%s", binaryExpr, tt.expected)
			}
		})
	}
}

func TestPreOrderTraverseBinaryExpr(t *testing.T) {
	tests := []struct {
		name             string
		binaryExprString string
		expected         [][]string
	}{
		{
			name:             "binary expr",
			binaryExprString: "location='coyote_creek'",
			expected:         [][]string{{"location", "location='coyote_creek'", "string"}},
		},
		{
			name:             "multiple binary expr",
			binaryExprString: "location='coyote_creek' AND randtag='2' AND index>=50",
			expected:         [][]string{{"location", "location='coyote_creek'", "string"}, {"randtag", "randtag='2'", "string"}, {"index", "index>=50", "int64"}},
		},
		{
			name:             "complex situation",
			binaryExprString: "location <> 'santa_monica' AND (water_level < -0.59 OR water_level > 9.95)",
			expected:         [][]string{{"location", "location!='santa_monica'", "string"}, {"water_level", "water_level<-0.590", "float64"}, {"water_level", "water_level>9.950", "float64"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conds := make([]string, 0)
			datatype := make([]string, 0)
			tag := make([]string, 0)
			binaryExpr := getBinaryExpr(tt.binaryExprString)
			tags, predicates, datatypes := preOrderTraverseBinaryExpr(binaryExpr, &tag, &conds, &datatype)
			for i, d := range *tags {
				if d != tt.expected[i][0] {
					t.Errorf("tag:\t%s\nexpected:\t%s", d, tt.expected[i][0])
				}
			}
			for i, p := range *predicates {
				if p != tt.expected[i][1] {
					t.Errorf("predicate:\t%s\nexpected:\t%s", p, tt.expected[i][1])
				}
			}
			for i, d := range *datatypes {
				if d != tt.expected[i][2] {
					t.Errorf("datatype:\t%s\nexpected:\t%s", d, tt.expected[i][2])
				}
			}
		})
	}
}

func TestGetSP(t *testing.T) {
	tests := []struct {
		name         string
		queryString  string
		expected     string
		expectedTags []string
	}{
		{
			name:         "three conditions and time range with GROUP BY",
			queryString:  "SELECT index FROM h2o_quality WHERE randtag='2' AND index>=50 AND location='santa_monica' AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY location",
			expected:     "{(index>=50[int64])}",
			expectedTags: []string{"location=santa_monica", "randtag=2"},
		},
		{
			name:         "three conditions and time range with GROUP BY",
			queryString:  "SELECT index FROM h2o_quality WHERE location='coyote_creek' AND randtag='2' AND index>=50 AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY location",
			expected:     "{(index>=50[int64])}",
			expectedTags: []string{"location=coyote_creek", "randtag=2"},
		},
		{
			name:         "three conditions(OR)",
			queryString:  "SELECT water_level FROM h2o_feet WHERE location != 'santa_monica' AND (water_level < -0.59 OR water_level > 9.95)",
			expected:     "{(water_level<-0.590[float64])(water_level>9.950[float64])}",
			expectedTags: []string{"location!=santa_monica"},
		},
		{
			name:         "three conditions and time range",
			queryString:  "SELECT water_level FROM h2o_feet WHERE location <> 'santa_monica' AND (water_level > -0.59 AND water_level < 9.95) AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY location",
			expected:     "{(water_level>-0.590[float64])(water_level<9.950[float64])}",
			expectedTags: []string{"location!=santa_monica"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := NewQuery(tt.queryString, MyDB, "s")
			resp, _ := c.Query(q)
			SP, tags := GetSP(tt.queryString, resp, TagKV)
			//fmt.Println(SP)
			if strings.Compare(SP, tt.expected) != 0 {
				t.Errorf("SP:\t%s\nexpected:\t%s", SP, tt.expected)
			}
			for i := range tags {
				if strings.Compare(tags[i], tt.expectedTags[i]) != 0 {
					t.Errorf("tag:\t%s\nexpected tag:\t%s", tags[i], tt.expectedTags[i])
				}
			}
		})
	}

}

func TestGetSPST(t *testing.T) {
	tests := []struct {
		name        string
		queryString string
		expected    string
	}{
		{
			name:        "without WHERE clause",
			queryString: "SELECT index FROM h2o_quality",
			expected:    "{empty}#{empty,empty}",
		},
		{
			name:        "only one predicate without time range",
			queryString: "SELECT index FROM h2o_quality WHERE location='coyote_creek'",
			expected:    "{(location='coyote_creek'[string])}#{empty,empty}",
		},
		{
			name:        "only time range(GE,LE)",
			queryString: "SELECT index FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z'",
			expected:    "{empty}#{1566086400000000000,1566088200000000000}",
		},
		{
			name:        "only time range(EQ)",
			queryString: "SELECT index FROM h2o_quality WHERE time = '2019-08-18T00:00:00Z'",
			expected:    "{empty}#{1566086400000000000,1566086400000000000}",
		},
		//{		// now()是当前时间，能正常用
		//	name:        "only time range(NOW)",
		//	queryString: "SELECT index FROM h2o_quality WHERE time <= now()",
		//	expected:    "{empty}#{empty,1704249836263677600}",
		//},
		{
			name:        "only time range(GT,LT)",
			queryString: "SELECT index FROM h2o_quality WHERE time > '2019-08-18T00:00:00Z' AND time < '2019-08-18T00:30:00Z'",
			expected:    "{empty}#{1566086400000000001,1566088199999999999}",
		},
		{
			name:        "only half time range(GE)",
			queryString: "SELECT index FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z'",
			expected:    "{empty}#{1566086400000000000,empty}",
		},
		{
			name:        "only half time range(LT)",
			queryString: "SELECT index FROM h2o_quality WHERE time < '2019-08-18T00:30:00Z'",
			expected:    "{empty}#{empty,1566088199999999999}",
		},
		{
			name:        "only half time range with arithmetic",
			queryString: "SELECT index FROM h2o_quality WHERE time <= '2019-08-18T00:30:00Z' - 10m",
			expected:    "{empty}#{empty,1566087600000000000}",
		},
		{
			name:        "only one predicate with half time range(GE)",
			queryString: "SELECT index FROM h2o_quality WHERE location='coyote_creek' AND  time >= '2019-08-18T00:00:00Z'",
			expected:    "{(location='coyote_creek'[string])}#{1566086400000000000,empty}",
		},
		{
			name:        "only one predicate with half time range(LE)",
			queryString: "SELECT index FROM h2o_quality WHERE location='coyote_creek' AND time <= '2019-08-18T00:30:00Z'",
			expected:    "{(location='coyote_creek'[string])}#{empty,1566088200000000000}",
		},
		{
			name:        "one condition and time range without GROUP BY",
			queryString: "SELECT index FROM h2o_quality WHERE location='coyote_creek' AND  time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z'",
			expected:    "{(location='coyote_creek'[string])}#{1566086400000000000,1566088200000000000}",
		},
		{
			name:        "one condition and time range with GROUP BY",
			queryString: "SELECT index FROM h2o_quality WHERE location='coyote_creek' AND  time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY location",
			expected:    "{(location='coyote_creek'[string])}#{1566086400000000000,1566088200000000000}",
		},
		{
			name:        "one condition with GROUP BY",
			queryString: "SELECT index FROM h2o_quality WHERE location='coyote_creek' GROUP BY location",
			expected:    "{(location='coyote_creek'[string])}#{empty,empty}",
		},
		{
			name:        "only half time range(LT) with GROUP BY",
			queryString: "SELECT index FROM h2o_quality WHERE time <= '2015-08-18T00:42:00Z' GROUP BY location",
			expected:    "{empty}#{empty,1439858520000000000}",
		},
		{
			name:        "two conditions and time range with GROUP BY",
			queryString: "SELECT index FROM h2o_quality WHERE location='coyote_creek' AND randtag='2' AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY location",
			expected:    "{(location='coyote_creek'[string])(randtag='2'[string])}#{1566086400000000000,1566088200000000000}",
		},
		{
			name:        "three conditions and time range with GROUP BY",
			queryString: "SELECT index FROM h2o_quality WHERE location='coyote_creek' AND randtag='2' AND index>=50 AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY location",
			expected:    "{(location='coyote_creek'[string])(randtag='2'[string])(index>=50[int64])}#{1566086400000000000,1566088200000000000}",
		},
		{
			name:        "three conditions(OR)",
			queryString: "SELECT water_level FROM h2o_feet WHERE location <> 'santa_monica' AND (water_level < -0.59 OR water_level > 9.95)",
			expected:    "{(location!='santa_monica'[string])(water_level<-0.590[float64])(water_level>9.950[float64])}#{empty,empty}",
		},
		{
			name:        "three conditions(OR) and time range",
			queryString: "SELECT water_level FROM h2o_feet WHERE location <> 'santa_monica' AND (water_level < -0.59 OR water_level > 9.95) AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY location",
			expected:    "{(location!='santa_monica'[string])(water_level<-0.590[float64])(water_level>9.950[float64])}#{1566086400000000000,1566088200000000000}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SPST := GetSPST(tt.queryString)
			if !reflect.DeepEqual(SPST, tt.expected) {
				t.Errorf("SPST:\t%s\nexpected:\t%s", SPST, tt.expected)
			}
		})
	}

}

func TestSemanticSegmentInstance(t *testing.T) {
	tests := []struct {
		name        string
		queryString string
		expected    string
	}{
		{
			name:        "1 1-1-T 直接查询",
			queryString: "select usage_guest from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:00:20Z' and hostname='host_0'",
			expected:    "{(cpu.hostname=host_0)}#{usage_guest[float64]}#{empty}#{empty,empty}",
		},
		{
			name:        "1 1-1-T MAX",
			queryString: "select max(usage_guest) from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:02:00Z' and hostname='host_0' group by time(1m)",
			expected:    "{(cpu.hostname=host_0)}#{usage_guest[float64]}#{empty}#{max,1m}",
		},
		{
			name:        "1 1-1-T MEAN",
			queryString: "select mean(usage_guest) from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:02:00Z' and hostname='host_0' group by time(1m)",
			expected:    "{(cpu.hostname=host_0)}#{usage_guest[float64]}#{empty}#{mean,1m}",
		},
		{
			name:        "2 3-1-T 直接查询",
			queryString: "select usage_guest,usage_nice,usage_guest_nice from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:00:20Z' and hostname='host_0'",
			expected:    "{(cpu.hostname=host_0)}#{usage_guest[float64],usage_nice[float64],usage_guest_nice[float64]}#{empty}#{empty,empty}",
		},
		{
			name:        "2 3-1-T MAX",
			queryString: "select max(usage_guest),max(usage_nice),max(usage_guest_nice) from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:02:00Z' and hostname='host_0' group by time(1m)",
			expected:    "{(cpu.hostname=host_0)}#{usage_guest[float64],usage_nice[float64],usage_guest_nice[float64]}#{empty}#{max,1m}",
		},
		{
			name:        "2 3-1-T MEAN",
			queryString: "select mean(usage_guest),mean(usage_nice),mean(usage_guest_nice) from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:02:00Z' and hostname='host_0' group by time(1m)",
			expected:    "{(cpu.hostname=host_0)}#{usage_guest[float64],usage_nice[float64],usage_guest_nice[float64]}#{empty}#{mean,1m}",
		},
		{
			name:        "3 3-1-T 直接查询",
			queryString: "select usage_system,usage_user,usage_guest from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:00:20Z' and hostname='host_0'",
			expected:    "{(cpu.hostname=host_0)}#{usage_system[float64],usage_user[float64],usage_guest[float64]}#{empty}#{empty,empty}",
		},
		{
			name:        "3 3-1-T MAX",
			queryString: "select max(usage_system),max(usage_user),max(usage_guest) from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:02:00Z' and hostname='host_0' group by time(1m)",
			expected:    "{(cpu.hostname=host_0)}#{usage_system[float64],usage_user[float64],usage_guest[float64]}#{empty}#{max,1m}",
		},
		{
			name:        "3 3-1-T MEAN",
			queryString: "select mean(usage_system),mean(usage_user),mean(usage_guest) from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:02:00Z' and hostname='host_0' group by time(1m)",
			expected:    "{(cpu.hostname=host_0)}#{usage_system[float64],usage_user[float64],usage_guest[float64]}#{empty}#{mean,1m}",
		},
		{
			name:        "4 5-1-T 直接查询",
			queryString: "select usage_system,usage_user,usage_guest,usage_nice,usage_guest_nice from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:00:20Z' and hostname='host_0'",
			expected:    "{(cpu.hostname=host_0)}#{usage_system[float64],usage_user[float64],usage_guest[float64],usage_nice[float64],usage_guest_nice[float64]}#{empty}#{empty,empty}",
		},
		{
			name:        "4 5-1-T MAX",
			queryString: "select max(usage_system),max(usage_user),max(usage_guest),max(usage_nice),max(usage_guest_nice) from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:02:00Z' and hostname='host_0' group by time(1m)",
			expected:    "{(cpu.hostname=host_0)}#{usage_system[float64],usage_user[float64],usage_guest[float64],usage_nice[float64],usage_guest_nice[float64]}#{empty}#{max,1m}",
		},
		{
			name:        "4 5-1-T MEAN",
			queryString: "select mean(usage_system),mean(usage_user),mean(usage_guest),mean(usage_nice),mean(usage_guest_nice) from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:02:00Z' and hostname='host_0' group by time(1m)",
			expected:    "{(cpu.hostname=host_0)}#{usage_system[float64],usage_user[float64],usage_guest[float64],usage_nice[float64],usage_guest_nice[float64]}#{empty}#{mean,1m}",
		},
		{
			name:        "5 10-1-T 直接查询",
			queryString: "select * from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:00:20Z' and hostname='host_0'",
			expected:    "{(cpu.hostname=host_0)}#{arch[string],datacenter[string],hostname[string],os[string],rack[string],region[string],service[string],service_environment[string],service_version[string],team[string],usage_guest[float64],usage_guest_nice[float64],usage_idle[float64],usage_iowait[float64],usage_irq[float64],usage_nice[float64],usage_softirq[float64],usage_steal[float64],usage_system[float64],usage_user[float64]}#{empty}#{empty,empty}",
		},
		{
			name:        "5 10-1-T MAX",
			queryString: "select max(*) from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:02:00Z' and hostname='host_0' group by time(1m)",
			expected:    "{(cpu.hostname=host_0)}#{usage_guest[float64],usage_guest_nice[float64],usage_idle[float64],usage_iowait[float64],usage_irq[float64],usage_nice[float64],usage_softirq[float64],usage_steal[float64],usage_system[float64],usage_user[float64]}#{empty}#{max,1m}",
		},
		{
			name:        "5 10-1-T MEAN",
			queryString: "select mean(*) from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:02:00Z' and hostname='host_0' group by time(1m)",
			expected:    "{(cpu.hostname=host_0)}#{usage_guest[float64],usage_guest_nice[float64],usage_idle[float64],usage_iowait[float64],usage_irq[float64],usage_nice[float64],usage_softirq[float64],usage_steal[float64],usage_system[float64],usage_user[float64]}#{empty}#{mean,1m}",
		},
		{
			name:        "6 1-1-T",
			queryString: "select usage_guest from test..cpu where time >= '2022-01-01T09:00:00Z' and time < '2022-01-01T10:00:00Z' and hostname='host_0' and usage_guest > 99.0",
			expected:    "{(cpu.hostname=host_0)}#{usage_guest[float64]}#{(usage_guest>99.000[float64])}#{empty,empty}",
		},
		{
			name:        "t7-1",
			queryString: "select usage_guest from test..cpu where time >= '2022-01-01T17:50:00Z' and time < '2022-01-01T18:00:00Z' and usage_guest > 99.0 group by hostname",
			expected:    "{(cpu.hostname=host_2)}#{usage_guest[float64]}#{(usage_guest>99.000[float64])}#{empty,empty}",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := NewQuery(tt.queryString, MyDB, "s")
			resp, err := c.Query(query)
			if err != nil {
				fmt.Println(err)
			}
			ss := SemanticSegment(tt.queryString, resp)
			//fmt.Println(ss)
			if strings.Compare(ss, tt.expected) != 0 {
				t.Errorf("samantic segment:\t%s", ss)
				t.Errorf("expected:\t%s", tt.expected)
			}
		})
	}
}

func TestSemanticSegmentDBTest(t *testing.T) {
	tests := []struct {
		name        string
		queryString string
		expected    string
	}{
		{
			name:        "1",
			queryString: "SELECT *::field FROM cpu limit 10",
			expected:    "{(cpu.empty)}#{usage_guest[float64],usage_guest_nice[float64],usage_idle[float64],usage_iowait[float64],usage_irq[float64],usage_nice[float64],usage_softirq[float64],usage_steal[float64],usage_system[float64],usage_user[float64]}#{empty}#{empty,empty}",
		},
		//{
		//	name:        "2",
		//	queryString: "SELECT *::field FROM cpu limit 10000000", // 中等规模数据集有一千二百五十万条数据	一万条数据 0.9s 	十万条数据 8.5s	一百万条数据 55.9s	一千万条数据 356.7s
		//	expected:    "{(cpu.empty)}#{usage_guest[float64],usage_guest_nice[float64],usage_idle[float64],usage_iowait[float64],usage_irq[float64],usage_nice[float64],usage_softirq[float64],usage_steal[float64],usage_system[float64],usage_user[float64]}#{empty}#{empty,empty}",
		//},
		{
			name:        "3",
			queryString: "SELECT usage_steal,usage_idle,usage_guest,usage_user FROM cpu GROUP BY service,team limit 10",
			expected:    "{(cpu.service=18,cpu.team=CHI)(cpu.service=2,cpu.team=LON)(cpu.service=4,cpu.team=NYC)(cpu.service=6,cpu.team=NYC)}#{usage_steal[float64],usage_idle[float64],usage_guest[float64],usage_user[float64]}#{empty}#{empty,empty}",
		},
		{
			name:        "4",
			queryString: "SELECT usage_steal,usage_idle,usage_guest,usage_user FROM cpu WHERE hostname = 'host_1' GROUP BY service,team limit 1000",
			expected:    "{(cpu.hostname=host_1,cpu.service=6,cpu.team=NYC)}#{usage_steal[float64],usage_idle[float64],usage_guest[float64],usage_user[float64]}#{empty}#{empty,empty}",
		},
		{
			name:        "5",
			queryString: "SELECT usage_steal,usage_guest,usage_user FROM cpu WHERE rack = '4' AND usage_user > 30.0 AND usage_steal < 90 GROUP BY service,team limit 10",
			expected:    "{(cpu.rack=4,cpu.service=18,cpu.team=CHI)}#{usage_steal[float64],usage_guest[float64],usage_user[float64]}#{(usage_user>30.000[float64])(usage_steal<90[int64])}#{empty,empty}",
		},
		{
			name:        "6",
			queryString: "SELECT MEAN(usage_steal) FROM cpu WHERE rack = '4' AND usage_user > 30.0 AND usage_steal < 90 GROUP BY service,team,time(1m) limit 10",
			expected:    "{(cpu.rack=4,cpu.service=18,cpu.team=CHI)}#{usage_steal[float64]}#{(usage_user>30.000[float64])(usage_steal<90[int64])}#{mean,1m}",
		},
		{
			name:        "7", // 11.8s 运行所需时间长是由于向数据库查询的时间长，不是客户端的问题，客户端生成语义段只用到了查询结果的表结构，不需要遍历表里的数据
			queryString: "SELECT MAX(usage_steal) FROM cpu WHERE usage_steal < 90.0 GROUP BY service,team,time(1m) limit 10",
			expected:    "{(cpu.service=18,cpu.team=CHI)(cpu.service=2,cpu.team=LON)(cpu.service=4,cpu.team=NYC)(cpu.service=6,cpu.team=NYC)}#{usage_steal[float64]}#{(usage_steal<90.000[float64])}#{max,1m}",
		},
		{
			name:        "8",
			queryString: "SELECT usage_steal,usage_nice,usage_iowait FROM cpu WHERE usage_steal < 90.0 AND time > '2022-01-01T00:00:00Z' AND time < '2022-05-01T00:00:00Z' GROUP BY service,team limit 10",
			expected:    "{(cpu.service=18,cpu.team=CHI)(cpu.service=2,cpu.team=LON)(cpu.service=4,cpu.team=NYC)(cpu.service=6,cpu.team=NYC)}#{usage_steal[float64],usage_nice[float64],usage_iowait[float64]}#{(usage_steal<90.000[float64])}#{empty,empty}",
		},
		{
			name:        "9",
			queryString: "SELECT usage_user,usage_nice,usage_irq,usage_system FROM cpu WHERE hostname = 'host_1' AND arch = 'x64' AND rack = '4' AND usage_user > 90.0 AND usage_irq > 100 AND time > '2022-01-01T00:00:00Z' AND time < '2022-05-01T00:00:00Z' GROUP BY service,region,team limit 10",
			expected:    "{empty response}",
		},
		{
			name:        "10",
			queryString: "SELECT usage_user,usage_nice,usage_irq,usage_system FROM cpu WHERE hostname = 'host_1' AND arch = 'x64' AND usage_user > 90.0 AND usage_irq > 10 AND service_version = '0' AND time > '2022-01-01T00:00:00Z' AND time < '2022-05-01T00:00:00Z' GROUP BY service,region,team limit 10",
			expected:    "{(cpu.arch=x64,cpu.hostname=host_1,cpu.region=us-west-2,cpu.service=6,cpu.service_version=0,cpu.team=NYC)}#{usage_user[float64],usage_nice[float64],usage_irq[float64],usage_system[float64]}#{(usage_user>90.000[float64])(usage_irq>10[int64])}#{empty,empty}",
		},
		{
			name:        "11", // 0.9s
			queryString: "SELECT COUNT(usage_user) FROM cpu WHERE hostname = 'host_1' AND arch = 'x64' AND usage_user > 90.0 AND usage_irq > 10.0 AND service_version = '0' AND time > '2022-01-01T00:00:00Z' AND time < '2022-05-01T00:00:00Z' GROUP BY service,region,team,time(3h) limit 10",
			expected:    "{(cpu.arch=x64,cpu.hostname=host_1,cpu.region=us-west-2,cpu.service=6,cpu.service_version=0,cpu.team=NYC)}#{usage_user[int64]}#{(usage_user>90.000[float64])(usage_irq>10.000[float64])}#{count,3h}",
		},
		{
			name:        "12", // 0.9s
			queryString: "SELECT COUNT(usage_user) FROM cpu WHERE hostname = 'host_1' AND arch = 'x64' AND usage_user > 90.0 AND usage_irq > 10.0 AND service_version = '0' AND time > '2022-01-01T00:00:00Z' AND time < '2022-05-01T00:00:00Z' GROUP BY service,region,team,time(3h)",
			expected:    "{(cpu.arch=x64,cpu.hostname=host_1,cpu.region=us-west-2,cpu.service=6,cpu.service_version=0,cpu.team=NYC)}#{usage_user[int64]}#{(usage_user>90.000[float64])(usage_irq>10.000[float64])}#{count,3h}",
		},
		{
			name:        "13",
			queryString: "SELECT MIN(usage_irq) FROM cpu WHERE hostname = 'host_1' AND usage_user > 90.0 AND usage_irq > 10.0 AND time > '2022-01-01T00:00:00Z' AND time < '2022-05-01T00:00:00Z' GROUP BY arch,service,region,team,time(3h) limit 10",
			expected:    "{(cpu.arch=x64,cpu.hostname=host_1,cpu.region=us-west-2,cpu.service=6,cpu.team=NYC)}#{usage_irq[float64]}#{(usage_user>90.000[float64])(usage_irq>10.000[float64])}#{min,3h}",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := NewQuery(tt.queryString, MyDB, "s")
			resp, err := c.Query(query)
			if err != nil {
				fmt.Println(err)
			}
			ss := SemanticSegment(tt.queryString, resp)
			//fmt.Println(ss)
			if strings.Compare(ss, tt.expected) != 0 {
				t.Errorf("samantic segment:\t%s", ss)
				t.Errorf("expected:\t%s", tt.expected)
			}
		})
	}
}

func TestSemanticSegment(t *testing.T) {
	tests := []struct {
		name        string
		queryString string
		expected    string
	}{
		{
			name:        "without WHERE",
			queryString: "SELECT index FROM h2o_quality",
			expected:    "{(h2o_quality.empty)}#{index[int64]}#{empty}#{empty,empty}",
		},
		{
			name:        "SF SP",
			queryString: "SELECT index FROM h2o_quality WHERE location='coyote_creek'",
			expected:    "{(h2o_quality.location=coyote_creek)}#{index[int64]}#{empty}#{empty,empty}",
		},
		{
			name:        "SF SP",
			queryString: "SELECT index FROM h2o_quality WHERE location='coyote_creek' GROUP BY randtag",
			expected:    "{(h2o_quality.location=coyote_creek,h2o_quality.randtag=1)(h2o_quality.location=coyote_creek,h2o_quality.randtag=2)(h2o_quality.location=coyote_creek,h2o_quality.randtag=3)}#{index[int64]}#{empty}#{empty,empty}",
		},
		{
			name:        "SF SP",
			queryString: "SELECT index FROM h2o_quality WHERE location='coyote_creek' GROUP BY randtag,location",
			expected:    "{(h2o_quality.location=coyote_creek,h2o_quality.randtag=1)(h2o_quality.location=coyote_creek,h2o_quality.randtag=2)(h2o_quality.location=coyote_creek,h2o_quality.randtag=3)}#{index[int64]}#{empty}#{empty,empty}",
		},
		{
			name:        "SF SP",
			queryString: "SELECT index FROM h2o_quality WHERE randtag='1' AND location='coyote_creek' AND index>50 GROUP BY randtag",
			expected:    "{(h2o_quality.location=coyote_creek,h2o_quality.randtag=1)}#{index[int64]}#{(index>50[int64])}#{empty,empty}",
		},
		{
			name:        "SM SF SP ST",
			queryString: "SELECT index FROM h2o_quality WHERE location='coyote_creek' AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag",
			expected:    "{(h2o_quality.location=coyote_creek,h2o_quality.randtag=1)(h2o_quality.location=coyote_creek,h2o_quality.randtag=2)(h2o_quality.location=coyote_creek,h2o_quality.randtag=3)}#{index[int64]}#{empty}#{empty,empty}",
		},
		{
			name:        "SM SF SP ST SG",
			queryString: "SELECT MAX(water_level) FROM h2o_feet WHERE location='coyote_creek' AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY location,time(12m)",
			expected:    "{(h2o_feet.location=coyote_creek)}#{water_level[float64]}#{empty}#{max,12m}",
		},
		{
			name:        "three fields without aggr",
			queryString: "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z'",
			expected:    "{(h2o_quality.empty)}#{index[int64],location[string],randtag[string]}#{empty}#{empty,empty}",
		},
		{
			name:        "SM three fields without aggr",
			queryString: "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag",
			expected:    "{(h2o_quality.randtag=1)(h2o_quality.randtag=2)(h2o_quality.randtag=3)}#{index[int64],location[string],randtag[string]}#{empty}#{empty,empty}",
		},
		{
			name:        "SM SP three fields without aggr",
			queryString: "SELECT index,location,randtag FROM h2o_quality WHERE location='coyote_creek' AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location",
			expected:    "{(h2o_quality.location=coyote_creek,h2o_quality.randtag=1)(h2o_quality.location=coyote_creek,h2o_quality.randtag=2)(h2o_quality.location=coyote_creek,h2o_quality.randtag=3)}#{index[int64],location[string],randtag[string]}#{empty}#{empty,empty}",
		},
		{
			name:        "SM SP three fields three predicates",
			queryString: "SELECT index,location,randtag FROM h2o_quality WHERE location='coyote_creek' AND randtag='2' AND index>50 AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location",
			expected:    "{(h2o_quality.location=coyote_creek,h2o_quality.randtag=2)}#{index[int64],location[string],randtag[string]}#{(index>50[int64])}#{empty,empty}",
		},
		{
			name:        "SP SG aggregation and three predicates",
			queryString: "SELECT COUNT(index) FROM h2o_quality WHERE location='coyote_creek' AND randtag='2' AND index>50 AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location,time(10s)",
			expected:    "{(h2o_quality.location=coyote_creek,h2o_quality.randtag=2)}#{index[int64]}#{(index>50[int64])}#{count,10s}",
		},
		{
			name:        "SP SG aggregation and three predicates",
			queryString: "SELECT COUNT(index) FROM h2o_quality WHERE location='coyote_creek' AND randtag='2' AND index>50 AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY time(10s)",
			expected:    "{(h2o_quality.location=coyote_creek,h2o_quality.randtag=2)}#{index[int64]}#{(index>50[int64])}#{count,10s}",
		},
		{
			name:        "three predicates(OR)",
			queryString: "SELECT water_level FROM h2o_feet WHERE location <> 'santa_monica' AND (water_level < -0.59 OR water_level > 9.95) AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-30T00:30:00Z' GROUP BY location",
			expected:    "{(h2o_feet.location=coyote_creek)}#{water_level[float64]}#{(water_level<-0.590[float64])(water_level>9.950[float64])}#{empty,empty}",
		},
		{
			name:        "three predicates(OR)",
			queryString: "SELECT water_level FROM h2o_feet WHERE location <> 'santa_monica' AND (water_level < -0.59 OR water_level > 9.95) AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-30T00:30:00Z'",
			expected:    "{(h2o_feet.location!=santa_monica)}#{water_level[float64]}#{(water_level<-0.590[float64])(water_level>9.950[float64])}#{empty,empty}",
		},
		{
			name:        "time() and two tags",
			queryString: "SELECT MAX(index) FROM h2o_quality WHERE randtag<>'1' AND index>=50 AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-20T00:30:00Z' GROUP BY location,time(12m),randtag",
			expected:    "{(h2o_quality.location=coyote_creek,h2o_quality.randtag=2)(h2o_quality.location=coyote_creek,h2o_quality.randtag=3)(h2o_quality.location=santa_monica,h2o_quality.randtag=2)(h2o_quality.location=santa_monica,h2o_quality.randtag=3)}#{index[int64]}#{(index>=50[int64])}#{max,12m}",
		},
		{
			name:        "time() and two tags",
			queryString: "SELECT MAX(index) FROM h2o_quality WHERE randtag<>'1' AND index>=50 AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-20T00:30:00Z' GROUP BY location,time(12m)",
			expected:    "{(h2o_quality.location=coyote_creek,h2o_quality.randtag!=1)(h2o_quality.location=santa_monica,h2o_quality.randtag!=1)}#{index[int64]}#{(index>=50[int64])}#{max,12m}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := NewQuery(tt.queryString, MyDB, "")
			response, err := c.Query(q)
			if err != nil {
				log.Println(err)
			}
			ss := SemanticSegment(tt.queryString, response)
			if !reflect.DeepEqual(ss, tt.expected) {
				t.Errorf("ss:\t%s\nexpected:\t%s", ss, tt.expected)
			}

		})
	}
}

func TestSeperateSemanticSegment(t *testing.T) {

	tests := []struct {
		name        string
		queryString string
		expected    []string
	}{
		{
			name:        "empty tag",
			queryString: "SELECT index FROM h2o_quality WHERE location='coyote_creek' AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z'",
			expected: []string{
				"{(h2o_quality.location=coyote_creek)}#{index[int64]}#{empty}#{empty,empty}",
			},
		},
		{
			name:        "four tables two tags",
			queryString: "SELECT MAX(index) FROM h2o_quality WHERE randtag<>'1' AND index>=50 AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-20T00:30:00Z' GROUP BY location,time(12m),randtag",
			expected: []string{
				"{(h2o_quality.location=coyote_creek,h2o_quality.randtag=2)}#{index[int64]}#{(index>=50[int64])}#{max,12m}",
				"{(h2o_quality.location=coyote_creek,h2o_quality.randtag=3)}#{index[int64]}#{(index>=50[int64])}#{max,12m}",
				"{(h2o_quality.location=santa_monica,h2o_quality.randtag=2)}#{index[int64]}#{(index>=50[int64])}#{max,12m}",
				"{(h2o_quality.location=santa_monica,h2o_quality.randtag=3)}#{index[int64]}#{(index>=50[int64])}#{max,12m}",
			},
		},
		{
			name:        "three table one tag",
			queryString: "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag",
			expected: []string{
				"{(h2o_quality.randtag=1)}#{index[int64],location[string],randtag[string]}#{empty}#{empty,empty}",
				"{(h2o_quality.randtag=2)}#{index[int64],location[string],randtag[string]}#{empty}#{empty,empty}",
				"{(h2o_quality.randtag=3)}#{index[int64],location[string],randtag[string]}#{empty}#{empty,empty}",
			},
		},
		{
			name:        "",
			queryString: "SELECT index FROM h2o_quality WHERE randtag='2' AND index<60 AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY location",
			expected: []string{
				"{(h2o_quality.location=santa_monica,h2o_quality.randtag=2)}#{index[int64]}#{(index<60[int64])}#{empty,empty}",
			},
		},
		{
			name:        "",
			queryString: "SELECT index FROM h2o_quality WHERE randtag='2' AND index>40 AND index<60 AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-09-30T00:30:00Z' GROUP BY location",
			expected: []string{
				"{(h2o_quality.location=coyote_creek,h2o_quality.randtag=2)}#{index[int64]}#{(index>40[int64])(index<60[int64])}#{empty,empty}",
				"{(h2o_quality.location=santa_monica,h2o_quality.randtag=2)}#{index[int64]}#{(index>40[int64])(index<60[int64])}#{empty,empty}",
			},
		},
		{
			name:        "",
			queryString: "SELECT index FROM h2o_quality WHERE randtag='2' AND index>40 AND index<60 AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-09-30T00:30:00Z' GROUP BY location,randtag",
			expected: []string{
				"{(h2o_quality.location=coyote_creek,h2o_quality.randtag=2)}#{index[int64]}#{(index>40[int64])(index<60[int64])}#{empty,empty}",
				"{(h2o_quality.location=santa_monica,h2o_quality.randtag=2)}#{index[int64]}#{(index>40[int64])(index<60[int64])}#{empty,empty}",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := NewQuery(tt.queryString, MyDB, "")
			resp, _ := c.Query(q)

			sepSemanticSegment := SeperateSemanticSegment(tt.queryString, resp)

			for i, s := range sepSemanticSegment {
				if strings.Compare(s, tt.expected[i]) != 0 {
					t.Errorf("semantic segment:%s", s)
					t.Errorf("expected:%s", tt.expected[i])
				}
			}

		})
	}

}

func TestTSCacheParameter(t *testing.T) {
	tests := []struct {
		name        string
		queryString string
		expected    string
	}{
		{
			name:        "1",
			queryString: "SELECT index,randtag FROM h2o_quality GROUP BY location limit 5",
			expected: "" +
				"(h2o_quality.location=coyote_creek).time[int64] 40" +
				"1566000000000000000 1566000360000000000 1566000720000000000 1566001080000000000 1566001440000000000 " +
				"(h2o_quality.location=coyote_creek).index[int64] 40" +
				"41 11 38 50 35 " +
				"(h2o_quality.location=coyote_creek).randtag[string] 125" +
				"1 3 1 1 3 " +
				"(h2o_quality.location=santa_monica).time[int64] 40" +
				"1566000000000000000 1566000360000000000 1566000720000000000 1566001080000000000 1566001440000000000 " +
				"(h2o_quality.location=santa_monica).index[int64] 40" +
				"99 56 65 57 8 " +
				"(h2o_quality.location=santa_monica).randtag[string] 125" +
				"2 2 3 3 3 ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := NewQuery(tt.queryString, MyDB, "s")
			resp, err := c.Query(query)
			if err != nil {
				fmt.Println(err)
			}
			datatypes := GetDataTypeArrayFromResponse(resp)

			tag_field, field_len, field_value := TSCacheParameter(resp)

			//table_num := len(tag_field)
			column_num := len(tag_field[0])
			arg1 := make([]string, 0)
			for i := range tag_field { // 子表数量
				for j := range tag_field[i] { // 列数量
					//fmt.Printf("%s %d\n", tag_field[i][j], field_len[i][j])
					str := fmt.Sprintf("%s %d", tag_field[i][j], field_len[i][j])
					arg1 = append(arg1, str)
				}
			}

			for i := range arg1 {
				fmt.Println(arg1[i])
				for _, value := range field_value[i] {
					switch datatypes[i%column_num] { // 每列数据的数据类型
					case "string":
						tmp_string := ByteArrayToString(value)
						fmt.Printf("%s ", tmp_string)

						break
					case "int64":
						tmp_int, _ := ByteArrayToInt64(value)
						fmt.Printf("%d ", tmp_int)

						break

					case "float64":
						tmp_float, _ := ByteArrayToFloat64(value)
						fmt.Printf("%f ", tmp_float)

						break
					default:
						tmp_bool, _ := ByteArrayToBool(value)
						fmt.Printf("%v ", tmp_bool)
					}
				}
				fmt.Println()
			}

		})
	}
}

func TestTSCacheByteToValue(t *testing.T) {
	tests := []struct {
		name        string
		queryString string
		exected     string
	}{
		{
			name:        "1",
			queryString: "SELECT index,randtag FROM h2o_quality GROUP BY location limit 5",
			exected: "SCHEMA time index randtag location=coyote_creek " +
				"1566000000000000000 41 1 " +
				"1566000360000000000 11 3 " +
				"1566000720000000000 38 1 " +
				"1566001080000000000 50 1 " +
				"1566001440000000000 35 3 " +
				"SCHEMA time index randtag location=santa_monica " +
				"1566000000000000000 99 2 " +
				"1566000360000000000 56 2 " +
				"1566000720000000000 65 3 " +
				"1566001080000000000 57 3 " +
				"1566001440000000000 8 3 " +
				"end",
		},
		{
			name:        "2",
			queryString: "SELECT index,randtag FROM h2o_quality GROUP BY location,randtag limit 5",
			exected: "SCHEMA time index randtag location=coyote_creek randtag=1 " +
				"1566000000000000000 41 1 " +
				"1566000720000000000 38 1 " +
				"1566001080000000000 50 1 " +
				"1566002160000000000 24 1 " +
				"1566004320000000000 94 1 " +
				"SCHEMA time index randtag location=coyote_creek randtag=2 " +
				"1566001800000000000 49 2 " +
				"1566003240000000000 32 2 " +
				"1566003960000000000 50 2 " +
				"1566005040000000000 90 2 " +
				"1566007200000000000 44 2 " +
				"SCHEMA time index randtag location=coyote_creek randtag=3 " +
				"1566000360000000000 11 3 " +
				"1566001440000000000 35 3 " +
				"1566002520000000000 92 3 " +
				"1566002880000000000 56 3 " +
				"1566003600000000000 68 3 " +
				"SCHEMA time index randtag location=santa_monica randtag=1 " +
				"1566004680000000000 9 1 " +
				"1566006120000000000 49 1 " +
				"1566006840000000000 4 1 " +
				"1566007200000000000 39 1 " +
				"1566007560000000000 46 1 " +
				"SCHEMA time index randtag location=santa_monica randtag=2 " +
				"1566000000000000000 99 2 " +
				"1566000360000000000 56 2 " +
				"1566001800000000000 36 2 " +
				"1566002160000000000 92 2 " +
				"1566005400000000000 69 2 " +
				"SCHEMA time index randtag location=santa_monica randtag=3 " +
				"1566000720000000000 65 3 " +
				"1566001080000000000 57 3 " +
				"1566001440000000000 8 3 " +
				"1566002520000000000 87 3 " +
				"1566002880000000000 81 3 " +
				"end",
		},
		{
			name:        "3",
			queryString: "SELECT index,randtag,location FROM h2o_quality GROUP BY location,randtag",
			exected:     "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := NewQuery(tt.queryString, MyDB, "s")
			resp, err := c.Query(query)
			if err != nil {
				fmt.Println(err)
			}

			byteArray := TSCacheValueToByte(resp)
			respConverted := TSCacheByteToValue(byteArray)

			fmt.Printf("resp:\n%s\n", resp.ToString())
			//fmt.Println(byteArray)
			fmt.Printf("resp converted:\n%s\n", respConverted.ToString())

			fmt.Println(*resp)
			fmt.Println(*respConverted)

			fmt.Println("\nbytes are equal:")
			fmt.Println(bytes.Equal(ResponseToByteArray(resp, tt.queryString), ResponseToByteArray(respConverted, tt.queryString)))

		})
	}
}

func TestSplitResponseByTime(t *testing.T) {
	queryString := `select usage_system,usage_user,usage_guest,usage_nice,usage_guest_nice from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T03:20:00Z' and hostname='host_0'`
	qs := NewQuery(queryString, MyDB, "s")
	resp, _ := c.Query(qs)

	//fmt.Println(resp.ToString())

	splitResp, starts, ends := SplitResponseValuesByTime(queryString, resp)

	for i, sr := range splitResp {
		fmt.Println(len(sr[0]))
		fmt.Println(starts[i])
		fmt.Println(ends[i])
	}

}

func TestSetToFatCache(t *testing.T) {
	queryString := `select usage_guest from test..cpu where time >= '2022-01-02T09:40:00Z' and time < '2022-01-02T10:10:00Z' and hostname='host_0'`

	SetToFatache(queryString)
	st, et := GetQueryTimeRange(queryString)
	ss := GetSemanticSegment(queryString)
	ss = fmt.Sprintf("%s[%d,%d]", ss, st, et)
	log.Printf("\tget:%s\n", ss)
	items, err := fatcacheConn.Get(ss)
	if err != nil {
		log.Fatal(err)
	} else {
		log.Println("GET.")
		log.Println("\tget byte length:", len(items.Value))
	}

}

func TestIntegratedClient(t *testing.T) {
	queryToBeGet := `select usage_system,usage_user,usage_guest,usage_nice,usage_guest_nice from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:00:20Z' and hostname='host_0'`

	queryToBeSet := `select usage_system,usage_user,usage_guest,usage_nice,usage_guest_nice from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:00:10Z' and hostname='host_0'`

	qm := NewQuery(queryToBeSet, MyDB, "s")
	respCache, _ := c.Query(qm)
	startTime, endTime := GetResponseTimeRange(respCache)
	numOfTab := GetNumOfTable(respCache)

	semanticSegment := GetSemanticSegment(queryToBeSet)
	respCacheByte := ResponseToByteArray(respCache, queryToBeSet)
	fmt.Println(respCache.ToString())
	//fmt.Println(respCacheByte)

	/* 向 stscache set 0-10 的数据 */
	err = stscacheConn.Set(&stscache.Item{Key: semanticSegment, Value: respCacheByte, Time_start: startTime, Time_end: endTime, NumOfTables: numOfTab})
	if err != nil {
		log.Fatalf("Error setting value: %v", err)
	} else {
		log.Printf("STORED.")
	}

	/* 向 cache get 0-20 的数据，缺失的数据向数据库查询并存入 cache */
	IntegratedClient(queryToBeGet)

	/* 向 cache get 0-20 的数据 */
	qgst, qget := GetQueryTimeRange(queryToBeGet)
	values, _, err := stscacheConn.Get(semanticSegment, qgst, qget)
	if errors.Is(err, stscache.ErrCacheMiss) {
		log.Printf("Key not found in cache")
	} else if err != nil {
		log.Fatalf("Error getting value: %v", err)
	} else {
		log.Printf("GET.")
	}

	/* 把查询结果从字节流转换成 Response 结构 */
	convertedResponse := ByteArrayToResponse(values)
	crst, cret := GetResponseTimeRange(convertedResponse)
	fmt.Println(convertedResponse.ToString())
	fmt.Println(crst)
	fmt.Println(cret)

}

// done 根据查询时向 client.Query() 传入的时间的参数不同，会返回string和int64的不同类型的时间戳
/*
	暂时把cache传回的字节数组只处理成int64
*/

// done Get()有长度限制，在哪里改
/*
	和字节数无关，只能读取最多 64 条数据（怎么会和数据条数相关 ?）

	说明：Get()按照 '\n' 读取每一行数据，字节码为 10 ，如果数据中有 int64 类型的 10，会错误地把数字当作换行符结束读取，导致一行的结尾不是 CRLF，报错
		可以去掉判断结尾是 CRLF 的异常处理，让Get()即使提前结束当前行的读取，也能继续读取下一行
		但是应该怎么判断结束一行的读取 ?(答案在下面)
*/

// done Get()设置合适的结束读取一行的条件，让他能完整的读取一行数据，不会混淆换行符和数字10
/*
	根本无所谓，无论Get()怎样从cache读取数据，无论当前读到的一行是否完整，都是把读到的所有字节直接存到字节数组中，不需要在Get()做任何处理
	Get()读取完cache返回的所有数据之后，把 未经处理 的整个字节数组交由客户端转换成结果类型，转换过程按照数据类型读取固定的字节数并转换，不受Get()的读取方式的影响
	Get()按任意方式从cache读取数据，最终的字节数组都是相同的，对结果没有影响
*/

// done cache 的所有操作的 key 都有长度限制
/*
	key 长度限制在 fc_memcache.c 中
		#define MEMCACHE_MAX_KEY_LENGTH 250  --->  change to 450
*/

// done  设计新的 TestMerge ，多用几组不同条件的查询和不同时间范围（表的数量尽量不同，顺带测试表结构合并）
/*
	当前的测试用例时间范围太大，导致表的数量基本相同，需要缩小时间范围，增加不同结果中表数量的差距
	对于时间精度和时间范围的测试，受当前数据集影响，基本只能使用 h 精度合并，即使选取的时间精度是 m ，查询到的数据也不能合并
	多测几组查询，用不同数量的tag、时间范围尽量小、让查询结果的表尽量不同

	对 Merge 相关的函数都重新进行了一次测试，结果符合预期，应该没问题
*/

// done  检查和 Merge 相关的所有函数以及 Test 的边界条件（查询结果为空应该没问题， tag map 为空会怎样）
/*
	查询结果为空会在对表排序的步骤直接跳过，并根据排好序的结果进行合并，空结果不会有影响
	tag map 为空就是下面所说的，只有一张表，tag字符串为空，数据直接合并成一张新的表；数据没有按照时间重新排序（不需要）
*/

// done  表合并之后数据是否需要再处理成按照时间顺序排列
/*
	不同查询的时间范围没有重叠，一张表里的数据本身就是按照时间顺序排列的；
	合并时两张表先按照起止时间排先后顺序，然后直接把后面的表的数据拼接到前面的表的数组上，这样就可以确保原本表内数据顺序不变，两张表的数据整体按照时间递增排列；
	所以先排序再合并的两张表的数据本身时间顺序就是对的，不需要再处理
*/

// done  确定在没使用 GROUP BY 时合并的过程、tag map的处理（好像没问题，但是为什么）
/*
	此时结果中只有一个表，tag map为空，合并会直接把两个结果的数据拼接成一个表，分别是两张表的数据按照时间顺序排列
	tag 字符串为空，存入数组时也是有长度的，不会出现数组越界，用空串进行比较等操作没有问题，会直接把唯一的表合并
*/

// done  测试时Merge()传入不合适的时间精度时会报错，是什么引起的，怎么解决
/*
	时间精度不合适导致没能合并，此时结果中的表数量多于 expected 中的表数量，用tests的索引遍历输出expected的表时出现数组越界问题，不是Merge()函数本身的问题
*/
