package client

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
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

const (
	MyDB     = "NOAA_water_database"
	username = "root"
	password = "12345678"
)

func TestGetSM(t *testing.T) {
	c, err := NewHTTPClient(HTTPConfig{
		Addr: "http://localhost:8086",
		//Username: username,
		//Password: password,
	})
	if err != nil {
		log.Fatal(err)
	}

	tests := []struct {
		name        string
		queryString string
		expected    []string
	}{
		{
			name:        "empty tag caused by having query results but no tags",
			queryString: "SELECT water_level FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z'",
			expected:    []string{"empty tag"},
		},
		{
			name:        "empty tag caused by no query results",
			queryString: "SELECT water_level FROM h2o_feet WHERE time >= '2024-08-18T00:00:00Z' AND time <= '2024-08-18T00:30:00Z'",
			expected:    []string{"empty tag"},
		},
		{
			name:        "one tag with two tables",
			queryString: "SELECT water_level FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY location",
			expected:    []string{"(h2o_feet.location=coyote_creek)", "(h2o_feet.location=santa_monica)"},
		},
		{
			name:        "two tags with six tables",
			queryString: "SELECT index FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location",
			expected:    []string{"(h2o_quality.location=coyote_creek)(h2o_quality.randtag=1)", "(h2o_quality.location=coyote_creek)(h2o_quality.randtag=2)", "(h2o_quality.location=coyote_creek)(h2o_quality.randtag=3)", "(h2o_quality.location=santa_monica)(h2o_quality.randtag=1)", "(h2o_quality.location=santa_monica)(h2o_quality.randtag=2)", "(h2o_quality.location=santa_monica)(h2o_quality.randtag=3)"},
		},
		{
			name:        "only time interval without tags",
			queryString: "SELECT COUNT(water_level) FROM h2o_feet WHERE location='coyote_creek' AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY time(12m)",
			expected:    []string{"empty tag"},
		},
		{
			name:        "one specific tag with time interval",
			queryString: "SELECT COUNT(water_level) FROM h2o_feet WHERE location='coyote_creek' AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY time(12m),location",
			expected:    []string{"(h2o_feet.location=coyote_creek)"},
		},
		{
			name:        "one tag with time interval",
			queryString: "SELECT COUNT(water_level) FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY time(12m),location",
			expected:    []string{"(h2o_feet.location=coyote_creek)", "(h2o_feet.location=santa_monica)"},
		},
		{
			name:        "two tags with time interval",
			queryString: "SELECT COUNT(index) FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY location,time(12m),randtag",
			expected:    []string{"(h2o_quality.location=coyote_creek)(h2o_quality.randtag=1)", "(h2o_quality.location=coyote_creek)(h2o_quality.randtag=2)", "(h2o_quality.location=coyote_creek)(h2o_quality.randtag=3)", "(h2o_quality.location=santa_monica)(h2o_quality.randtag=1)", "(h2o_quality.location=santa_monica)(h2o_quality.randtag=2)", "(h2o_quality.location=santa_monica)(h2o_quality.randtag=3)"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := NewQuery(tt.queryString, MyDB, "")
			response, err := c.Query(q)

			if err != nil {
				log.Println(err)
			}

			SM := GetSM(response)
			for i := range SM {
				if strings.Compare(SM[i], tt.expected[i]) != 0 {
					t.Errorf("GetSM:query\t%s\nSM=%s\nexpected:%s", tt.queryString, SM[i], tt.expected[i])
				}
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
			expected:    []string{"time,water_level", "empty"},
		},
		{
			name:        "two fields without aggr",
			queryString: "SELECT water_level,location FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z'",
			expected:    []string{"time,water_level,location", "empty"},
		},
		{
			name:        "three fields without aggr",
			queryString: "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z'",
			expected:    []string{"time,index,location,randtag", "empty"},
		},
		{
			name:        "one field with aggr count",
			queryString: "SELECT COUNT(water_level) FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY time(12m)",
			expected:    []string{"time,water_level", "count"},
		},
		{
			name:        "one field with aggr max",
			queryString: "SELECT MAX(water_level) FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY time(12m)",
			expected:    []string{"time,water_level", "max"},
		},
		{
			name:        "one field with aggr mean",
			queryString: "SELECT MEAN(water_level) FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY time(12m)",
			expected:    []string{"time,water_level", "mean"},
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
			binaryExpr := GetBinaryExpr(tt.expression)
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
			expected:         [][]string{{"location='coyote_creek'", "string"}},
		},
		{
			name:             "multiple binary expr",
			binaryExprString: "location='coyote_creek' AND randtag='2' AND index>=50",
			expected:         [][]string{{"location='coyote_creek'", "string"}, {"randtag='2'", "string"}, {"index>=50", "int"}},
		},
		{
			name:             "complex situation",
			binaryExprString: "location <> 'santa_monica' AND (water_level < -0.59 OR water_level > 9.95)",
			expected:         [][]string{{"location!='santa_monica'", "string"}, {"water_level<-0.590", "double"}, {"water_level>9.950", "double"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conds := make([]string, 0)
			datatype := make([]string, 0)
			binaryExpr := GetBinaryExpr(tt.binaryExprString)
			predicates, datatypes := PreOrderTraverseBinaryExpr(binaryExpr, &conds, &datatype)
			for i, p := range *predicates {
				if p != tt.expected[i][0] {
					t.Errorf("predicate:\t%s\nexpected:\t%s", p, tt.expected[i][0])
				}
			}
			for i, d := range *datatypes {
				if d != tt.expected[i][1] {
					t.Errorf("datatype:\t%s\nexpected:\t%s", d, tt.expected[i][1])
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
			expected:    "{(location='coyote_creek'[string])(randtag='2'[string])(index>=50[int])}#{1566086400000000000,1566088200000000000}",
		},
		{
			name:        "three conditions(OR)",
			queryString: "SELECT water_level FROM h2o_feet WHERE location <> 'santa_monica' AND (water_level < -0.59 OR water_level > 9.95)",
			expected:    "{(location!='santa_monica'[string])(water_level<-0.590[double])(water_level>9.950[double])}#{empty,empty}",
		},
		{
			name:        "three conditions(OR) and time range",
			queryString: "SELECT water_level FROM h2o_feet WHERE location <> 'santa_monica' AND (water_level < -0.59 OR water_level > 9.95) AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY location",
			expected:    "{(location!='santa_monica'[string])(water_level<-0.590[double])(water_level>9.950[double])}#{1566086400000000000,1566088200000000000}",
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

func TestSemanticSegment(t *testing.T) {
	tests := []struct {
		name        string
		queryString string
		expected    []string
	}{
		{
			name:        "without WHERE",
			queryString: "SELECT index FROM h2o_quality",
			expected:    []string{"{empty tag}#{time,index}#{empty}#{empty,empty}#{empty,empty}"},
		},
		{
			name:        "SF SP",
			queryString: "SELECT index FROM h2o_quality WHERE location='coyote_creek'",
			expected:    []string{"{empty tag}#{time,index}#{(location='coyote_creek'[string])}#{empty,empty}#{empty,empty}"},
		},
		{
			name:        "SF ST",
			queryString: "SELECT index FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z'",
			expected:    []string{"{empty tag}#{time,index}#{empty}#{1566086400000000000,1566088200000000000}#{empty,empty}"},
		},
		{
			name:        "SF and half ST",
			queryString: "SELECT index FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z'",
			expected:    []string{"{empty tag}#{time,index}#{empty}#{1566086400000000000,empty}#{empty,empty}"},
		},
		{
			name:        "SF SP and half ST",
			queryString: "SELECT index FROM h2o_quality WHERE location='coyote_creek' AND  time >= '2019-08-18T00:00:00Z'",
			expected:    []string{"{empty tag}#{time,index}#{(location='coyote_creek'[string])}#{1566086400000000000,empty}#{empty,empty}"},
		},
		{
			name:        "SF SP ST",
			queryString: "SELECT index FROM h2o_quality WHERE location='coyote_creek' AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z'",
			expected:    []string{"{empty tag}#{time,index}#{(location='coyote_creek'[string])}#{1566086400000000000,1566088200000000000}#{empty,empty}"},
		},
		{
			name:        "SM SF SP ST",
			queryString: "SELECT index FROM h2o_quality WHERE location='coyote_creek' AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag",
			expected: []string{
				"{(h2o_quality.randtag=1)}#{time,index}#{(location='coyote_creek'[string])}#{1566086400000000000,1566088200000000000}#{empty,empty}",
				"{(h2o_quality.randtag=2)}#{time,index}#{(location='coyote_creek'[string])}#{1566086400000000000,1566088200000000000}#{empty,empty}",
				"{(h2o_quality.randtag=3)}#{time,index}#{(location='coyote_creek'[string])}#{1566086400000000000,1566088200000000000}#{empty,empty}"},
		},
		{
			name:        "SM SF SP ST SG",
			queryString: "SELECT MAX(water_level) FROM h2o_feet WHERE location='coyote_creek' AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY location,time(12m)",
			expected:    []string{"{(h2o_feet.location=coyote_creek)}#{time,water_level}#{(location='coyote_creek'[string])}#{1566086400000000000,1566088200000000000}#{max,12m}"},
		},
		{
			name:        "three fields without aggr",
			queryString: "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z'",
			expected:    []string{"{empty tag}#{time,index,location,randtag}#{empty}#{1566086400000000000,1566088200000000000}#{empty,empty}"},
		},
		{
			name:        "SM three fields without aggr",
			queryString: "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag",
			expected: []string{
				"{(h2o_quality.randtag=1)}#{time,index,location,randtag}#{empty}#{1566086400000000000,1566088200000000000}#{empty,empty}",
				"{(h2o_quality.randtag=2)}#{time,index,location,randtag}#{empty}#{1566086400000000000,1566088200000000000}#{empty,empty}",
				"{(h2o_quality.randtag=3)}#{time,index,location,randtag}#{empty}#{1566086400000000000,1566088200000000000}#{empty,empty}"},
		},
		{
			name:        "SM SP three fields without aggr",
			queryString: "SELECT index,location,randtag FROM h2o_quality WHERE location='coyote_creek' AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location",
			expected: []string{
				"{(h2o_quality.location=coyote_creek)(h2o_quality.randtag=1)}#{time,index,location,randtag}#{(location='coyote_creek'[string])}#{1566086400000000000,1566088200000000000}#{empty,empty}",
				"{(h2o_quality.location=coyote_creek)(h2o_quality.randtag=2)}#{time,index,location,randtag}#{(location='coyote_creek'[string])}#{1566086400000000000,1566088200000000000}#{empty,empty}",
				"{(h2o_quality.location=coyote_creek)(h2o_quality.randtag=3)}#{time,index,location,randtag}#{(location='coyote_creek'[string])}#{1566086400000000000,1566088200000000000}#{empty,empty}"},
		},
		{
			name:        "SM SP three fields three predicates",
			queryString: "SELECT index,location,randtag FROM h2o_quality WHERE location='coyote_creek' AND randtag='2' AND index>50 AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location",
			expected:    []string{"{(h2o_quality.location=coyote_creek)(h2o_quality.randtag=2)}#{time,index,location,randtag}#{(location='coyote_creek'[string])(randtag='2'[string])(index>50[int])}#{1566086400000000000,1566088200000000000}#{empty,empty}"},
		},
		{
			name:        "SP SG aggregation and three predicates",
			queryString: "SELECT COUNT(index) FROM h2o_quality WHERE location='coyote_creek' AND randtag='2' AND index>50 AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location,time(10s)",
			expected:    []string{"{(h2o_quality.location=coyote_creek)(h2o_quality.randtag=2)}#{time,index}#{(location='coyote_creek'[string])(randtag='2'[string])(index>50[int])}#{1566086400000000000,1566088200000000000}#{count,10s}"},
		},
		{
			name:        "three predicates(OR)",
			queryString: "SELECT water_level FROM h2o_feet WHERE location <> 'santa_monica' AND (water_level < -0.59 OR water_level > 9.95) AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-30T00:30:00Z' GROUP BY location",
			expected:    []string{"{(h2o_feet.location=coyote_creek)}#{time,water_level}#{(location!='santa_monica'[string])(water_level<-0.590[double])(water_level>9.950[double])}#{1566086400000000000,1567125000000000000}#{empty,empty}"},
		},
		{
			name:        "time() and two tags",
			queryString: "SELECT MAX(index) FROM h2o_quality WHERE randtag<>'1' AND index>=50 AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-20T00:30:00Z' GROUP BY location,time(12m),randtag",
			expected: []string{
				"{(h2o_quality.location=coyote_creek)(h2o_quality.randtag=2)}#{time,index}#{(randtag!='1'[string])(index>=50[int])}#{1566086400000000000,1566261000000000000}#{max,12m}",
				"{(h2o_quality.location=coyote_creek)(h2o_quality.randtag=3)}#{time,index}#{(randtag!='1'[string])(index>=50[int])}#{1566086400000000000,1566261000000000000}#{max,12m}",
				"{(h2o_quality.location=santa_monica)(h2o_quality.randtag=2)}#{time,index}#{(randtag!='1'[string])(index>=50[int])}#{1566086400000000000,1566261000000000000}#{max,12m}",
				"{(h2o_quality.location=santa_monica)(h2o_quality.randtag=3)}#{time,index}#{(randtag!='1'[string])(index>=50[int])}#{1566086400000000000,1566261000000000000}#{max,12m}"},
		},
	}

	c, err := NewHTTPClient(HTTPConfig{
		Addr: "http://localhost:8086",
		//Username: username,
		//Password: password,
	})
	if err != nil {
		log.Fatal(err)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := NewQuery(tt.queryString, MyDB, "")
			response, err := c.Query(q)
			if err != nil {
				log.Println(err)
			}
			ss := SemanticSegment(tt.queryString, response)
			for i := range ss {
				if !reflect.DeepEqual(ss[i], tt.expected[i]) {
					t.Errorf("ss:\t%s\nexpected\t%s", ss[i], tt.expected[i])
				}
			}

		})
	}
}

func TestGetTagArr(t *testing.T) {
	tests := []struct {
		name        string
		queryString string
		expected    []string
	}{
		{
			name:        "one tag",
			queryString: "SELECT index FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag",
			expected:    []string{"randtag"},
		},
		{
			name:        "two tags",
			queryString: "SELECT index FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location",
			expected:    []string{"location", "randtag"},
		},
		{
			name:        "two tags in different sequence",
			queryString: "SELECT index FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY location,randtag",
			expected:    []string{"location", "randtag"},
		},
		{
			name:        "two tags with time interval",
			queryString: "SELECT COUNT(index) FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY location,time(12m),randtag",
			expected:    []string{"location", "randtag"},
		},
	}

	c, err := NewHTTPClient(HTTPConfig{
		Addr: "http://localhost:8086",
	})
	if err != nil {
		log.Fatal(err)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := NewQuery(tt.queryString, MyDB, "")
			response, err := c.Query(q)
			if err != nil {
				log.Println(err)
			}
			tags := GetTagNameArr(response)
			for i := range tags {
				if tags[i] != tt.expected[i] {
					t.Errorf("tag:\t%s\nexpected:\t%s", tags[i], tt.expected[i])
				}
			}
		})
	}
}

func TestGetResponseTimeRange(t *testing.T) {
	tests := []struct {
		name        string
		queryString string
		expected    []uint64
	}{
		{
			name:        "common situation",
			queryString: "SELECT index FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag",
			expected:    []uint64{1566086400000000000, 1566261000000000000},
		},
		{
			name:        "no results",
			queryString: "SELECT index FROM h2o_quality WHERE time >= '2029-08-18T00:00:00Z' AND time <= '2029-08-18T00:30:00Z' GROUP BY randtag",
			expected:    []uint64{math.MaxInt64, 0},
		},
	}

	c, err := NewHTTPClient(HTTPConfig{
		Addr: "http://localhost:8086",
	})
	if err != nil {
		log.Fatal(err)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := NewQuery(tt.queryString, MyDB, "")
			response, err := c.Query(q)
			if err != nil {
				log.Println(err)
			}
			st, et := GetResponseTimeRange(response)
			if st < tt.expected[0] {
				t.Errorf("start time:\t%d\nexpected:\t%d", st, tt.expected[0])
			}
			if et > tt.expected[1] {
				t.Errorf("end time:\t%d\nexpected:\t%d", et, tt.expected[1])
			}
		})
	}
}

func TestSortResponseWithTimeRange(t *testing.T) {
	c, err := NewHTTPClient(HTTPConfig{
		Addr: "http://localhost:8086",
	})
	if err != nil {
		log.Fatal(err)
	}

	queryString1 := "SELECT COUNT(water_level) FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY time(12m),location"
	q := NewQuery(queryString1, MyDB, "")
	response1, err := c.Query(q)
	st1, et1 := GetResponseTimeRange(response1)

	// 和 query1 相差一分钟	00:01:00Z
	queryString2 := "SELECT COUNT(water_level) FROM h2o_feet WHERE time >= '2019-08-18T00:31:00Z' AND time <= '2019-08-18T01:00:00Z' GROUP BY time(12m),location"
	q2 := NewQuery(queryString2, MyDB, "")
	response2, err := c.Query(q2)
	st2, et2 := GetResponseTimeRange(response2)

	// 和 query2 相差一小时	01:00:00Z
	queryString3 := "SELECT COUNT(water_level) FROM h2o_feet WHERE time >= '2019-08-18T02:00:00Z' AND time <= '2019-08-18T02:30:00Z' GROUP BY time(12m),location"
	q3 := NewQuery(queryString3, MyDB, "")
	response3, err := c.Query(q3)
	st3, et3 := GetResponseTimeRange(response3)

	tests := []struct {
		name     string
		rts      []RespWithTimeRange
		expected []RespWithTimeRange
	}{
		{
			name:     "fake time",
			rts:      []RespWithTimeRange{{nil, 1, 5}, {nil, 90, 100}, {nil, 30, 50}, {nil, 10, 20}, {nil, 6, 9}},
			expected: []RespWithTimeRange{{nil, 1, 5}, {nil, 6, 9}, {nil, 10, 20}, {nil, 30, 50}, {nil, 90, 100}},
		},
		{
			name:     "real Responses fake time",
			rts:      []RespWithTimeRange{{response2, 90, 100}, {response1, 30, 50}, {response3, 1, 5}},
			expected: []RespWithTimeRange{{response3, 1, 5}, {response1, 30, 50}, {response2, 90, 100}},
		},
		{
			name:     " 3 1 2 ",
			rts:      []RespWithTimeRange{{response3, st3, et3}, {response1, st1, et1}, {response2, st2, et2}},
			expected: []RespWithTimeRange{{response1, st1, et1}, {response2, st2, et2}, {response3, st3, et3}},
		},
		{
			name:     " 3 2 1 ",
			rts:      []RespWithTimeRange{{response3, st3, et3}, {response2, st2, et2}, {response1, st1, et1}},
			expected: []RespWithTimeRange{{response1, st1, et1}, {response2, st2, et2}, {response3, st3, et3}},
		},
		{
			name:     " 2 3 1 ",
			rts:      []RespWithTimeRange{{response2, st2, et2}, {response3, st3, et3}, {response1, st1, et1}},
			expected: []RespWithTimeRange{{response1, st1, et1}, {response2, st2, et2}, {response3, st3, et3}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sorted := SortResponseWithTimeRange(tt.rts)
			for i := range sorted {
				if sorted[i] != tt.expected[i] {
					t.Error("sorted:\t", sorted)
					t.Error("expected:\t", tt.expected)
					break
				}
			}
		})
	}
}

func TestSortResponseWithTimeRange2(t *testing.T) {
	c, err := NewHTTPClient(HTTPConfig{
		Addr: "http://localhost:8086",
	})
	if err != nil {
		log.Fatal(err)
	}

	queryString1 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:10:00Z' GROUP BY randtag,location"
	q1 := NewQuery(queryString1, MyDB, "")
	response1, err := c.Query(q1)
	st1, et1 := GetResponseTimeRange(response1)
	rwtr1 := RespWithTimeRange{response1, st1, et1}

	queryString2 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:15:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location"
	q2 := NewQuery(queryString2, MyDB, "")
	response2, err := c.Query(q2)
	st2, et2 := GetResponseTimeRange(response2)
	rwtr2 := RespWithTimeRange{response2, st2, et2}

	queryString3 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T01:31:00Z' AND time <= '2019-08-18T01:40:00Z' GROUP BY randtag,location"
	q3 := NewQuery(queryString3, MyDB, "")
	response3, err := c.Query(q3)
	st3, et3 := GetResponseTimeRange(response3)
	rwtr3 := RespWithTimeRange{response3, st3, et3}

	queryString4 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T03:31:00Z' AND time <= '2019-08-18T03:40:00Z' GROUP BY randtag,location"
	q4 := NewQuery(queryString4, MyDB, "")
	response4, err := c.Query(q4)
	st4, et4 := GetResponseTimeRange(response4)
	rwtr4 := RespWithTimeRange{response4, st4, et4}

	queryString5 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T03:40:00Z' AND time <= '2019-08-18T04:00:00Z' GROUP BY randtag,location"
	q5 := NewQuery(queryString5, MyDB, "")
	response5, err := c.Query(q5)
	st5, et5 := GetResponseTimeRange(response5)
	rwtr5 := RespWithTimeRange{response5, st5, et5}

	tests := []struct {
		name     string
		rts      []RespWithTimeRange
		expected []RespWithTimeRange
	}{
		{
			name:     " 1 2 3 4 5 ",
			rts:      []RespWithTimeRange{rwtr1, rwtr2, rwtr3, rwtr4, rwtr5},
			expected: []RespWithTimeRange{rwtr1, rwtr2, rwtr3, rwtr4, rwtr5},
		},
		{
			name:     " 3 1 2 5 4 ",
			rts:      []RespWithTimeRange{rwtr3, rwtr1, rwtr2, rwtr5, rwtr4},
			expected: []RespWithTimeRange{rwtr1, rwtr2, rwtr3, rwtr4, rwtr5},
		},
		{
			name:     " 5 4 3 2 1 ",
			rts:      []RespWithTimeRange{rwtr5, rwtr4, rwtr3, rwtr2, rwtr1},
			expected: []RespWithTimeRange{rwtr1, rwtr2, rwtr3, rwtr4, rwtr5},
		},
		{
			name:     " 4 1 5 3 2 ",
			rts:      []RespWithTimeRange{rwtr4, rwtr1, rwtr5, rwtr3, rwtr2},
			expected: []RespWithTimeRange{rwtr1, rwtr2, rwtr3, rwtr4, rwtr5},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sorted := SortResponseWithTimeRange(tt.rts)
			for i := range sorted {
				if sorted[i] != tt.expected[i] {
					t.Error("sorted:\t", sorted)
					t.Error("expected:\t", tt.expected)
					break
				}
			}
		})
	}
}

func TestSortResponses(t *testing.T) {
	c, err := NewHTTPClient(HTTPConfig{
		Addr: "http://localhost:8086",
	})
	if err != nil {
		log.Fatal(err)
	}

	queryString1 := "SELECT COUNT(water_level) FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY time(12m),location"
	q := NewQuery(queryString1, MyDB, "")
	response1, err := c.Query(q)

	// 和 query1 相差一分钟	00:01:00Z
	queryString2 := "SELECT COUNT(water_level) FROM h2o_feet WHERE time >= '2019-08-18T00:31:00Z' AND time <= '2019-08-18T01:00:00Z' GROUP BY time(12m),location"
	q2 := NewQuery(queryString2, MyDB, "")
	response2, err := c.Query(q2)

	// 和 query2 相差一小时	01:00:00Z
	queryString3 := "SELECT COUNT(water_level) FROM h2o_feet WHERE time >= '2019-08-18T02:00:00Z' AND time <= '2019-08-18T02:30:00Z' GROUP BY time(12m),location"
	q3 := NewQuery(queryString3, MyDB, "")
	response3, err := c.Query(q3)

	var responseNil *Response
	responseNil = nil

	query1 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag"
	nq1 := NewQuery(query1, MyDB, "")
	resp1, err := c.Query(nq1)

	// 1 min
	query2 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:31:00Z' AND time <= '2019-08-18T01:00:00Z' GROUP BY randtag"
	nq2 := NewQuery(query2, MyDB, "")
	resp2, err := c.Query(nq2)

	// 0.5 h
	query3 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T01:31:00Z' AND time <= '2019-08-18T02:00:00Z' GROUP BY randtag"
	nq3 := NewQuery(query3, MyDB, "")
	resp3, err := c.Query(nq3)

	// 1 h
	query4 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T03:00:00Z' AND time <= '2019-08-18T04:00:00Z' GROUP BY randtag"
	nq4 := NewQuery(query4, MyDB, "")
	resp4, err := c.Query(nq4)

	// 1 s
	query5 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T04:00:01Z' AND time <= '2019-08-18T04:30:00Z' GROUP BY randtag"
	nq5 := NewQuery(query5, MyDB, "")
	resp5, err := c.Query(nq5)

	tests := []struct {
		name     string
		resps    []*Response
		expected []*Response
	}{
		{
			name:     " 3 2 1 ",
			resps:    []*Response{response3, response2, response1},
			expected: []*Response{response1, response2, response3},
		},
		{
			name:     " 1 2 3 ",
			resps:    []*Response{response1, response2, response3},
			expected: []*Response{response1, response2, response3},
		},
		{
			name:     " 3 1 2 ",
			resps:    []*Response{response3, response1, response2},
			expected: []*Response{response1, response2, response3},
		},
		{
			name:     " 2 3 1 ",
			resps:    []*Response{response2, response3, response1},
			expected: []*Response{response1, response2, response3},
		},
		{
			name:     " 2 3 1 nil ",
			resps:    []*Response{response2, response3, response1, responseNil},
			expected: []*Response{response1, response2, response3},
		},
		{
			name:     " 3 nil 2 1 ",
			resps:    []*Response{response3, responseNil, response2, response1},
			expected: []*Response{response1, response2, response3},
		},
		/**/
		{
			name:     " 5 2 4 nil 1 3 ",
			resps:    []*Response{resp5, resp2, resp4, responseNil, resp1, resp3},
			expected: []*Response{resp1, resp2, resp3, resp4, resp5},
		},
		{
			name:     " 3 1 2 5 4 ",
			resps:    []*Response{resp3, resp1, resp2, resp5, resp4},
			expected: []*Response{resp1, resp2, resp3, resp4, resp5},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sorted := SortResponses(tt.resps)
			for i := range sorted {
				if sorted[i] != tt.expected[i] {
					t.Error("sorted:\t", sorted)
					t.Error("expected:\t", tt.expected)
					break
				}
			}
		})
	}
}

func TestSortResponses2(t *testing.T) {
	c, err := NewHTTPClient(HTTPConfig{
		Addr: "http://localhost:8086",
	})
	if err != nil {
		log.Fatal(err)
	}

	queryString1 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:10:00Z' GROUP BY randtag,location"
	q1 := NewQuery(queryString1, MyDB, "")
	resp1, err := c.Query(q1)

	queryString2 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:15:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location"
	q2 := NewQuery(queryString2, MyDB, "")
	resp2, err := c.Query(q2)

	queryString3 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T01:31:00Z' AND time <= '2019-08-18T01:40:00Z' GROUP BY randtag,location"
	q3 := NewQuery(queryString3, MyDB, "")
	resp3, err := c.Query(q3)

	queryString4 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T03:31:00Z' AND time <= '2019-08-18T03:40:00Z' GROUP BY randtag,location"
	q4 := NewQuery(queryString4, MyDB, "")
	resp4, err := c.Query(q4)

	queryString5 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T03:40:00Z' AND time <= '2019-08-18T04:00:00Z' GROUP BY randtag,location"
	q5 := NewQuery(queryString5, MyDB, "")
	resp5, err := c.Query(q5)

	var respNil *Response
	respNil = nil

	tests := []struct {
		name     string
		resps    []*Response
		expected []*Response
	}{
		{
			name:     " 5 nil 2 4 nil 1 3 ",
			resps:    []*Response{resp5, respNil, resp2, resp4, respNil, resp1, resp3},
			expected: []*Response{resp1, resp2, resp3, resp4, resp5},
		},
		{
			name:     " 1 2 3 4 5 ",
			resps:    []*Response{resp1, resp2, resp3, resp4, resp5},
			expected: []*Response{resp1, resp2, resp3, resp4, resp5},
		},
		{
			name:     " 5 4 3 2 1 ",
			resps:    []*Response{resp5, resp4, resp3, resp2, resp1},
			expected: []*Response{resp1, resp2, resp3, resp4, resp5},
		},
		{
			name:     " 3 5 1 4 2 ",
			resps:    []*Response{resp3, resp5, resp1, resp4, resp2},
			expected: []*Response{resp1, resp2, resp3, resp4, resp5},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sorted := SortResponses(tt.resps)
			for i := range sorted {
				if sorted[i] != tt.expected[i] {
					t.Error("sorted:\t", sorted)
					t.Error("expected:\t", tt.expected)
					break
				}
			}
		})
	}
}

func TestMergeResultTable(t *testing.T) {
	c, err := NewHTTPClient(HTTPConfig{
		Addr: "http://localhost:8086",
	})
	if err != nil {
		log.Fatal(err)
	}

	query1 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location"
	nq1 := NewQuery(query1, MyDB, "")
	resp1, err := c.Query(nq1)
	resp1.ToString()
	//SCHEMA time index location randtag location=coyote_creek randtag=1
	//2019-08-18T00:06:00Z 66 coyote_creek 1
	//2019-08-18T00:18:00Z 91 coyote_creek 1
	//2019-08-18T00:24:00Z 29 coyote_creek 1
	//SCHEMA time index location randtag location=coyote_creek randtag=2
	//2019-08-18T00:12:00Z 78 coyote_creek 2
	//SCHEMA time index location randtag location=coyote_creek randtag=3
	//2019-08-18T00:00:00Z 85 coyote_creek 3
	//2019-08-18T00:30:00Z 75 coyote_creek 3
	//SCHEMA time index location randtag location=santa_monica randtag=1
	//2019-08-18T00:06:00Z 67 santa_monica 1
	//2019-08-18T00:18:00Z 14 santa_monica 1
	//SCHEMA time index location randtag location=santa_monica randtag=2
	//2019-08-18T00:00:00Z 11 santa_monica 2
	//2019-08-18T00:12:00Z 91 santa_monica 2
	//2019-08-18T00:30:00Z 79 santa_monica 2
	//SCHEMA time index location randtag location=santa_monica randtag=3
	//2019-08-18T00:24:00Z 44 santa_monica 3
	//end

	// 1 min
	query2 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:31:00Z' AND time <= '2019-08-18T01:00:00Z' GROUP BY randtag,location"
	nq2 := NewQuery(query2, MyDB, "")
	resp2, err := c.Query(nq2)
	resp2.ToString()
	//SCHEMA time index location randtag location=coyote_creek randtag=1
	//2019-08-18T00:42:00Z 55 coyote_creek 1
	//SCHEMA time index location randtag location=coyote_creek randtag=3
	//2019-08-18T00:36:00Z 33 coyote_creek 3
	//2019-08-18T00:48:00Z 29 coyote_creek 3
	//2019-08-18T00:54:00Z 94 coyote_creek 3
	//2019-08-18T01:00:00Z 16 coyote_creek 3
	//SCHEMA time index location randtag location=santa_monica randtag=1
	//2019-08-18T00:36:00Z 25 santa_monica 1
	//2019-08-18T00:42:00Z 10 santa_monica 1
	//2019-08-18T00:48:00Z 7 santa_monica 1
	//SCHEMA time index location randtag location=santa_monica randtag=2
	//2019-08-18T01:00:00Z 83 santa_monica 2
	//SCHEMA time index location randtag location=santa_monica randtag=3
	//2019-08-18T00:54:00Z 27 santa_monica 3
	//end

	// 0.5 h
	query3 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T01:31:00Z' AND time <= '2019-08-18T02:00:00Z' GROUP BY randtag,location"
	nq3 := NewQuery(query3, MyDB, "")
	resp3, err := c.Query(nq3)
	fmt.Println(resp3)

	// 1 h
	query4 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T03:00:00Z' AND time <= '2019-08-18T04:00:00Z' GROUP BY randtag,location"
	nq4 := NewQuery(query4, MyDB, "")
	resp4, err := c.Query(nq4)
	fmt.Println(resp4)

	// 1 s
	query5 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T04:00:01Z' AND time <= '2019-08-18T04:30:00Z' GROUP BY randtag,location"
	nq5 := NewQuery(query5, MyDB, "")
	resp5, err := c.Query(nq5)
	fmt.Println(resp5)

	tests := []struct {
		name        string
		queryString []string
		expected    string
	}{
		{
			name: " 1 2 ",
			queryString: []string{
				"SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location",
				"SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:31:00Z' AND time <= '2019-08-18T01:00:00Z' GROUP BY randtag,location",
			},
			expected: "SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
				"2019-08-18T00:06:00Z 66 coyote_creek 1 \r\n" +
				"2019-08-18T00:18:00Z 91 coyote_creek 1 \r\n" +
				"2019-08-18T00:24:00Z 29 coyote_creek 1 \r\n" +
				"2019-08-18T00:42:00Z 55 coyote_creek 1 \r\n" +
				"SCHEMA time index location randtag location=coyote_creek randtag=2 \r\n" +
				"2019-08-18T00:12:00Z 78 coyote_creek 2 \r\n" +
				"SCHEMA time index location randtag location=coyote_creek randtag=3 \r\n" +
				"2019-08-18T00:00:00Z 85 coyote_creek 3 \r\n" +
				"2019-08-18T00:30:00Z 75 coyote_creek 3 \r\n" +
				"2019-08-18T00:36:00Z 33 coyote_creek 3 \r\n" +
				"2019-08-18T00:48:00Z 29 coyote_creek 3 \r\n" +
				"2019-08-18T00:54:00Z 94 coyote_creek 3 \r\n" +
				"2019-08-18T01:00:00Z 16 coyote_creek 3 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=1 \r\n" +
				"2019-08-18T00:06:00Z 67 santa_monica 1 \r\n" +
				"2019-08-18T00:18:00Z 14 santa_monica 1 \r\n" +
				"2019-08-18T00:36:00Z 25 santa_monica 1 \r\n" +
				"2019-08-18T00:42:00Z 10 santa_monica 1 \r\n" +
				"2019-08-18T00:48:00Z 7 santa_monica 1 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=2 \r\n" +
				"2019-08-18T00:00:00Z 11 santa_monica 2 \r\n" +
				"2019-08-18T00:12:00Z 91 santa_monica 2 \r\n" +
				"2019-08-18T00:30:00Z 79 santa_monica 2 \r\n" +
				"2019-08-18T01:00:00Z 83 santa_monica 2 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
				"2019-08-18T00:24:00Z 44 santa_monica 3 \r\n" +
				"2019-08-18T00:54:00Z 27 santa_monica 3 \r\n" +
				"end",
		},
		{
			name: " 2 1 ",
			queryString: []string{
				"SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:31:00Z' AND time <= '2019-08-18T01:00:00Z' GROUP BY randtag,location",
				"SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location",
			},
			expected: "SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
				"2019-08-18T00:42:00Z 55 coyote_creek 1 \r\n" +
				"2019-08-18T00:06:00Z 66 coyote_creek 1 \r\n" +
				"2019-08-18T00:18:00Z 91 coyote_creek 1 \r\n" +
				"2019-08-18T00:24:00Z 29 coyote_creek 1 \r\n" +
				"SCHEMA time index location randtag location=coyote_creek randtag=2 \r\n" +
				"2019-08-18T00:12:00Z 78 coyote_creek 2 \r\n" +
				"SCHEMA time index location randtag location=coyote_creek randtag=3 \r\n" +
				"2019-08-18T00:36:00Z 33 coyote_creek 3 \r\n" +
				"2019-08-18T00:48:00Z 29 coyote_creek 3 \r\n" +
				"2019-08-18T00:54:00Z 94 coyote_creek 3 \r\n" +
				"2019-08-18T01:00:00Z 16 coyote_creek 3 \r\n" +
				"2019-08-18T00:00:00Z 85 coyote_creek 3 \r\n" +
				"2019-08-18T00:30:00Z 75 coyote_creek 3 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=1 \r\n" +
				"2019-08-18T00:36:00Z 25 santa_monica 1 \r\n" +
				"2019-08-18T00:42:00Z 10 santa_monica 1 \r\n" +
				"2019-08-18T00:48:00Z 7 santa_monica 1 \r\n" +
				"2019-08-18T00:06:00Z 67 santa_monica 1 \r\n" +
				"2019-08-18T00:18:00Z 14 santa_monica 1 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=2 \r\n" +
				"2019-08-18T01:00:00Z 83 santa_monica 2 \r\n" +
				"2019-08-18T00:00:00Z 11 santa_monica 2 \r\n" +
				"2019-08-18T00:12:00Z 91 santa_monica 2 \r\n" +
				"2019-08-18T00:30:00Z 79 santa_monica 2 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
				"2019-08-18T00:54:00Z 27 santa_monica 3 \r\n" +
				"2019-08-18T00:24:00Z 44 santa_monica 3 \r\n" +
				"end",
		},
		{
			name: " 2 1 without GROUP BY ",
			queryString: []string{
				"SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:31:00Z' AND time <= '2019-08-18T01:00:00Z'",
				//SCHEMA time index location randtag
				//2019-08-18T00:36:00Z 33 coyote_creek 3
				//2019-08-18T00:36:00Z 25 santa_monica 1
				//2019-08-18T00:42:00Z 55 coyote_creek 1
				//2019-08-18T00:42:00Z 10 santa_monica 1
				//2019-08-18T00:48:00Z 29 coyote_creek 3
				//2019-08-18T00:48:00Z 7 santa_monica 1
				//2019-08-18T00:54:00Z 94 coyote_creek 3
				//2019-08-18T00:54:00Z 27 santa_monica 3
				//2019-08-18T01:00:00Z 16 coyote_creek 3
				//2019-08-18T01:00:00Z 83 santa_monica 2
				//end
				"SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z'",
				//SCHEMA time index location randtag
				//2019-08-18T00:00:00Z 11 santa_monica 2
				//2019-08-18T00:00:00Z 85 coyote_creek 3
				//2019-08-18T00:06:00Z 66 coyote_creek 1
				//2019-08-18T00:06:00Z 67 santa_monica 1
				//2019-08-18T00:12:00Z 78 coyote_creek 2
				//2019-08-18T00:12:00Z 91 santa_monica 2
				//2019-08-18T00:18:00Z 91 coyote_creek 1
				//2019-08-18T00:18:00Z 14 santa_monica 1
				//2019-08-18T00:24:00Z 29 coyote_creek 1
				//2019-08-18T00:24:00Z 44 santa_monica 3
				//2019-08-18T00:30:00Z 79 santa_monica 2
				//2019-08-18T00:30:00Z 75 coyote_creek 3
				//end
			},
			expected: "SCHEMA time index location randtag \r\n" +
				"2019-08-18T00:36:00Z 33 coyote_creek 3 \r\n" +
				"2019-08-18T00:36:00Z 25 santa_monica 1 \r\n" +
				"2019-08-18T00:42:00Z 55 coyote_creek 1 \r\n" +
				"2019-08-18T00:42:00Z 10 santa_monica 1 \r\n" +
				"2019-08-18T00:48:00Z 29 coyote_creek 3 \r\n" +
				"2019-08-18T00:48:00Z 7 santa_monica 1 \r\n" +
				"2019-08-18T00:54:00Z 94 coyote_creek 3 \r\n" +
				"2019-08-18T00:54:00Z 27 santa_monica 3 \r\n" +
				"2019-08-18T01:00:00Z 16 coyote_creek 3 \r\n" +
				"2019-08-18T01:00:00Z 83 santa_monica 2 \r\n" +
				"2019-08-18T00:00:00Z 11 santa_monica 2 \r\n" +
				"2019-08-18T00:00:00Z 85 coyote_creek 3 \r\n" +
				"2019-08-18T00:06:00Z 66 coyote_creek 1 \r\n" +
				"2019-08-18T00:06:00Z 67 santa_monica 1 \r\n" +
				"2019-08-18T00:12:00Z 78 coyote_creek 2 \r\n" +
				"2019-08-18T00:12:00Z 91 santa_monica 2 \r\n" +
				"2019-08-18T00:18:00Z 91 coyote_creek 1 \r\n" +
				"2019-08-18T00:18:00Z 14 santa_monica 1 \r\n" +
				"2019-08-18T00:24:00Z 29 coyote_creek 1 \r\n" +
				"2019-08-18T00:24:00Z 44 santa_monica 3 \r\n" +
				"2019-08-18T00:30:00Z 79 santa_monica 2 \r\n" +
				"2019-08-18T00:30:00Z 75 coyote_creek 3 \r\n" +
				"end",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q1 := NewQuery(tt.queryString[0], MyDB, "")
			resp1, _ := c.Query(q1)
			q2 := NewQuery(tt.queryString[1], MyDB, "")
			resp2, _ := c.Query(q2)
			resp := MergeResultTable(resp1, resp2)
			if resp.ToString() != tt.expected {
				t.Error("merged resp:\t", resp.ToString())
				t.Error("expected:\t", tt.expected)
			}
		})
	}
}

func TestMergeResultTable2(t *testing.T) {
	c, err := NewHTTPClient(HTTPConfig{
		Addr: "http://localhost:8086",
	})
	if err != nil {
		log.Fatal(err)
	}
	queryString1 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:10:00Z' GROUP BY randtag,location"
	//SCHEMA time index location randtag location=coyote_creek randtag=1
	//2019-08-18T00:06:00Z 66 coyote_creek 1
	//SCHEMA time index location randtag location=coyote_creek randtag=3
	//2019-08-18T00:00:00Z 85 coyote_creek 3
	//SCHEMA time index location randtag location=santa_monica randtag=1
	//2019-08-18T00:06:00Z 67 santa_monica 1
	//SCHEMA time index location randtag location=santa_monica randtag=2
	//2019-08-18T00:00:00Z 11 santa_monica 2
	//end
	queryString2 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:15:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location"
	//SCHEMA time index location randtag location=coyote_creek randtag=1
	//2019-08-18T00:18:00Z 91 coyote_creek 1
	//2019-08-18T00:24:00Z 29 coyote_creek 1
	//SCHEMA time index location randtag location=coyote_creek randtag=3
	//2019-08-18T00:30:00Z 75 coyote_creek 3
	//SCHEMA time index location randtag location=santa_monica randtag=1
	//2019-08-18T00:18:00Z 14 santa_monica 1
	//SCHEMA time index location randtag location=santa_monica randtag=2
	//2019-08-18T00:30:00Z 79 santa_monica 2
	//SCHEMA time index location randtag location=santa_monica randtag=3
	//2019-08-18T00:24:00Z 44 santa_monica 3
	//end
	queryString3 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T01:31:00Z' AND time <= '2019-08-18T01:40:00Z' GROUP BY randtag,location"
	//SCHEMA time index location randtag location=coyote_creek randtag=1
	//2019-08-18T01:36:00Z 71 coyote_creek 1
	//SCHEMA time index location randtag location=santa_monica randtag=3
	//2019-08-18T01:36:00Z 75 santa_monica 3
	//end
	queryString4 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T03:31:00Z' AND time <= '2019-08-18T03:40:00Z' GROUP BY randtag,location"
	//SCHEMA time index location randtag location=coyote_creek randtag=2
	//2019-08-18T03:36:00Z 5 coyote_creek 2
	//SCHEMA time index location randtag location=santa_monica randtag=2
	//2019-08-18T03:36:00Z 66 santa_monica 2
	//end
	queryString5 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T03:40:00Z' AND time <= '2019-08-18T04:00:00Z' GROUP BY randtag,location"
	//SCHEMA time index location randtag location=coyote_creek randtag=1
	//2019-08-18T03:48:00Z 43 coyote_creek 1
	//SCHEMA time index location randtag location=coyote_creek randtag=2
	//2019-08-18T03:42:00Z 77 coyote_creek 2
	//SCHEMA time index location randtag location=coyote_creek randtag=3
	//2019-08-18T03:54:00Z 73 coyote_creek 3
	//2019-08-18T04:00:00Z 57 coyote_creek 3
	//SCHEMA time index location randtag location=santa_monica randtag=1
	//2019-08-18T03:48:00Z 62 santa_monica 1
	//2019-08-18T03:54:00Z 27 santa_monica 1
	//SCHEMA time index location randtag location=santa_monica randtag=3
	//2019-08-18T03:42:00Z 69 santa_monica 3
	//2019-08-18T04:00:00Z 22 santa_monica 3
	//end

	tests := []struct {
		name     string
		querys   []string
		expected string
	}{
		{
			name:   " 1 2 ",
			querys: []string{queryString1, queryString2},
			expected: "SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
				"2019-08-18T00:06:00Z 66 coyote_creek 1 \r\n" +
				"2019-08-18T00:18:00Z 91 coyote_creek 1 \r\n" +
				"2019-08-18T00:24:00Z 29 coyote_creek 1 \r\n" +
				"SCHEMA time index location randtag location=coyote_creek randtag=3 \r\n" +
				"2019-08-18T00:00:00Z 85 coyote_creek 3 \r\n" +
				"2019-08-18T00:30:00Z 75 coyote_creek 3 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=1 \r\n" +
				"2019-08-18T00:06:00Z 67 santa_monica 1 \r\n" +
				"2019-08-18T00:18:00Z 14 santa_monica 1 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=2 \r\n" +
				"2019-08-18T00:00:00Z 11 santa_monica 2 \r\n" +
				"2019-08-18T00:30:00Z 79 santa_monica 2 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
				"2019-08-18T00:24:00Z 44 santa_monica 3 \r\n" +
				"end",
		},
		{
			name:   " 3 2 ",
			querys: []string{queryString3, queryString2},
			expected: "SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
				"2019-08-18T01:36:00Z 71 coyote_creek 1 \r\n" +
				"2019-08-18T00:18:00Z 91 coyote_creek 1 \r\n" +
				"2019-08-18T00:24:00Z 29 coyote_creek 1 \r\n" +
				"SCHEMA time index location randtag location=coyote_creek randtag=3 \r\n" +
				"2019-08-18T00:30:00Z 75 coyote_creek 3 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=1 \r\n" +
				"2019-08-18T00:18:00Z 14 santa_monica 1 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=2 \r\n" +
				"2019-08-18T00:30:00Z 79 santa_monica 2 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
				"2019-08-18T01:36:00Z 75 santa_monica 3 \r\n" +
				"2019-08-18T00:24:00Z 44 santa_monica 3 \r\n" +
				"end",
		},
		{
			name:   " 3 4 ",
			querys: []string{queryString3, queryString4},
			expected: "SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
				"2019-08-18T01:36:00Z 71 coyote_creek 1 \r\n" +
				"SCHEMA time index location randtag location=coyote_creek randtag=2 \r\n" +
				"2019-08-18T03:36:00Z 5 coyote_creek 2 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=2 \r\n" +
				"2019-08-18T03:36:00Z 66 santa_monica 2 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
				"2019-08-18T01:36:00Z 75 santa_monica 3 \r\n" +
				"end",
		},
		{
			name:   " 4 5 ",
			querys: []string{queryString4, queryString5},
			expected: "SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
				"2019-08-18T03:48:00Z 43 coyote_creek 1 \r\n" +
				"SCHEMA time index location randtag location=coyote_creek randtag=2 \r\n" +
				"2019-08-18T03:36:00Z 5 coyote_creek 2 \r\n" +
				"2019-08-18T03:42:00Z 77 coyote_creek 2 \r\n" +
				"SCHEMA time index location randtag location=coyote_creek randtag=3 \r\n" +
				"2019-08-18T03:54:00Z 73 coyote_creek 3 \r\n" +
				"2019-08-18T04:00:00Z 57 coyote_creek 3 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=1 \r\n" +
				"2019-08-18T03:48:00Z 62 santa_monica 1 \r\n" +
				"2019-08-18T03:54:00Z 27 santa_monica 1 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=2 \r\n" +
				"2019-08-18T03:36:00Z 66 santa_monica 2 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
				"2019-08-18T03:42:00Z 69 santa_monica 3 \r\n" +
				"2019-08-18T04:00:00Z 22 santa_monica 3 \r\n" +
				"end",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query1 := NewQuery(tt.querys[0], MyDB, "")
			resp1, _ := c.Query(query1)
			query2 := NewQuery(tt.querys[1], MyDB, "")
			resp2, _ := c.Query(query2)

			merged := MergeResultTable(resp1, resp2)
			if strings.Compare(merged.ToString(), tt.expected) != 0 {
				t.Errorf("merged:\n%s", merged.ToString())
				t.Errorf("expected:\n%s", tt.expected)
			}
		})
	}

}

func TestMerge(t *testing.T) {
	c, err := NewHTTPClient(HTTPConfig{
		Addr: "http://localhost:8086",
	})
	if err != nil {
		log.Fatal(err)
	}

	query1 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location"
	nq1 := NewQuery(query1, MyDB, "")
	resp1, err := c.Query(nq1)
	resp1.ToString()
	//SCHEMA time index location randtag location=coyote_creek randtag=1
	//2019-08-18T00:06:00Z 66 coyote_creek 1
	//2019-08-18T00:18:00Z 91 coyote_creek 1
	//2019-08-18T00:24:00Z 29 coyote_creek 1
	//SCHEMA time index location randtag location=coyote_creek randtag=2
	//2019-08-18T00:12:00Z 78 coyote_creek 2
	//SCHEMA time index location randtag location=coyote_creek randtag=3
	//2019-08-18T00:00:00Z 85 coyote_creek 3
	//2019-08-18T00:30:00Z 75 coyote_creek 3
	//SCHEMA time index location randtag location=santa_monica randtag=1
	//2019-08-18T00:06:00Z 67 santa_monica 1
	//2019-08-18T00:18:00Z 14 santa_monica 1
	//SCHEMA time index location randtag location=santa_monica randtag=2
	//2019-08-18T00:00:00Z 11 santa_monica 2
	//2019-08-18T00:12:00Z 91 santa_monica 2
	//2019-08-18T00:30:00Z 79 santa_monica 2
	//SCHEMA time index location randtag location=santa_monica randtag=3
	//2019-08-18T00:24:00Z 44 santa_monica 3
	//end

	// 1 min
	query2 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:31:00Z' AND time <= '2019-08-18T01:00:00Z' GROUP BY randtag,location"
	nq2 := NewQuery(query2, MyDB, "")
	resp2, err := c.Query(nq2)
	resp2.ToString()
	//SCHEMA time index location randtag location=coyote_creek randtag=1
	//2019-08-18T00:42:00Z 55 coyote_creek 1
	//SCHEMA time index location randtag location=coyote_creek randtag=3
	//2019-08-18T00:36:00Z 33 coyote_creek 3
	//2019-08-18T00:48:00Z 29 coyote_creek 3
	//2019-08-18T00:54:00Z 94 coyote_creek 3
	//2019-08-18T01:00:00Z 16 coyote_creek 3
	//SCHEMA time index location randtag location=santa_monica randtag=1
	//2019-08-18T00:36:00Z 25 santa_monica 1
	//2019-08-18T00:42:00Z 10 santa_monica 1
	//2019-08-18T00:48:00Z 7 santa_monica 1
	//SCHEMA time index location randtag location=santa_monica randtag=2
	//2019-08-18T01:00:00Z 83 santa_monica 2
	//SCHEMA time index location randtag location=santa_monica randtag=3
	//2019-08-18T00:54:00Z 27 santa_monica 3
	//end

	// 30 min
	query3 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T01:31:00Z' AND time <= '2019-08-18T02:00:00Z' GROUP BY randtag,location"
	nq3 := NewQuery(query3, MyDB, "")
	resp3, err := c.Query(nq3)
	resp3.ToString()
	//SCHEMA time index location randtag location=coyote_creek randtag=1
	//2019-08-18T01:36:00Z 71 coyote_creek 1
	//2019-08-18T01:54:00Z 8 coyote_creek 1
	//2019-08-18T02:00:00Z 97 coyote_creek 1
	//SCHEMA time index location randtag location=coyote_creek randtag=2
	//2019-08-18T01:48:00Z 24 coyote_creek 2
	//SCHEMA time index location randtag location=coyote_creek randtag=3
	//2019-08-18T01:42:00Z 67 coyote_creek 3
	//SCHEMA time index location randtag location=santa_monica randtag=1
	//2019-08-18T01:42:00Z 8 santa_monica 1
	//2019-08-18T01:48:00Z 70 santa_monica 1
	//2019-08-18T02:00:00Z 82 santa_monica 1
	//SCHEMA time index location randtag location=santa_monica randtag=2
	//2019-08-18T01:54:00Z 86 santa_monica 2
	//SCHEMA time index location randtag location=santa_monica randtag=3
	//2019-08-18T01:36:00Z 75 santa_monica 3
	//end

	// 1 h
	query4 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T03:00:00Z' AND time <= '2019-08-18T04:00:00Z' GROUP BY randtag,location"
	nq4 := NewQuery(query4, MyDB, "")
	resp4, err := c.Query(nq4)
	st4, et4 := GetResponseTimeRange(resp4)
	fmt.Printf("st4:%d\tet4:%d\n", st4, et4)
	resp4.ToString()
	//SCHEMA time index location randtag location=coyote_creek randtag=1
	//2019-08-18T03:12:00Z 90 coyote_creek 1
	//2019-08-18T03:18:00Z 41 coyote_creek 1
	//2019-08-18T03:48:00Z 43 coyote_creek 1
	//SCHEMA time index location randtag location=coyote_creek randtag=2
	//2019-08-18T03:30:00Z 70 coyote_creek 2
	//2019-08-18T03:36:00Z 5 coyote_creek 2
	//2019-08-18T03:42:00Z 77 coyote_creek 2
	//SCHEMA time index location randtag location=coyote_creek randtag=3
	//2019-08-18T03:00:00Z 37 coyote_creek 3
	//2019-08-18T03:06:00Z 13 coyote_creek 3
	//2019-08-18T03:24:00Z 22 coyote_creek 3
	//2019-08-18T03:54:00Z 73 coyote_creek 3
	//2019-08-18T04:00:00Z 57 coyote_creek 3
	//SCHEMA time index location randtag location=santa_monica randtag=1
	//2019-08-18T03:06:00Z 28 santa_monica 1
	//2019-08-18T03:12:00Z 19 santa_monica 1
	//2019-08-18T03:48:00Z 62 santa_monica 1
	//2019-08-18T03:54:00Z 27 santa_monica 1
	//SCHEMA time index location randtag location=santa_monica randtag=2
	//2019-08-18T03:00:00Z 90 santa_monica 2
	//2019-08-18T03:18:00Z 56 santa_monica 2
	//2019-08-18T03:30:00Z 96 santa_monica 2
	//2019-08-18T03:36:00Z 66 santa_monica 2
	//SCHEMA time index location randtag location=santa_monica randtag=3
	//2019-08-18T03:24:00Z 1 santa_monica 3
	//2019-08-18T03:42:00Z 69 santa_monica 3
	//2019-08-18T04:00:00Z 22 santa_monica 3
	//end

	// 1 s
	query5 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T04:00:01Z' AND time <= '2019-08-18T04:30:00Z' GROUP BY randtag,location"
	nq5 := NewQuery(query5, MyDB, "")
	resp5, err := c.Query(nq5)
	st5, et5 := GetResponseTimeRange(resp5)
	fmt.Printf("st5:%d\tet5:%d\n", st5, et5)
	resp5.ToString()
	//SCHEMA time index location randtag location=coyote_creek randtag=1
	//2019-08-18T04:18:00Z 64 coyote_creek 1
	//2019-08-18T04:30:00Z 14 coyote_creek 1
	//SCHEMA time index location randtag location=coyote_creek randtag=2
	//2019-08-18T04:06:00Z 63 coyote_creek 2
	//2019-08-18T04:24:00Z 59 coyote_creek 2
	//SCHEMA time index location randtag location=coyote_creek randtag=3
	//2019-08-18T04:12:00Z 41 coyote_creek 3
	//SCHEMA time index location randtag location=santa_monica randtag=1
	//2019-08-18T04:18:00Z 89 santa_monica 1
	//2019-08-18T04:24:00Z 80 santa_monica 1
	//SCHEMA time index location randtag location=santa_monica randtag=2
	//2019-08-18T04:06:00Z 24 santa_monica 2
	//SCHEMA time index location randtag location=santa_monica randtag=3
	//2019-08-18T04:12:00Z 48 santa_monica 3
	//2019-08-18T04:30:00Z 42 santa_monica 3
	//end

	//当前时间间隔设置为 1 min,	上面的五个结果中，resp1和resp2、resp4和resp5 理论上可以合并，实际上resp1和resp2的起止时间之差超过了误差范围，不能合并
	// 时间间隔设置为 1h 时，可以合并	暂时修改为 1h
	fmt.Printf("st5 - et4:%d\t\n", st5-et4)
	fmt.Println("(st5-et4)>uint64(time.Minute):", (st5-et4) > uint64(time.Minute))
	fmt.Println("(st5-et4)>uint64(time.Hour):", (st5-et4) > uint64(time.Hour))

	tests := []struct {
		name     string
		resps    []*Response
		expected []string
	}{
		{
			name:  " 5 4 ",
			resps: []*Response{resp5, resp4},
			expected: []string{"SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
				"2019-08-18T03:12:00Z 90 coyote_creek 1 \r\n" +
				"2019-08-18T03:18:00Z 41 coyote_creek 1 \r\n" +
				"2019-08-18T03:48:00Z 43 coyote_creek 1 \r\n" +
				"2019-08-18T04:18:00Z 64 coyote_creek 1 \r\n" +
				"2019-08-18T04:30:00Z 14 coyote_creek 1 \r\n" +
				"SCHEMA time index location randtag location=coyote_creek randtag=2 \r\n" +
				"2019-08-18T03:30:00Z 70 coyote_creek 2 \r\n" +
				"2019-08-18T03:36:00Z 5 coyote_creek 2 \r\n" +
				"2019-08-18T03:42:00Z 77 coyote_creek 2 \r\n" +
				"2019-08-18T04:06:00Z 63 coyote_creek 2 \r\n" +
				"2019-08-18T04:24:00Z 59 coyote_creek 2 \r\n" +
				"SCHEMA time index location randtag location=coyote_creek randtag=3 \r\n" +
				"2019-08-18T03:00:00Z 37 coyote_creek 3 \r\n" +
				"2019-08-18T03:06:00Z 13 coyote_creek 3 \r\n" +
				"2019-08-18T03:24:00Z 22 coyote_creek 3 \r\n" +
				"2019-08-18T03:54:00Z 73 coyote_creek 3 \r\n" +
				"2019-08-18T04:00:00Z 57 coyote_creek 3 \r\n" +
				"2019-08-18T04:12:00Z 41 coyote_creek 3 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=1 \r\n" +
				"2019-08-18T03:06:00Z 28 santa_monica 1 \r\n" +
				"2019-08-18T03:12:00Z 19 santa_monica 1 \r\n" +
				"2019-08-18T03:48:00Z 62 santa_monica 1 \r\n" +
				"2019-08-18T03:54:00Z 27 santa_monica 1 \r\n" +
				"2019-08-18T04:18:00Z 89 santa_monica 1 \r\n" +
				"2019-08-18T04:24:00Z 80 santa_monica 1 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=2 \r\n" +
				"2019-08-18T03:00:00Z 90 santa_monica 2 \r\n" +
				"2019-08-18T03:18:00Z 56 santa_monica 2 \r\n" +
				"2019-08-18T03:30:00Z 96 santa_monica 2 \r\n" +
				"2019-08-18T03:36:00Z 66 santa_monica 2 \r\n" +
				"2019-08-18T04:06:00Z 24 santa_monica 2 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
				"2019-08-18T03:24:00Z 1 santa_monica 3 \r\n" +
				"2019-08-18T03:42:00Z 69 santa_monica 3 \r\n" +
				"2019-08-18T04:00:00Z 22 santa_monica 3 \r\n" +
				"2019-08-18T04:12:00Z 48 santa_monica 3 \r\n" +
				"2019-08-18T04:30:00Z 42 santa_monica 3 \r\n" +
				"end"},
		},
		{
			name:  " 2 1 ",
			resps: []*Response{resp2, resp1},
			expected: []string{"SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
				"2019-08-18T00:06:00Z 66 coyote_creek 1 \r\n" +
				"2019-08-18T00:18:00Z 91 coyote_creek 1 \r\n" +
				"2019-08-18T00:24:00Z 29 coyote_creek 1 \r\n" +
				"2019-08-18T00:42:00Z 55 coyote_creek 1 \r\n" +
				"SCHEMA time index location randtag location=coyote_creek randtag=2 \r\n" +
				"2019-08-18T00:12:00Z 78 coyote_creek 2 \r\n" +
				"SCHEMA time index location randtag location=coyote_creek randtag=3 \r\n" +
				"2019-08-18T00:00:00Z 85 coyote_creek 3 \r\n" +
				"2019-08-18T00:30:00Z 75 coyote_creek 3 \r\n" +
				"2019-08-18T00:36:00Z 33 coyote_creek 3 \r\n" +
				"2019-08-18T00:48:00Z 29 coyote_creek 3 \r\n" +
				"2019-08-18T00:54:00Z 94 coyote_creek 3 \r\n" +
				"2019-08-18T01:00:00Z 16 coyote_creek 3 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=1 \r\n" +
				"2019-08-18T00:06:00Z 67 santa_monica 1 \r\n" +
				"2019-08-18T00:18:00Z 14 santa_monica 1 \r\n" +
				"2019-08-18T00:36:00Z 25 santa_monica 1 \r\n" +
				"2019-08-18T00:42:00Z 10 santa_monica 1 \r\n" +
				"2019-08-18T00:48:00Z 7 santa_monica 1 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=2 \r\n" +
				"2019-08-18T00:00:00Z 11 santa_monica 2 \r\n" +
				"2019-08-18T00:12:00Z 91 santa_monica 2 \r\n" +
				"2019-08-18T00:30:00Z 79 santa_monica 2 \r\n" +
				"2019-08-18T01:00:00Z 83 santa_monica 2 \r\n" +
				"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
				"2019-08-18T00:24:00Z 44 santa_monica 3 \r\n" +
				"2019-08-18T00:54:00Z 27 santa_monica 3 \r\n" +
				"end",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			merged := Merge("h", tt.resps[0], tt.resps[1])
			for m := range merged {
				//if merged[m].ToString() != tt.expected[m] {
				//	t.Error("merged:\t", merged[m].ToString())
				//	t.Error("expected:\t", tt.expected[m])
				//}
				fmt.Printf("merged:\t%s\n", merged[m].ToString())
			}
		})
	}

}

func TestMerge2(t *testing.T) {
	c, err := NewHTTPClient(HTTPConfig{
		Addr: "http://localhost:8086",
	})
	if err != nil {
		log.Fatal(err)
	}

	queryString1 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:10:00Z' GROUP BY randtag,location"
	//SCHEMA time index location randtag location=coyote_creek randtag=1
	//2019-08-18T00:06:00Z 66 coyote_creek 1
	//SCHEMA time index location randtag location=coyote_creek randtag=3
	//2019-08-18T00:00:00Z 85 coyote_creek 3
	//SCHEMA time index location randtag location=santa_monica randtag=1
	//2019-08-18T00:06:00Z 67 santa_monica 1
	//SCHEMA time index location randtag location=santa_monica randtag=2
	//2019-08-18T00:00:00Z 11 santa_monica 2
	//end
	queryString2 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:15:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location"
	//SCHEMA time index location randtag location=coyote_creek randtag=1
	//2019-08-18T00:18:00Z 91 coyote_creek 1
	//2019-08-18T00:24:00Z 29 coyote_creek 1
	//SCHEMA time index location randtag location=coyote_creek randtag=3
	//2019-08-18T00:30:00Z 75 coyote_creek 3
	//SCHEMA time index location randtag location=santa_monica randtag=1
	//2019-08-18T00:18:00Z 14 santa_monica 1
	//SCHEMA time index location randtag location=santa_monica randtag=2
	//2019-08-18T00:30:00Z 79 santa_monica 2
	//SCHEMA time index location randtag location=santa_monica randtag=3
	//2019-08-18T00:24:00Z 44 santa_monica 3
	//end
	queryString3 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T01:31:00Z' AND time <= '2019-08-18T01:40:00Z' GROUP BY randtag,location"
	//SCHEMA time index location randtag location=coyote_creek randtag=1
	//2019-08-18T01:36:00Z 71 coyote_creek 1
	//SCHEMA time index location randtag location=santa_monica randtag=3
	//2019-08-18T01:36:00Z 75 santa_monica 3
	//end
	queryString4 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T03:31:00Z' AND time <= '2019-08-18T03:40:00Z' GROUP BY randtag,location"
	//SCHEMA time index location randtag location=coyote_creek randtag=2
	//2019-08-18T03:36:00Z 5 coyote_creek 2
	//SCHEMA time index location randtag location=santa_monica randtag=2
	//2019-08-18T03:36:00Z 66 santa_monica 2
	//end
	queryString5 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T03:40:00Z' AND time <= '2019-08-18T04:00:00Z' GROUP BY randtag,location"
	//SCHEMA time index location randtag location=coyote_creek randtag=1
	//2019-08-18T03:48:00Z 43 coyote_creek 1
	//SCHEMA time index location randtag location=coyote_creek randtag=2
	//2019-08-18T03:42:00Z 77 coyote_creek 2
	//SCHEMA time index location randtag location=coyote_creek randtag=3
	//2019-08-18T03:54:00Z 73 coyote_creek 3
	//2019-08-18T04:00:00Z 57 coyote_creek 3
	//SCHEMA time index location randtag location=santa_monica randtag=1
	//2019-08-18T03:48:00Z 62 santa_monica 1
	//2019-08-18T03:54:00Z 27 santa_monica 1
	//SCHEMA time index location randtag location=santa_monica randtag=3
	//2019-08-18T03:42:00Z 69 santa_monica 3
	//2019-08-18T04:00:00Z 22 santa_monica 3
	//end
	tests := []struct {
		name     string
		querys   []string
		expected []string
	}{
		{
			name:   " 1 2 3 4 5 precision=\"h\" merged: 1 with 2 , 4 with 5 ",
			querys: []string{queryString1, queryString2, queryString3, queryString4, queryString5},
			expected: []string{
				"SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
					"2019-08-18T00:06:00Z 66 coyote_creek 1 \r\n" +
					"2019-08-18T00:18:00Z 91 coyote_creek 1 \r\n" +
					"2019-08-18T00:24:00Z 29 coyote_creek 1 \r\n" +
					"SCHEMA time index location randtag location=coyote_creek randtag=3 \r\n" +
					"2019-08-18T00:00:00Z 85 coyote_creek 3 \r\n" +
					"2019-08-18T00:30:00Z 75 coyote_creek 3 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=1 \r\n" +
					"2019-08-18T00:06:00Z 67 santa_monica 1 \r\n" +
					"2019-08-18T00:18:00Z 14 santa_monica 1 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=2 \r\n" +
					"2019-08-18T00:00:00Z 11 santa_monica 2 \r\n" +
					"2019-08-18T00:30:00Z 79 santa_monica 2 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
					"2019-08-18T00:24:00Z 44 santa_monica 3 \r\n" +
					"end",
				"SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
					"2019-08-18T01:36:00Z 71 coyote_creek 1 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
					"2019-08-18T01:36:00Z 75 santa_monica 3 \r\n" +
					"end",
				"SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
					"2019-08-18T03:48:00Z 43 coyote_creek 1 \r\n" +
					"SCHEMA time index location randtag location=coyote_creek randtag=2 \r\n" +
					"2019-08-18T03:36:00Z 5 coyote_creek 2 \r\n" +
					"2019-08-18T03:42:00Z 77 coyote_creek 2 \r\n" +
					"SCHEMA time index location randtag location=coyote_creek randtag=3 \r\n" +
					"2019-08-18T03:54:00Z 73 coyote_creek 3 \r\n" +
					"2019-08-18T04:00:00Z 57 coyote_creek 3 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=1 \r\n" +
					"2019-08-18T03:48:00Z 62 santa_monica 1 \r\n" +
					"2019-08-18T03:54:00Z 27 santa_monica 1 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=2 \r\n" +
					"2019-08-18T03:36:00Z 66 santa_monica 2 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
					"2019-08-18T03:42:00Z 69 santa_monica 3 \r\n" +
					"2019-08-18T04:00:00Z 22 santa_monica 3 \r\n" +
					"end",
			},
		},
		{
			name:   " 3 5 2 1 4 precision=\"h\" merged: 1 with 2 , 4 with 5 ",
			querys: []string{queryString3, queryString5, queryString2, queryString1, queryString4},
			expected: []string{
				"SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
					"2019-08-18T00:06:00Z 66 coyote_creek 1 \r\n" +
					"2019-08-18T00:18:00Z 91 coyote_creek 1 \r\n" +
					"2019-08-18T00:24:00Z 29 coyote_creek 1 \r\n" +
					"SCHEMA time index location randtag location=coyote_creek randtag=3 \r\n" +
					"2019-08-18T00:00:00Z 85 coyote_creek 3 \r\n" +
					"2019-08-18T00:30:00Z 75 coyote_creek 3 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=1 \r\n" +
					"2019-08-18T00:06:00Z 67 santa_monica 1 \r\n" +
					"2019-08-18T00:18:00Z 14 santa_monica 1 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=2 \r\n" +
					"2019-08-18T00:00:00Z 11 santa_monica 2 \r\n" +
					"2019-08-18T00:30:00Z 79 santa_monica 2 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
					"2019-08-18T00:24:00Z 44 santa_monica 3 \r\n" +
					"end",
				"SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
					"2019-08-18T01:36:00Z 71 coyote_creek 1 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
					"2019-08-18T01:36:00Z 75 santa_monica 3 \r\n" +
					"end",
				"SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
					"2019-08-18T03:48:00Z 43 coyote_creek 1 \r\n" +
					"SCHEMA time index location randtag location=coyote_creek randtag=2 \r\n" +
					"2019-08-18T03:36:00Z 5 coyote_creek 2 \r\n" +
					"2019-08-18T03:42:00Z 77 coyote_creek 2 \r\n" +
					"SCHEMA time index location randtag location=coyote_creek randtag=3 \r\n" +
					"2019-08-18T03:54:00Z 73 coyote_creek 3 \r\n" +
					"2019-08-18T04:00:00Z 57 coyote_creek 3 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=1 \r\n" +
					"2019-08-18T03:48:00Z 62 santa_monica 1 \r\n" +
					"2019-08-18T03:54:00Z 27 santa_monica 1 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=2 \r\n" +
					"2019-08-18T03:36:00Z 66 santa_monica 2 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
					"2019-08-18T03:42:00Z 69 santa_monica 3 \r\n" +
					"2019-08-18T04:00:00Z 22 santa_monica 3 \r\n" +
					"end",
			},
		},
		{
			name:   " 5 4 3 2 1 precision=\"h\" merged: 1 with 2 , 4 with 5 ",
			querys: []string{queryString5, queryString4, queryString3, queryString2, queryString1},
			expected: []string{
				"SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
					"2019-08-18T00:06:00Z 66 coyote_creek 1 \r\n" +
					"2019-08-18T00:18:00Z 91 coyote_creek 1 \r\n" +
					"2019-08-18T00:24:00Z 29 coyote_creek 1 \r\n" +
					"SCHEMA time index location randtag location=coyote_creek randtag=3 \r\n" +
					"2019-08-18T00:00:00Z 85 coyote_creek 3 \r\n" +
					"2019-08-18T00:30:00Z 75 coyote_creek 3 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=1 \r\n" +
					"2019-08-18T00:06:00Z 67 santa_monica 1 \r\n" +
					"2019-08-18T00:18:00Z 14 santa_monica 1 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=2 \r\n" +
					"2019-08-18T00:00:00Z 11 santa_monica 2 \r\n" +
					"2019-08-18T00:30:00Z 79 santa_monica 2 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
					"2019-08-18T00:24:00Z 44 santa_monica 3 \r\n" +
					"end",
				"SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
					"2019-08-18T01:36:00Z 71 coyote_creek 1 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
					"2019-08-18T01:36:00Z 75 santa_monica 3 \r\n" +
					"end",
				"SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
					"2019-08-18T03:48:00Z 43 coyote_creek 1 \r\n" +
					"SCHEMA time index location randtag location=coyote_creek randtag=2 \r\n" +
					"2019-08-18T03:36:00Z 5 coyote_creek 2 \r\n" +
					"2019-08-18T03:42:00Z 77 coyote_creek 2 \r\n" +
					"SCHEMA time index location randtag location=coyote_creek randtag=3 \r\n" +
					"2019-08-18T03:54:00Z 73 coyote_creek 3 \r\n" +
					"2019-08-18T04:00:00Z 57 coyote_creek 3 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=1 \r\n" +
					"2019-08-18T03:48:00Z 62 santa_monica 1 \r\n" +
					"2019-08-18T03:54:00Z 27 santa_monica 1 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=2 \r\n" +
					"2019-08-18T03:36:00Z 66 santa_monica 2 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
					"2019-08-18T03:42:00Z 69 santa_monica 3 \r\n" +
					"2019-08-18T04:00:00Z 22 santa_monica 3 \r\n" +
					"end",
			},
		},
		{
			name:   " 5 4 2  precision=\"h\" merged:  4 with 5 ",
			querys: []string{queryString5, queryString4, queryString2},
			expected: []string{
				"SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
					"2019-08-18T00:18:00Z 91 coyote_creek 1 \r\n" +
					"2019-08-18T00:24:00Z 29 coyote_creek 1 \r\n" +
					"SCHEMA time index location randtag location=coyote_creek randtag=3 \r\n" +
					"2019-08-18T00:30:00Z 75 coyote_creek 3 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=1 \r\n" +
					"2019-08-18T00:18:00Z 14 santa_monica 1 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=2 \r\n" +
					"2019-08-18T00:30:00Z 79 santa_monica 2 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
					"2019-08-18T00:24:00Z 44 santa_monica 3 \r\n" +
					"end",
				"SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
					"2019-08-18T03:48:00Z 43 coyote_creek 1 \r\n" +
					"SCHEMA time index location randtag location=coyote_creek randtag=2 \r\n" +
					"2019-08-18T03:36:00Z 5 coyote_creek 2 \r\n" +
					"2019-08-18T03:42:00Z 77 coyote_creek 2 \r\n" +
					"SCHEMA time index location randtag location=coyote_creek randtag=3 \r\n" +
					"2019-08-18T03:54:00Z 73 coyote_creek 3 \r\n" +
					"2019-08-18T04:00:00Z 57 coyote_creek 3 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=1 \r\n" +
					"2019-08-18T03:48:00Z 62 santa_monica 1 \r\n" +
					"2019-08-18T03:54:00Z 27 santa_monica 1 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=2 \r\n" +
					"2019-08-18T03:36:00Z 66 santa_monica 2 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
					"2019-08-18T03:42:00Z 69 santa_monica 3 \r\n" +
					"2019-08-18T04:00:00Z 22 santa_monica 3 \r\n" +
					"end",
			},
		},
		{
			name:   " 3 1 4  precision=\"h\" merged: none ",
			querys: []string{queryString3, queryString1, queryString4},
			expected: []string{
				"SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
					"2019-08-18T00:06:00Z 66 coyote_creek 1 \r\n" +
					"SCHEMA time index location randtag location=coyote_creek randtag=3 \r\n" +
					"2019-08-18T00:00:00Z 85 coyote_creek 3 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=1 \r\n" +
					"2019-08-18T00:06:00Z 67 santa_monica 1 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=2 \r\n" +
					"2019-08-18T00:00:00Z 11 santa_monica 2 \r\n" +
					"end",
				"SCHEMA time index location randtag location=coyote_creek randtag=1 \r\n" +
					"2019-08-18T01:36:00Z 71 coyote_creek 1 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=3 \r\n" +
					"2019-08-18T01:36:00Z 75 santa_monica 3 \r\n" +
					"end",
				"SCHEMA time index location randtag location=coyote_creek randtag=2 \r\n" +
					"2019-08-18T03:36:00Z 5 coyote_creek 2 \r\n" +
					"SCHEMA time index location randtag location=santa_monica randtag=2 \r\n" +
					"2019-08-18T03:36:00Z 66 santa_monica 2 \r\n" +
					"end",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var resps []*Response
			for i := range tt.querys {
				query := NewQuery(tt.querys[i], MyDB, "")
				respTmp, _ := c.Query(query)
				resps = append(resps, respTmp)
			}
			merged := Merge("h", resps...)
			for i, m := range merged {
				//fmt.Println(m.ToString())
				if strings.Compare(m.ToString(), tt.expected[i]) != 0 {
					t.Errorf("merged:\n%s", m.ToString())
					t.Errorf("expexted:\n%s", tt.expected[i])
				}
			}
		})
	}

}

func TestGetSeriesTagsMap(t *testing.T) {
	c, err := NewHTTPClient(HTTPConfig{
		Addr: "http://localhost:8086",
	})
	if err != nil {
		log.Fatal(err)
	}

	tests := []struct {
		name        string
		queryString string
		expected    string
	}{
		{
			name:        " 6 series ",
			queryString: "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location",
			expected:    "length == 6  map[0:map[location:coyote_creek randtag:1] 1:map[location:coyote_creek randtag:2] 2:map[location:coyote_creek randtag:3] 3:map[location:santa_monica randtag:1] 4:map[location:santa_monica randtag:2] 5:map[location:santa_monica randtag:3]]",
		},
		{
			name:        " 5 series ",
			queryString: "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:31:00Z' AND time <= '2019-08-18T01:00:00Z' GROUP BY randtag,location",
			expected:    "length == 5  map[0:map[location:coyote_creek randtag:1] 1:map[location:coyote_creek randtag:3] 2:map[location:santa_monica randtag:1] 3:map[location:santa_monica randtag:2] 4:map[location:santa_monica randtag:3]]",
		},
		{
			name:        " 1 series (without GROUP BY) ",
			queryString: "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:31:00Z' AND time <= '2019-08-18T01:00:00Z'",
			expected:    "length == 1  map[0:map[]]",
		},
		{
			name:        " 1 series (with GROUP BY) ",
			queryString: "SELECT index,location,randtag FROM h2o_quality WHERE randtag='1' AND time >= '2019-08-18T00:31:00Z' AND time <= '2019-08-18T01:00:00Z' GROUP BY randtag",
			expected:    "length == 1  map[0:map[randtag:1]]",
		},
		{
			name:        " 0 series ",
			queryString: "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2029-08-18T00:31:00Z' AND time <= '2029-08-18T01:00:00Z'",
			expected:    "length == 0  map[]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := NewQuery(tt.queryString, MyDB, "")
			resp, _ := c.Query(query)
			tagsMap := GetSeriesTagsMap(resp)
			fmt.Println(len(tagsMap))
			fmt.Println(tagsMap)
		})
	}

}

func TestTagsMapToString(t *testing.T) {
	tests := []struct {
		name     string
		tagsMap  map[string]string
		expected string
	}{
		{
			name:     "empty",
			tagsMap:  map[string]string{},
			expected: "",
		},
		{
			name:     "single",
			tagsMap:  map[string]string{"location": "LA"},
			expected: "location=LA ",
		},
		{
			name:     "double",
			tagsMap:  map[string]string{"location": "LA", "randtag": "2"},
			expected: "location=LA randtag=2 ",
		},
		{
			name:     "multy",
			tagsMap:  map[string]string{"location": "LA", "randtag": "2", "age": "4", "test": "tt"},
			expected: "age=4 location=LA randtag=2 test=tt ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			str := TagsMapToString(tt.tagsMap)
			if str != tt.expected {
				t.Errorf("string:\t%s\nexpected:\t%s", str, tt.expected)
			}
		})
	}
}

func TestMergeSeries(t *testing.T) {
	c, err := NewHTTPClient(HTTPConfig{
		Addr: "http://localhost:8086",
	})
	if err != nil {
		log.Fatal(err)
	}

	tests := []struct {
		name        string
		queryString []string
		expected    string
	}{
		{
			name: " one table without GROUP BY",
			queryString: []string{
				"SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z'",
				"SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:31:00Z' AND time <= '2019-08-18T01:00:00Z'",
			},
			expected: "\r\n",
		},
		{
			name: " first 6 tables, second 5 tables, merged 6 tables",
			queryString: []string{
				"SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location",
				"SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:31:00Z' AND time <= '2019-08-18T01:00:00Z' GROUP BY randtag,location",
			},
			expected: "location=coyote_creek randtag=1 \r\n" +
				"location=coyote_creek randtag=2 \r\n" +
				"location=coyote_creek randtag=3 \r\n" +
				"location=santa_monica randtag=1 \r\n" +
				"location=santa_monica randtag=2 \r\n" +
				"location=santa_monica randtag=3 \r\n",
		},
		{
			name: " first 2 tables, second 2 tables, merged 2 tables ",
			queryString: []string{
				"SELECT COUNT(water_level) FROM h2o_feet WHERE time >= '2019-08-18T00:31:00Z' AND time <= '2019-08-18T01:00:00Z' GROUP BY time(12m),location",
				"SELECT COUNT(water_level) FROM h2o_feet WHERE time >= '2019-08-18T02:00:00Z' AND time <= '2019-08-18T02:30:00Z' GROUP BY time(12m),location",
			},
			expected: "location=coyote_creek \r\n" +
				"location=santa_monica \r\n",
		},
		{
			name: " first 6 tables, second 2 tables, merged 6 tables ",
			queryString: []string{
				"SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T01:31:00Z' AND time <= '2019-08-18T02:00:00Z' GROUP BY randtag,location",
				"SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T01:31:00Z' AND time <= '2019-08-18T01:40:00Z' GROUP BY randtag,location",
			},
			expected: "location=coyote_creek randtag=1 \r\n" +
				"location=coyote_creek randtag=2 \r\n" +
				"location=coyote_creek randtag=3 \r\n" +
				"location=santa_monica randtag=1 \r\n" +
				"location=santa_monica randtag=2 \r\n" +
				"location=santa_monica randtag=3 \r\n",
		},
		{
			name: " first 2 tables, second 6 tables, merged 6 tables",
			queryString: []string{
				"SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T01:31:00Z' AND time <= '2019-08-18T01:40:00Z' GROUP BY randtag,location",
				"SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T01:31:00Z' AND time <= '2019-08-18T02:00:00Z' GROUP BY randtag,location",
			},
			expected: "location=coyote_creek randtag=1 \r\n" +
				"location=coyote_creek randtag=2 \r\n" +
				"location=coyote_creek randtag=3 \r\n" +
				"location=santa_monica randtag=1 \r\n" +
				"location=santa_monica randtag=2 \r\n" +
				"location=santa_monica randtag=3 \r\n",
		},
		{
			name: " first 2 tables, second 5 tables, merged 6 tables",
			queryString: []string{
				"SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T01:31:00Z' AND time <= '2019-08-18T01:40:00Z' GROUP BY randtag,location",
				"SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T01:40:00Z' AND time <= '2019-08-18T02:00:00Z' GROUP BY randtag,location",
			},
			expected: "location=coyote_creek randtag=1 \r\n" +
				"location=coyote_creek randtag=2 \r\n" +
				"location=coyote_creek randtag=3 \r\n" +
				"location=santa_monica randtag=1 \r\n" +
				"location=santa_monica randtag=2 \r\n" +
				"location=santa_monica randtag=3 \r\n",
		},
		{
			name: "first 2 tables, second 3 tables, merged 5 tables",
			queryString: []string{
				"SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T01:31:00Z' AND time <= '2019-08-18T01:40:00Z' GROUP BY randtag,location",
				"SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T01:40:00Z' AND time <= '2019-08-18T01:50:00Z' GROUP BY randtag,location",
			},
			expected: "location=coyote_creek randtag=1 \r\n" +
				"location=coyote_creek randtag=2 \r\n" +
				"location=coyote_creek randtag=3 \r\n" +
				"location=santa_monica randtag=1 \r\n" +
				"location=santa_monica randtag=3 \r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q1 := NewQuery(tt.queryString[0], MyDB, "")
			q2 := NewQuery(tt.queryString[1], MyDB, "")
			resp1, _ := c.Query(q1)
			resp2, _ := c.Query(q2)

			seriesMerged := MergeSeries(resp1, resp2)
			//fmt.Printf("len:%d\n", len(seriesMerged))
			var tagStr string
			for _, s := range seriesMerged {
				tagStr += TagsMapToString(s.Tags)
				tagStr += "\r\n"
			}
			//fmt.Println(tagStr)
			if strings.Compare(tagStr, tt.expected) != 0 {
				t.Errorf("merged:\n%s", tagStr)
				t.Errorf("expected:\n%s", tt.expected)
			}
		})
	}
}

func TestMergeSeries2(t *testing.T) {
	c, err := NewHTTPClient(HTTPConfig{
		Addr: "http://localhost:8086",
	})
	if err != nil {
		log.Fatal(err)
	}

	queryString1 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:10:00Z' GROUP BY randtag,location"
	//SCHEMA time index location randtag location=coyote_creek randtag=1
	//2019-08-18T00:06:00Z 66 coyote_creek 1
	//SCHEMA time index location randtag location=coyote_creek randtag=3
	//2019-08-18T00:00:00Z 85 coyote_creek 3
	//SCHEMA time index location randtag location=santa_monica randtag=1
	//2019-08-18T00:06:00Z 67 santa_monica 1
	//SCHEMA time index location randtag location=santa_monica randtag=2
	//2019-08-18T00:00:00Z 11 santa_monica 2
	//end
	queryString2 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:15:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location"
	//SCHEMA time index location randtag location=coyote_creek randtag=1
	//2019-08-18T00:18:00Z 91 coyote_creek 1
	//2019-08-18T00:24:00Z 29 coyote_creek 1
	//SCHEMA time index location randtag location=coyote_creek randtag=3
	//2019-08-18T00:30:00Z 75 coyote_creek 3
	//SCHEMA time index location randtag location=santa_monica randtag=1
	//2019-08-18T00:18:00Z 14 santa_monica 1
	//SCHEMA time index location randtag location=santa_monica randtag=2
	//2019-08-18T00:30:00Z 79 santa_monica 2
	//SCHEMA time index location randtag location=santa_monica randtag=3
	//2019-08-18T00:24:00Z 44 santa_monica 3
	//end
	queryString3 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T01:31:00Z' AND time <= '2019-08-18T01:40:00Z' GROUP BY randtag,location"
	//SCHEMA time index location randtag location=coyote_creek randtag=1
	//2019-08-18T01:36:00Z 71 coyote_creek 1
	//SCHEMA time index location randtag location=santa_monica randtag=3
	//2019-08-18T01:36:00Z 75 santa_monica 3
	//end
	queryString4 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T03:31:00Z' AND time <= '2019-08-18T03:40:00Z' GROUP BY randtag,location"
	//SCHEMA time index location randtag location=coyote_creek randtag=2
	//2019-08-18T03:36:00Z 5 coyote_creek 2
	//SCHEMA time index location randtag location=santa_monica randtag=2
	//2019-08-18T03:36:00Z 66 santa_monica 2
	//end
	queryString5 := "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T03:40:00Z' AND time <= '2019-08-18T04:00:00Z' GROUP BY randtag,location"
	//SCHEMA time index location randtag location=coyote_creek randtag=1
	//2019-08-18T03:48:00Z 43 coyote_creek 1
	//SCHEMA time index location randtag location=coyote_creek randtag=2
	//2019-08-18T03:42:00Z 77 coyote_creek 2
	//SCHEMA time index location randtag location=coyote_creek randtag=3
	//2019-08-18T03:54:00Z 73 coyote_creek 3
	//2019-08-18T04:00:00Z 57 coyote_creek 3
	//SCHEMA time index location randtag location=santa_monica randtag=1
	//2019-08-18T03:48:00Z 62 santa_monica 1
	//2019-08-18T03:54:00Z 27 santa_monica 1
	//SCHEMA time index location randtag location=santa_monica randtag=3
	//2019-08-18T03:42:00Z 69 santa_monica 3
	//2019-08-18T04:00:00Z 22 santa_monica 3
	//end
	tests := []struct {
		name     string
		querys   []string
		expected string
	}{
		{
			name:   " 1 2 ",
			querys: []string{queryString1, queryString2},
			expected: "location=coyote_creek randtag=1 \r\n" +
				"location=coyote_creek randtag=3 \r\n" +
				"location=santa_monica randtag=1 \r\n" +
				"location=santa_monica randtag=2 \r\n" +
				"location=santa_monica randtag=3 \r\n",
		},
		{
			name:   " 3 2 ",
			querys: []string{queryString3, queryString2},
			expected: "location=coyote_creek randtag=1 \r\n" +
				"location=coyote_creek randtag=3 \r\n" +
				"location=santa_monica randtag=1 \r\n" +
				"location=santa_monica randtag=2 \r\n" +
				"location=santa_monica randtag=3 \r\n",
		},
		{
			name:   " 1 4 ",
			querys: []string{queryString1, queryString4},
			expected: "location=coyote_creek randtag=1 \r\n" +
				"location=coyote_creek randtag=2 \r\n" +
				"location=coyote_creek randtag=3 \r\n" +
				"location=santa_monica randtag=1 \r\n" +
				"location=santa_monica randtag=2 \r\n",
		},
		{
			name:   " 3 4 ",
			querys: []string{queryString3, queryString4},
			expected: "location=coyote_creek randtag=1 \r\n" +
				"location=coyote_creek randtag=2 \r\n" +
				"location=santa_monica randtag=2 \r\n" +
				"location=santa_monica randtag=3 \r\n",
		},
		{
			name:   " 4 3 ",
			querys: []string{queryString4, queryString3},
			expected: "location=coyote_creek randtag=1 \r\n" +
				"location=coyote_creek randtag=2 \r\n" +
				"location=santa_monica randtag=2 \r\n" +
				"location=santa_monica randtag=3 \r\n",
		},
		{
			name:   " 4 5 ",
			querys: []string{queryString4, queryString5},
			expected: "location=coyote_creek randtag=1 \r\n" +
				"location=coyote_creek randtag=2 \r\n" +
				"location=coyote_creek randtag=3 \r\n" +
				"location=santa_monica randtag=1 \r\n" +
				"location=santa_monica randtag=2 \r\n" +
				"location=santa_monica randtag=3 \r\n",
		},
		{
			name:   " 5 2 ",
			querys: []string{queryString5, queryString2},
			expected: "location=coyote_creek randtag=1 \r\n" +
				"location=coyote_creek randtag=2 \r\n" +
				"location=coyote_creek randtag=3 \r\n" +
				"location=santa_monica randtag=1 \r\n" +
				"location=santa_monica randtag=2 \r\n" +
				"location=santa_monica randtag=3 \r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q1 := NewQuery(tt.querys[0], MyDB, "")
			q2 := NewQuery(tt.querys[1], MyDB, "")
			resp1, _ := c.Query(q1)
			resp2, _ := c.Query(q2)

			seriesMerged := MergeSeries(resp1, resp2)
			var tagStr string
			for _, s := range seriesMerged {
				tagStr += TagsMapToString(s.Tags)
				tagStr += "\r\n"
			}

			if strings.Compare(tagStr, tt.expected) != 0 {
				t.Errorf("merged:\n%s", tagStr)
				t.Errorf("expected:\n%s", tt.expected)
			}

		})
	}
}

// done todo 设计新的 TestMerge ，多用几组不同条件的查询和不同时间范围（表的数量尽量不同，顺带测试表结构合并）
/*
	当前的测试用例时间范围太大，导致表的数量基本相同，需要缩小时间范围，增加不同结果中表数量的差距
	对于时间精度和时间范围的测试，受当前数据集影响，基本只能使用 h 精度合并，即使选取的时间精度是 m ，查询到的数据也不能合并
	多测几组查询，用不同数量的tag、时间范围尽量小、让查询结果的表尽量不同

	对 Merge 相关的函数都重新进行了一次测试，结果符合预期，应该没问题
*/

// done todo 检查和 Merge 相关的所有函数以及 Test 的边界条件（查询结果为空应该没问题， tag map 为空会怎样）
/*
	查询结果为空会在对表排序的步骤直接跳过，并根据排好序的结果进行合并，空结果不会有影响
	tag map 为空就是下面所说的，只有一张表，tag字符串为空，数据直接合并成一张新的表；数据没有按照时间重新排序（不需要）
*/

// done todo 表合并之后数据是否需要再处理成按照时间顺序排列
/*
	不同查询的时间范围没有重叠，一张表里的数据本身就是按照时间顺序排列的；
	合并时两张表先按照起止时间排先后顺序，然后直接把后面的表的数据拼接到前面的表的数组上，这样就可以确保原本表内数据顺序不变，两张表的数据整体按照时间递增排列；
	所以先排序再合并的两张表的数据本身时间顺序就是对的，不需要再处理
*/

// done todo 确定在没使用 GROUP BY 时合并的过程、tag map的处理（好像没问题，但是为什么）
/*
	此时结果中只有一个表，tag map为空，合并会直接把两个结果的数据拼接成一个表，分别是两张表的数据按照时间顺序排列
	tag 字符串为空，存入数组时也是有长度的，不会出现数组越界，用空串进行比较等操作没有问题，会直接把唯一的表合并
*/

// done todo Merge()传入不合适的时间精度时会报错，是什么引起的，怎么解决
/*
	时间精度不合适导致没能合并，此时结果中的表数量多于 expected 中的表数量，用tests的索引遍历输出expected的表时出现数组越界问题，不是函数本身的问题
*/