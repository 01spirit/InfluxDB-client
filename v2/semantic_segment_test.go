package client

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

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

func TestFieldsAndAggregation(t *testing.T) {
	tests := []struct {
		name        string
		queryString string
		expected    []string
	}{
		{
			name:        "1",
			queryString: "SELECT water_level FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z'",
			expected:    []string{"water_level[float64]", "empty"},
		},
		{
			name:        "2",
			queryString: "SELECT water_level,location FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z'",
			expected:    []string{"water_level[float64],location[string]", "empty"},
		},
		{
			name:        "3",
			queryString: "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z'",
			expected:    []string{"index[int64],location[string],randtag[string]", "empty"},
		},
		{
			name:        "4",
			queryString: "SELECT location,index,randtag,index FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z'",
			expected:    []string{"location[string],index[int64],randtag[string],index[int64]", "empty"},
		},
		{
			name:        "5",
			queryString: "SELECT COUNT(water_level) FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY time(12m)",
			expected:    []string{"water_level[float64]", "count"},
		},
		{
			name:        "6",
			queryString: "select max(water_level) from h2o_feet where time >= '2019-08-18T00:00:00Z' and time <= '2019-08-18T00:30:00Z' group by time(12m)",
			expected:    []string{"water_level[float64]", "max"},
		},
		{
			name:        "7",
			queryString: "SELECT MEAN(water_level),MEAN(water_level) FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY time(12m)",
			expected:    []string{"water_level[float64],water_level[float64]", "mean"},
		},
		{
			name:        "8",
			queryString: "SELECT MEAN(*) FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY time(12m)",
			expected:    []string{"index[int64]", "mean"},
		},
		{
			name:        "9",
			queryString: "SELECT * FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z'",
			expected:    []string{"index[int64],location[string],randtag[string]", "empty"},
		},
	}
	fmt.Println(Fields)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meaName := MeasurementName(tt.queryString)
			sf, aggr := FieldsAndAggregation(tt.queryString, meaName)
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

func TestPredicatesAndTagConditions(t *testing.T) {
	tests := []struct {
		name         string
		queryString  string
		expected     string
		expectedTags []string
	}{
		{
			name:         "1",
			queryString:  "SELECT index FROM h2o_quality WHERE randtag='2' AND index>=50 AND location='santa_monica' AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY location",
			expected:     "{(index>=50[int64])}",
			expectedTags: []string{"location=santa_monica", "randtag=2"},
		},
		{
			name:         "2",
			queryString:  "SELECT index FROM h2o_quality WHERE location='coyote_creek' AND randtag='2' AND index>=50 AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY location",
			expected:     "{(index>=50[int64])}",
			expectedTags: []string{"location=coyote_creek", "randtag=2"},
		},
		{
			name:         "3",
			queryString:  "SELECT water_level FROM h2o_feet WHERE location != 'santa_monica' AND (water_level < -0.59 OR water_level > 9.95)",
			expected:     "{(water_level<-0.590[float64])(water_level>9.950[float64])}",
			expectedTags: []string{"location!=santa_monica"},
		},
		{
			name:         "4",
			queryString:  "SELECT water_level FROM h2o_feet WHERE location <> 'santa_monica' AND (water_level > -0.59 AND water_level < 9.95) AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY location",
			expected:     "{(water_level>-0.590[float64])(water_level<9.950[float64])}",
			expectedTags: []string{"location!=santa_monica"},
		},
		{
			name:         "5",
			queryString:  "select max(usage_guest) from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:02:00Z' and hostname='host_0' group by time(1m)",
			expected:     "{empty}",
			expectedTags: []string{"hostname=host_0"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meaName := MeasurementName(tt.queryString)
			SP, tagConds := PredicatesAndTagConditions(tt.queryString, meaName, TagKV)

			if strings.Compare(SP, tt.expected) != 0 {
				t.Errorf("SP:\t%s\nexpected:\t%s", SP, tt.expected)
			}
			for i := range tagConds {
				if strings.Compare(tagConds[i], tt.expectedTags[i]) != 0 {
					t.Errorf("tag:\t%s\nexpected tag:\t%s", tagConds[i], tt.expectedTags[i])
				}
			}
			//fmt.Println(SP)
			//fmt.Println(tagConds)
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

func TestMeasurementName(t *testing.T) {
	tests := []struct {
		name        string
		queryString string
		expected    string
	}{
		{
			name:        "1",
			queryString: "SELECT index FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z'",
			expected:    "h2o_quality",
		},
		{
			name:        "2",
			queryString: "select usage_guest from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:00:20Z' and hostname='host_0'",
			expected:    "cpu",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			measurementName := MeasurementName(tt.queryString)

			if measurementName != tt.expected {
				t.Errorf("measurement:\t%s\n", measurementName)
				t.Errorf("expected:\t%s\n", tt.expected)
			}
			//fmt.Println(measurementName)
		})
	}
}

func TestIntegratedSM(t *testing.T) {
	tests := []struct {
		name        string
		queryString string
		expected    string
	}{
		{
			name:        "SF SP",
			queryString: "SELECT index FROM h2o_quality WHERE location='coyote_creek'",
			expected:    "{(h2o_quality.location=coyote_creek)}",
		},
		{
			name:        "SF SP",
			queryString: "SELECT index FROM h2o_quality WHERE randtag='1' AND location='coyote_creek' AND index>50 GROUP BY randtag",
			expected:    "{(h2o_quality.location=coyote_creek,h2o_quality.randtag=1)}",
		},
		{
			name:        "SM SF SP ST",
			queryString: "SELECT index FROM h2o_quality WHERE location='coyote_creek' AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag",
			expected:    "{(h2o_quality.location=coyote_creek,h2o_quality.randtag=1)(h2o_quality.location=coyote_creek,h2o_quality.randtag=2)(h2o_quality.location=coyote_creek,h2o_quality.randtag=3)}",
		},
		{
			name:        "SM SF SP ST SG",
			queryString: "SELECT MAX(water_level) FROM h2o_feet WHERE location='coyote_creek' AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY location,time(12m)",
			expected:    "{(h2o_feet.location=coyote_creek)}",
		},
		{
			name:        "three fields without aggr",
			queryString: "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z'",
			expected:    "{(h2o_quality.empty)}",
		},
		{
			name:        "SM three fields without aggr",
			queryString: "SELECT index,location,randtag FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag",
			expected:    "{(h2o_quality.randtag=1)(h2o_quality.randtag=2)(h2o_quality.randtag=3)}",
		},
		{
			name:        "SM SP three fields without aggr",
			queryString: "SELECT index,location,randtag FROM h2o_quality WHERE location='coyote_creek' AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location",
			expected:    "{(h2o_quality.location=coyote_creek,h2o_quality.randtag=1)(h2o_quality.location=coyote_creek,h2o_quality.randtag=2)(h2o_quality.location=coyote_creek,h2o_quality.randtag=3)}",
		},
		{
			name:        "SP SG aggregation and three predicates",
			queryString: "SELECT COUNT(index) FROM h2o_quality WHERE location='coyote_creek' AND randtag='2' AND index>50 AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY randtag,location,time(10s)",
			expected:    "{(h2o_quality.location=coyote_creek,h2o_quality.randtag=2)}",
		},
		{
			name:        "SP SG aggregation and three predicates",
			queryString: "SELECT COUNT(index) FROM h2o_quality WHERE location='coyote_creek' AND randtag='2' AND index>50 AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY time(10s)",
			expected:    "{(h2o_quality.location=coyote_creek,h2o_quality.randtag=2)}",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			measurement := MeasurementName(tt.queryString)
			_, tagConds := PredicatesAndTagConditions(tt.queryString, measurement, TagKV)
			//fields, aggr := FieldsAndAggregation(queryString, measurement)
			tags := GroupByTags(tt.queryString, measurement)
			//
			//Interval := GetInterval(queryString)

			ss := IntegratedSM(measurement, tagConds, tags)
			//fmt.Println(ss)
			if strings.Compare(ss, tt.expected) != 0 {
				t.Errorf("samantic segment:\t%s", ss)
				t.Errorf("expected:\t%s", tt.expected)
			}
		})
	}
}

func TestCombinationTagValues(t *testing.T) {
	//tagConds := []string{"a=1", "b=2", "c=3"}
	tagValues := [][]string{{"1", "2"}, {"3", "4", "5"}, {"6", "7", "8"}}
	result := make([]string, 0)
	result = combinationTagValues(tagValues)
	fmt.Println(result)
}

func TestGroupByTags(t *testing.T) {
	tests := []struct {
		name        string
		queryString string
		expected    []string
	}{
		{
			name:        "1",
			queryString: "SELECT water_level FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z'",
			expected:    []string{},
		},
		{
			name:        "2",
			queryString: "SELECT water_level FROM h2o_feet WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' group by time(12m)",
			expected:    []string{},
		},
		{
			name:        "3",
			queryString: "SELECT index FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUp by location,time(12m)",
			expected:    []string{"location"},
		},
		{
			name:        "4",
			queryString: "SELECT index FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUp by location, randtag, time(12m)",
			expected:    []string{"location", "randtag"},
		},
		{
			name:        "5",
			queryString: "SELECT index FROM h2o_quality WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUp by location, randtag",
			expected:    []string{"location", "randtag"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meaName := MeasurementName(tt.queryString)
			tagValues := GroupByTags(tt.queryString, meaName)

			for i, val := range tagValues {
				//fmt.Println(val)
				if val != tt.expected[i] {
					t.Errorf("tag value:\t%s\n", val)
					t.Errorf("expected:\t%s\n", tt.expected[i])
				}
			}
			//fmt.Println()
		})
	}
}

func TestSeperateSM(t *testing.T) {
	tests := []struct {
		name        string
		queryString string
		expected    []string
	}{
		{
			name:        "1 1-1-T 直接查询",
			queryString: "select usage_guest from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:00:20Z' and hostname='host_0'",
			expected:    []string{"cpu.hostname=host_0"},
		},
		{
			name:        "1 1-1-T MAX",
			queryString: "select max(usage_guest) from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:02:00Z' and hostname='host_0' group by time(1m)",
			expected:    []string{"cpu.hostname=host_0"},
		},
		{
			name:        "1 1-1-T MEAN",
			queryString: "select mean(usage_guest) from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:02:00Z' and hostname='host_0' group by time(1m)",
			expected:    []string{"cpu.hostname=host_0"},
		},
		{
			name:        "2 3-1-T 直接查询",
			queryString: "select usage_guest,usage_nice,usage_guest_nice from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:00:20Z' and hostname='host_0'",
			expected:    []string{"cpu.hostname=host_0"},
		},
		{
			name:        "2 3-1-T MAX",
			queryString: "select max(usage_guest),max(usage_nice),max(usage_guest_nice) from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:02:00Z' and hostname='host_0' group by time(1m)",
			expected:    []string{"cpu.hostname=host_0"},
		},
		{
			name:        "2 3-1-T MEAN",
			queryString: "select mean(usage_guest),mean(usage_nice),mean(usage_guest_nice) from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:02:00Z' and hostname='host_0' group by time(1m)",
			expected:    []string{"cpu.hostname=host_0"},
		},
		{
			name:        "3 3-1-T 直接查询",
			queryString: "select usage_system,usage_user,usage_guest from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:00:20Z' and hostname='host_0'",
			expected:    []string{"cpu.hostname=host_0"},
		},
		{
			name:        "3 3-1-T MAX",
			queryString: "select max(usage_system),max(usage_user),max(usage_guest) from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:02:00Z' and hostname='host_0' group by time(1m)",
			expected:    []string{"cpu.hostname=host_0"},
		},
		{
			name:        "3 3-1-T MEAN",
			queryString: "select mean(usage_system),mean(usage_user),mean(usage_guest) from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:02:00Z' and hostname='host_0' group by time(1m)",
			expected:    []string{"cpu.hostname=host_0"},
		},
		{
			name:        "4 5-1-T 直接查询",
			queryString: "select usage_system,usage_user,usage_guest,usage_nice,usage_guest_nice from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:00:20Z' and hostname='host_0'",
			expected:    []string{"cpu.hostname=host_0"},
		},
		{
			name:        "4 5-1-T MAX",
			queryString: "select max(usage_system),max(usage_user),max(usage_guest),max(usage_nice),max(usage_guest_nice) from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:02:00Z' and hostname='host_0' group by time(1m)",
			expected:    []string{"cpu.hostname=host_0"},
		},
		{
			name:        "4 5-1-T MEAN",
			queryString: "select mean(usage_system),mean(usage_user),mean(usage_guest),mean(usage_nice),mean(usage_guest_nice) from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:02:00Z' and hostname='host_0' group by time(1m)",
			expected:    []string{"cpu.hostname=host_0"},
		},
		{
			name:        "5 10-1-T 直接查询",
			queryString: "select * from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:00:20Z' and hostname='host_0'",
			expected:    []string{"cpu.hostname=host_0"},
		},
		{
			name:        "5 10-1-T MAX",
			queryString: "select max(*) from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:02:00Z' and hostname='host_0' group by time(1m)",
			expected:    []string{"cpu.hostname=host_0"},
		},
		{
			name:        "5 10-1-T MEAN",
			queryString: "select mean(*) from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:02:00Z' and hostname='host_0' group by time(1m)",
			expected:    []string{"cpu.hostname=host_0"},
		},
		{
			name:        "6 1-1-T",
			queryString: "select usage_guest from test..cpu where time >= '2022-01-01T09:00:00Z' and time < '2022-01-01T10:00:00Z' and hostname='host_0' and usage_guest > 99.0",
			expected:    []string{"cpu.hostname=host_0"},
		},
		{
			name:        "t7-1",
			queryString: "select usage_guest from test..cpu where time >= '2022-01-01T17:50:00Z' and time < '2022-01-01T18:00:00Z' and usage_guest > 99.0 group by hostname",
			expected:    []string{"cpu.hostname=host_0", "cpu.hostname=host_1", "cpu.hostname=host_2", "cpu.hostname=host_3"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			measurement := MeasurementName(tt.queryString)
			_, tagConds := PredicatesAndTagConditions(tt.queryString, measurement, TagKV)
			tags := GroupByTags(tt.queryString, measurement)
			sm := IntegratedSM(measurement, tagConds, tags)

			sepSM := SeperateSM(sm)
			//fmt.Println(sepSM)
			for i, sm := range sepSM {
				if strings.Compare(sm, tt.expected[i]) != 0 {
					t.Errorf("samantic segment:\t%s", sm)
					t.Errorf("expected:\t%s", tt.expected)
				}
			}

		})
	}
}

func TestGetSeperateSemanticSegment(t *testing.T) {
	tests := []struct {
		name        string
		queryString string
		expected    []string
	}{
		{
			name:        "1 1-1-T 直接查询",
			queryString: "select usage_guest from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:00:20Z' and hostname='host_0'",
			expected:    []string{"{(cpu.hostname=host_0)}#{usage_guest[float64]}#{empty}#{empty,empty}"},
		},
		{
			name:        "1 1-1-T MAX",
			queryString: "select max(usage_guest) from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:02:00Z' and hostname='host_0' group by time(1m)",
			expected:    []string{"{(cpu.hostname=host_0)}#{usage_guest[float64]}#{empty}#{max,1m}"},
		},
		{
			name:        "1 1-1-T MEAN",
			queryString: "select mean(usage_guest) from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:02:00Z' and hostname='host_0' group by time(1m)",
			expected:    []string{"{(cpu.hostname=host_0)}#{usage_guest[float64]}#{empty}#{mean,1m}"},
		},
		{
			name:        "2 3-1-T 直接查询",
			queryString: "select usage_guest,usage_nice,usage_guest_nice from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:00:20Z' and hostname='host_0'",
			expected:    []string{"{(cpu.hostname=host_0)}#{usage_guest[float64],usage_nice[float64],usage_guest_nice[float64]}#{empty}#{empty,empty}"},
		},
		{
			name:        "2 3-1-T MAX",
			queryString: "select max(usage_guest),max(usage_nice),max(usage_guest_nice) from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:02:00Z' and hostname='host_0' group by time(1m)",
			expected:    []string{"{(cpu.hostname=host_0)}#{usage_guest[float64],usage_nice[float64],usage_guest_nice[float64]}#{empty}#{max,1m}"},
		},
		{
			name:        "2 3-1-T MEAN",
			queryString: "select mean(usage_guest),mean(usage_nice),mean(usage_guest_nice) from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:02:00Z' and hostname='host_0' group by time(1m)",
			expected:    []string{"{(cpu.hostname=host_0)}#{usage_guest[float64],usage_nice[float64],usage_guest_nice[float64]}#{empty}#{mean,1m}"},
		},
		{
			name:        "3 3-1-T 直接查询",
			queryString: "select usage_system,usage_user,usage_guest from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:00:20Z' and hostname='host_0'",
			expected:    []string{"{(cpu.hostname=host_0)}#{usage_system[float64],usage_user[float64],usage_guest[float64]}#{empty}#{empty,empty}"},
		},
		{
			name:        "3 3-1-T MAX",
			queryString: "select max(usage_system),max(usage_user),max(usage_guest) from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:02:00Z' and hostname='host_0' group by time(1m)",
			expected:    []string{"{(cpu.hostname=host_0)}#{usage_system[float64],usage_user[float64],usage_guest[float64]}#{empty}#{max,1m}"},
		},
		{
			name:        "3 3-1-T MEAN",
			queryString: "select mean(usage_system),mean(usage_user),mean(usage_guest) from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:02:00Z' and hostname='host_0' group by time(1m)",
			expected:    []string{"{(cpu.hostname=host_0)}#{usage_system[float64],usage_user[float64],usage_guest[float64]}#{empty}#{mean,1m}"},
		},
		{
			name:        "4 5-1-T 直接查询",
			queryString: "select usage_system,usage_user,usage_guest,usage_nice,usage_guest_nice from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:00:20Z' and hostname='host_0'",
			expected:    []string{"{(cpu.hostname=host_0)}#{usage_system[float64],usage_user[float64],usage_guest[float64],usage_nice[float64],usage_guest_nice[float64]}#{empty}#{empty,empty}"},
		},
		{
			name:        "4 5-1-T MAX",
			queryString: "select max(usage_system),max(usage_user),max(usage_guest),max(usage_nice),max(usage_guest_nice) from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:02:00Z' and hostname='host_0' group by time(1m)",
			expected:    []string{"{(cpu.hostname=host_0)}#{usage_system[float64],usage_user[float64],usage_guest[float64],usage_nice[float64],usage_guest_nice[float64]}#{empty}#{max,1m}"},
		},
		{
			name:        "4 5-1-T MEAN",
			queryString: "select mean(usage_system),mean(usage_user),mean(usage_guest),mean(usage_nice),mean(usage_guest_nice) from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:02:00Z' and hostname='host_0' group by time(1m)",
			expected:    []string{"{(cpu.hostname=host_0)}#{usage_system[float64],usage_user[float64],usage_guest[float64],usage_nice[float64],usage_guest_nice[float64]}#{empty}#{mean,1m}"},
		},
		{
			name:        "5 10-1-T 直接查询",
			queryString: "select * from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:00:20Z' and hostname='host_0'",
			expected:    []string{"{(cpu.hostname=host_0)}#{arch[string],datacenter[string],hostname[string],os[string],rack[string],region[string],service[string],service_environment[string],service_version[string],team[string],usage_guest[float64],usage_guest_nice[float64],usage_idle[float64],usage_iowait[float64],usage_irq[float64],usage_nice[float64],usage_softirq[float64],usage_steal[float64],usage_system[float64],usage_user[float64]}#{empty}#{empty,empty}"},
		},
		{
			name:        "5 10-1-T MAX",
			queryString: "select max(*) from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:02:00Z' and hostname='host_0' group by time(1m)",
			expected:    []string{"{(cpu.hostname=host_0)}#{usage_guest[float64],usage_guest_nice[float64],usage_idle[float64],usage_iowait[float64],usage_irq[float64],usage_nice[float64],usage_softirq[float64],usage_steal[float64],usage_system[float64],usage_user[float64]}#{empty}#{max,1m}"},
		},
		{
			name:        "5 10-1-T MEAN",
			queryString: "select mean(*) from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:02:00Z' and hostname='host_0' group by time(1m)",
			expected:    []string{"{(cpu.hostname=host_0)}#{usage_guest[float64],usage_guest_nice[float64],usage_idle[float64],usage_iowait[float64],usage_irq[float64],usage_nice[float64],usage_softirq[float64],usage_steal[float64],usage_system[float64],usage_user[float64]}#{empty}#{mean,1m}"},
		},
		{
			name:        "6 1-1-T",
			queryString: "select usage_guest from test..cpu where time >= '2022-01-01T09:00:00Z' and time < '2022-01-01T10:00:00Z' and hostname='host_0' and usage_guest > 99.0",
			expected:    []string{"{(cpu.hostname=host_0)}#{usage_guest[float64]}#{(usage_guest>99.000[float64])}#{empty,empty}"},
		},
		{
			name:        "t7-1",
			queryString: "select usage_guest from test..cpu where time >= '2022-01-01T17:50:00Z' and time < '2022-01-01T18:00:00Z' and usage_guest > 99.0 group by hostname",
			expected: []string{"{(cpu.hostname=host_0)}#{usage_guest[float64]}#{(usage_guest>99.000[float64])}#{empty,empty}",
				"{(cpu.hostname=host_1)}#{usage_guest[float64]}#{(usage_guest>99.000[float64])}#{empty,empty}",
				"{(cpu.hostname=host_2)}#{usage_guest[float64]}#{(usage_guest>99.000[float64])}#{empty,empty}",
				"{(cpu.hostname=host_3)}#{usage_guest[float64]}#{(usage_guest>99.000[float64])}#{empty,empty}"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sepSS := GetSeperateSemanticSegment(tt.queryString)

			for i, ss := range sepSS {
				//fmt.Println(ss)
				if strings.Compare(ss, tt.expected[i]) != 0 {
					t.Errorf("samantic segment:\t%s", ss)
					t.Errorf("expected:\t%s", tt.expected[i])
				}
			}
		})
	}
}

func TestGetSemanticSegment(t *testing.T) {
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
			expected:    "{(cpu.hostname=host_0)(cpu.hostname=host_1)(cpu.hostname=host_2)(cpu.hostname=host_3)}#{usage_guest[float64]}#{(usage_guest>99.000[float64])}#{empty,empty}",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ss := GetSemanticSegment(tt.queryString)
			//fmt.Println(ss)
			if strings.Compare(ss, tt.expected) != 0 {
				t.Errorf("samantic segment:\t%s", ss)
				t.Errorf("expected:\t%s", tt.expected)
			}
		})
	}
}
