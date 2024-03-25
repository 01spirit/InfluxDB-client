package client

import (
	"fmt"
	"log"
	"reflect"
	"strings"
	"testing"
)

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
