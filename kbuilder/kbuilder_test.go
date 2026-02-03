package kbuilder_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/vingarcia/ksql"
	"github.com/vingarcia/ksql/ksqltest"
	tt "github.com/vingarcia/ksql/internal/testtools"
	"github.com/vingarcia/ksql/kbuilder"
)

func TestBuilderRunAndCount(t *testing.T) {
	ctx := context.Background()

	type mockCall struct {
		expectedQuery  string
		expectedParams []interface{}
	}

	tests := []struct {
		desc string

		inputQuery  kbuilder.Query
		queryErrs   []error
		queryOneErr error
		countResult int

		expectedQueryCalls   []mockCall
		expectedQueryOneCals []mockCall

		expectedCount      int
		expectErrToContain []string
	}{
		{
			desc: "should run query and count successfully",

			// State before
			inputQuery: kbuilder.Query{
				Select:  &User{},
				From:    "users",
				Where:   kbuilder.Where("age > %s", 18),
				OrderBy: "name ASC",
				Limit:   10,
				Offset:  20,
			},

			// Mock outputs
			queryErrs:   []error{nil},
			queryOneErr: nil,
			countResult: 42,

			// Expected mock inputs
			expectedQueryCalls: []mockCall{
				{
					expectedQuery:  `SELECT "name", "age" FROM users WHERE age > $1 ORDER BY name ASC LIMIT 10 OFFSET 20`,
					expectedParams: []interface{}{18},
				},
			},
			expectedQueryOneCals: []mockCall{
				{
					expectedQuery:  `SELECT count(*) AS c FROM users WHERE age > $1`,
					expectedParams: []interface{}{18},
				},
			},

			// Expected outputs
			expectedCount: 42,
		},
		{
			desc: "should propagate error from main query",

			// State before
			inputQuery: kbuilder.Query{
				Select: &User{},
				From:   "users",
				Where:  kbuilder.Where("status = %s", "active"),
			},

			// Mock outputs
			queryErrs: []error{fmt.Errorf("query error")},

			// Expected mock inputs
			expectedQueryCalls: []mockCall{
				{
					expectedQuery:  `SELECT "name", "age" FROM users WHERE status = $1`,
					expectedParams: []interface{}{"active"},
				},
			},
			expectedQueryOneCals: []mockCall{},

			// Expected outputs
			expectErrToContain: []string{"query error"},
		},
		{
			desc: "should propagate error from count query",

			// State before
			inputQuery: kbuilder.Query{
				Select: &User{},
				From:   "users",
			},

			// Mock outputs
			queryErrs:   []error{nil},
			queryOneErr: fmt.Errorf("count query error"),

			// Expected mock inputs
			expectedQueryCalls: []mockCall{
				{
					expectedQuery:  `SELECT "name", "age" FROM users`,
					expectedParams: []interface{}{},
				},
			},
			expectedQueryOneCals: []mockCall{
				{
					expectedQuery:  `SELECT count(*) AS c FROM users`,
					expectedParams: []interface{}{},
				},
			},

			// Expected outputs
			expectErrToContain: []string{"count query error"},
		},
		{
			desc: "should correctly modify query for count (remove limit, offset, orderby)",

			// State before
			inputQuery: kbuilder.Query{
				Select:  &User{},
				From:    "users",
				Where:   kbuilder.Where("age > %s", 18).Where("active = %s", true),
				OrderBy: "name DESC",
				Limit:   50,
				Offset:  100,
			},

			// Mock outputs
			queryErrs:   []error{nil},
			queryOneErr: nil,
			countResult: 100,

			// Expected mock inputs
			expectedQueryCalls: []mockCall{
				{
					expectedQuery:  `SELECT "name", "age" FROM users WHERE age > $1 AND active = $2 ORDER BY name DESC LIMIT 50 OFFSET 100`,
					expectedParams: []interface{}{18, true},
				},
			},
			expectedQueryOneCals: []mockCall{
				{
					expectedQuery:  `SELECT count(*) AS c FROM users WHERE age > $1 AND active = $2`,
					expectedParams: []interface{}{18, true},
				},
			},

			// Expected outputs
			expectedCount: 100,
		},
		{
			desc: "should handle invalid query builder (missing FROM)",

			// State before
			inputQuery: kbuilder.Query{
				Select: &User{},
				// Missing From field - should cause build error
			},

			// Mock outputs
			queryErrs: []error{},

			// Expected mock inputs
			expectedQueryCalls:   []mockCall{},
			expectedQueryOneCals: []mockCall{},

			// Expected outputs
			expectErrToContain: []string{"From field is mandatory"},
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			// Setup captured calls
			var capturedQueryCalls []mockCall
			var capturedQueryOneCalls []mockCall

			// Setup mock responses
			queryErrs := test.queryErrs
			queryOneErr := test.queryOneErr
			countResult := test.countResult

			mock := ksql.Mock{
				QueryFn: func(ctx context.Context, records interface{}, query string, params ...interface{}) error {
					capturedQueryCalls = append(capturedQueryCalls, mockCall{
						expectedQuery:  query,
						expectedParams: params,
					})
					return tt.NextResponse(t, &queryErrs)
				},
				QueryOneFn: func(ctx context.Context, record interface{}, query string, params ...interface{}) error {
					capturedQueryOneCalls = append(capturedQueryOneCalls, mockCall{
						expectedQuery:  query,
						expectedParams: params,
					})
					if queryOneErr != nil {
						return queryOneErr
					}
					// Use FillStructWith to set the C field since the struct is created internally
					return ksqltest.FillStructWith(record, map[string]interface{}{
						"c": countResult,
					})
				},
			}

			builder, err := kbuilder.New("postgres")
			tt.AssertNoErr(t, err)

			var results []User
			count, err := builder.RunAndCount(ctx, mock, &results, test.inputQuery)

			// Verify outputs
			if test.expectErrToContain != nil {
				tt.AssertErrContains(t, err, test.expectErrToContain...)
				return
			}
			tt.AssertNoErr(t, err)

			tt.AssertEqual(t, count, test.expectedCount)

			tt.AssertEqual(t, capturedQueryCalls, test.expectedQueryCalls)
			tt.AssertEqual(t, capturedQueryOneCalls, test.expectedQueryOneCals)
		})
	}
}
