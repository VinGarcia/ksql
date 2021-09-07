package kbuilder_test

import (
	"testing"

	"github.com/tj/assert"
	"github.com/vingarcia/ksql/kbuilder"
)

func TestInsertQuery(t *testing.T) {
	tests := []struct {
		desc           string
		query          kbuilder.Insert
		expectedQuery  string
		expectedParams []interface{}
		expectedErr    bool
	}{
		{
			desc: "should build queries witha single record correctly",
			query: kbuilder.Insert{
				Into: "users",
				Data: &User{
					Name: "foo",
					Age:  42,
				},
			},
			expectedQuery:  `INSERT INTO "users" ("name", "age") VALUES ($1, $2)`,
			expectedParams: []interface{}{"foo", 42},
		},
		{
			desc: "should build queries with multiple records correctly",
			query: kbuilder.Insert{
				Into: "users",
				Data: []User{
					{
						Name: "foo",
						Age:  42,
					},
					{
						Name: "bar",
						Age:  43,
					},
				},
			},
			expectedQuery:  `INSERT INTO "users" ("name", "age") VALUES ($1, $2), ($3, $4)`,
			expectedParams: []interface{}{"foo", 42, "bar", 43},
		},

		/* * * * * Testing error cases: * * * * */
		{
			desc: "should report error if the `Data` attribute is missing",
			query: kbuilder.Insert{
				Into: "users",
			},

			expectedErr: true,
		},
		{
			desc: "should report error if the `Into` attribute is missing",
			query: kbuilder.Insert{
				Data: &User{
					Name: "foo",
					Age:  42,
				},
			},

			expectedErr: true,
		},
		{
			desc: "should report error Data contains an empty list",
			query: kbuilder.Insert{
				Into: "users",
				Data: []User{},
			},

			expectedErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			b, err := kbuilder.New("postgres")
			assert.Equal(t, nil, err)

			query, params, err := b.Build(test.query)

			expectError(t, test.expectedErr, err)
			assert.Equal(t, test.expectedQuery, query)
			assert.Equal(t, test.expectedParams, params)
		})
	}
}
