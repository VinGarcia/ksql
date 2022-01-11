package structs_test

import (
	"reflect"
	"testing"

	"github.com/vingarcia/ksql/internal/structs"
	tt "github.com/vingarcia/ksql/internal/testtools"
)

type user struct {
	ID   int    `ksql:"id"`
	Name string `ksql:"name"`
}

func TestParseInputFunc(t *testing.T) {
	t.Run("should parse a function correctly", func(t *testing.T) {
		chunkType, err := structs.ParseInputFunc(func(users []user) error {
			return nil
		})
		tt.AssertNoErr(t, err)
		tt.AssertEqual(t, reflect.TypeOf([]user{}), chunkType)
	})

	t.Run("should return errors correctly", func(t *testing.T) {
		tests := []struct {
			desc               string
			fn                 interface{}
			expectErrToContain []string
		}{
			{
				desc:               "null input function",
				fn:                 nil,
				expectErrToContain: []string{"ForEachChunk", "cannot be nil"},
			},
			{
				desc:               "input is not a function",
				fn:                 "not a function",
				expectErrToContain: []string{"ForEachChunk", "must be a function"},
			},
			{
				desc: "wrong number of arguments",
				fn: func(users []user, foo int) error {
					return nil
				},
				expectErrToContain: []string{"ForEachChunk", "must have 1 argument"},
			},
			{
				desc: "wrong number of return values",
				fn: func(users []user) (int, error) {
					return 0, nil
				},
				expectErrToContain: []string{"ForEachChunk", "must have a single return value"},
			},
			{
				desc: "return value is not an error",
				fn: func(users []user) int {
					return 0
				},
				expectErrToContain: []string{"ForEachChunk", "must be of type error"},
			},
			{
				desc: "input function argument is not slice",
				fn: func(users user) error {
					return nil
				},
				expectErrToContain: []string{"ForEachChunk", "must a slice"},
			},
			{
				desc: "input function argument is not a slice of structs",
				fn: func(users []string) error {
					return nil
				},
				expectErrToContain: []string{"ForEachChunk", "must a slice of structs"},
			},
		}

		for _, test := range tests {
			t.Run(test.desc, func(t *testing.T) {
				_, err := structs.ParseInputFunc(test.fn)
				tt.AssertErrContains(t, err, test.expectErrToContain...)
			})
		}
	})
}
