package kbuilder_test

import (
	"testing"

	"github.com/tj/assert"
	"github.com/vingarcia/ksql/kbuilder"
)

type User struct {
	Name string `ksql:"name"`
	Age  string `ksql:"age"`
}

func TestBuilder(t *testing.T) {
	t.Run("should build queries correctly", func(t *testing.T) {
		b, err := kbuilder.New("postgres")
		assert.Equal(t, nil, err)

		var user User
		var nullableField *int
		query, params, err := b.Build(kbuilder.Query{
			Select: &user,
			From:   "users",
			Where: kbuilder.
				Where("foo < %s", 42).
				Where("bar LIKE %s", "%ending").
				WhereIf("foobar = %s", nullableField),

			OrderBy: kbuilder.OrderBy("id").Desc(),
			Offset:  100,
			Limit:   10,
		})

		assert.Equal(t, nil, err)
		assert.Equal(t, `SELECT "name", "age" FROM users WHERE foo < $1 AND bar LIKE $2 ORDER BY id DESC LIMIT 10 OFFSET 100`, query)
		assert.Equal(t, []interface{}{42, "%ending"}, params)
	})
}
