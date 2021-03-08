package structs

import (
	"testing"

	"github.com/ditointernet/go-assert"
	"github.com/vingarcia/kisssql/nullable"
)

func TestStructToMap(t *testing.T) {
	type S1 struct {
		Name string `kisssql:"name_attr"`
		Age  int    `kisssql:"age_attr"`
	}
	t.Run("should convert plain structs to maps", func(t *testing.T) {
		m, err := StructToMap(S1{
			Name: "my name",
			Age:  22,
		})

		assert.Equal(t, nil, err)
		assert.Equal(t, map[string]interface{}{
			"name_attr": "my name",
			"age_attr":  22,
		}, m)
	})

	t.Run("should not ignore zero value attrs, if they are not pointers", func(t *testing.T) {
		m, err := StructToMap(S1{
			Name: "",
			Age:  0,
		})

		assert.Equal(t, nil, err)
		assert.Equal(t, map[string]interface{}{
			"name_attr": "",
			"age_attr":  0,
		}, m)
	})

	type S2 struct {
		Name *string `kisssql:"name"`
		Age  *int    `kisssql:"age"`
	}

	t.Run("should not ignore not nil pointers", func(t *testing.T) {
		str := ""
		age := 0
		m, err := StructToMap(S2{
			Name: &str,
			Age:  &age,
		})

		assert.Equal(t, nil, err)
		assert.Equal(t, map[string]interface{}{
			"name": "",
			"age":  0,
		}, m)
	})

	t.Run("should ignore nil pointers", func(t *testing.T) {
		m, err := StructToMap(S2{
			Name: nil,
			Age:  nil,
		})

		assert.Equal(t, nil, err)
		assert.Equal(t, map[string]interface{}{}, m)
	})
}

func TestFillStructWith(t *testing.T) {
	t.Run("should fill a struct correctly", func(t *testing.T) {
		var user struct {
			Name string `kisssql:"name"`
			Age  int    `kisssql:"age"`
		}
		err := FillStructWith(&user, map[string]interface{}{
			"name": "Breno",
			"age":  22,
		})

		assert.Equal(t, nil, err)
		assert.Equal(t, "Breno", user.Name)
		assert.Equal(t, 22, user.Age)
	})

	t.Run("should fill ptr fields with ptr values", func(t *testing.T) {
		var user struct {
			Name *string `kisssql:"name"`
			Age  *int    `kisssql:"age"`
		}
		err := FillStructWith(&user, map[string]interface{}{
			"name": nullable.String("Breno"),
			"age":  nullable.Int(22),
		})

		assert.Equal(t, nil, err)
		assert.Equal(t, nullable.String("Breno"), user.Name)
		assert.Equal(t, nullable.Int(22), user.Age)
	})

	t.Run("should fill ptr fields with non-ptr values", func(t *testing.T) {
		var user struct {
			Name *string `kisssql:"name"`
			Age  *int    `kisssql:"age"`
		}
		err := FillStructWith(&user, map[string]interface{}{
			"name": "Breno",
			"age":  22,
		})

		assert.Equal(t, nil, err)
		assert.Equal(t, nullable.String("Breno"), user.Name)
		assert.Equal(t, nullable.Int(22), user.Age)
	})

	t.Run("should fill non ptr fields with ptr values", func(t *testing.T) {
		var user struct {
			Name string `kisssql:"name"`
			Age  int    `kisssql:"age"`
		}
		err := FillStructWith(&user, map[string]interface{}{
			"name": nullable.String("Breno"),
			"age":  nullable.Int(22),
		})

		assert.Equal(t, nil, err)
		assert.Equal(t, "Breno", user.Name)
		assert.Equal(t, 22, user.Age)
	})

	t.Run("should fill ptr fields with nil when necessary", func(t *testing.T) {
		var user struct {
			Name *string `kisssql:"name"`
			Age  *int    `kisssql:"age"`
		}
		err := FillStructWith(&user, map[string]interface{}{
			"name": nil,
			"age":  nil,
		})

		assert.Equal(t, nil, err)
		assert.Equal(t, (*string)(nil), user.Name)
		assert.Equal(t, (*int)(nil), user.Age)
	})

	t.Run("should interpret nil fields as zero values when necessary", func(t *testing.T) {
		var user struct {
			Name string `kisssql:"name"`
			Age  int    `kisssql:"age"`
		}
		user.Name = "not empty"
		user.Age = 42

		err := FillStructWith(&user, map[string]interface{}{
			"name": nil,
			"age":  nil,
		})

		assert.Equal(t, nil, err)
		assert.Equal(t, "", user.Name)
		assert.Equal(t, 0, user.Age)
	})

	t.Run("should ignore extra or missing fields", func(t *testing.T) {
		var user struct {
			Name    string `kisssql:"name"`
			Age     int    `kisssql:"age"`
			Missing string `kisssql:"missing"`
		}
		user.Missing = "should be untouched"

		err := FillStructWith(&user, map[string]interface{}{
			"name":        "fake name",
			"age":         42,
			"extra_field": "some value",
		})

		assert.Equal(t, nil, err)
		assert.Equal(t, "fake name", user.Name)
		assert.Equal(t, 42, user.Age)
		assert.Equal(t, "should be untouched", user.Missing)
	})
}

func TestFillSliceWith(t *testing.T) {
	t.Run("should fill a list correctly", func(t *testing.T) {
		var users []struct {
			Name string `kisssql:"name"`
			Age  int    `kisssql:"age"`
		}
		err := FillSliceWith(&users, []map[string]interface{}{
			{
				"name": "Jorge",
			},
			{
				"name": "Luciana",
			},
			{
				"name": "Breno",
			},
		})

		assert.Equal(t, nil, err)
		assert.Equal(t, 3, len(users))
		assert.Equal(t, "Jorge", users[0].Name)
		assert.Equal(t, "Luciana", users[1].Name)
		assert.Equal(t, "Breno", users[2].Name)
	})
}
