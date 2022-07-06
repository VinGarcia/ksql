package kstructs

import (
	"fmt"
	"testing"

	"github.com/ditointernet/go-assert"
	tt "github.com/vingarcia/ksql/internal/testtools"
	"github.com/vingarcia/ksql/nullable"
)

func TestStructToMap(t *testing.T) {
	type S1 struct {
		Name string `ksql:"name_attr"`
		Age  int    `ksql:"age_attr"`
	}
	t.Run("should convert plain structs to maps", func(t *testing.T) {
		m, err := StructToMap(S1{
			Name: "my name",
			Age:  22,
		})

		tt.AssertEqual(t, err, nil)
		tt.AssertEqual(t, m, map[string]interface{}{
			"name_attr": "my name",
			"age_attr":  22,
		})
	})

	t.Run("should not ignore zero value attrs, if they are not pointers", func(t *testing.T) {
		m, err := StructToMap(S1{
			Name: "",
			Age:  0,
		})

		tt.AssertEqual(t, err, nil)
		tt.AssertEqual(t, m, map[string]interface{}{
			"name_attr": "",
			"age_attr":  0,
		})
	})

	type S2 struct {
		Name *string `ksql:"name"`
		Age  *int    `ksql:"age"`
	}

	t.Run("should not ignore not nil pointers", func(t *testing.T) {
		str := ""
		age := 0
		m, err := StructToMap(S2{
			Name: &str,
			Age:  &age,
		})

		tt.AssertEqual(t, err, nil)
		tt.AssertEqual(t, m, map[string]interface{}{
			"name": "",
			"age":  0,
		})
	})

	t.Run("should ignore nil pointers", func(t *testing.T) {
		m, err := StructToMap(S2{
			Name: nil,
			Age:  nil,
		})

		tt.AssertEqual(t, err, nil)
		tt.AssertEqual(t, m, map[string]interface{}{})
	})

	t.Run("should ignore fields not tagged with ksql", func(t *testing.T) {
		m, err := StructToMap(struct {
			Name              string `ksql:"name_attr"`
			Age               int    `ksql:"age_attr"`
			NotPartOfTheQuery int
		}{
			Name:              "fake-name",
			Age:               42,
			NotPartOfTheQuery: 42,
		})

		tt.AssertEqual(t, err, nil)
		tt.AssertEqual(t, m, map[string]interface{}{
			"name_attr": "fake-name",
			"age_attr":  42,
		})
	})

	t.Run("should return error for duplicated ksql tag names", func(t *testing.T) {
		_, err := StructToMap(struct {
			Name           string `ksql:"name_attr"`
			DuplicatedName string `ksql:"name_attr"`
			Age            int    `ksql:"age_attr"`
		}{
			Name:           "fake-name",
			Age:            42,
			DuplicatedName: "fake-duplicated-name",
		})

		assert.NotEqual(t, nil, err)
	})

	t.Run("should return error for structs with no ksql tags", func(t *testing.T) {
		_, err := StructToMap(struct {
			Name string
			Age  int `json:"age"`
		}{
			Name: "fake-name",
			Age:  42,
		})

		assert.NotEqual(t, nil, err)
	})
}

func TestFillStructWith(t *testing.T) {
	t.Run("should fill a struct correctly", func(t *testing.T) {
		var user struct {
			Name string `ksql:"name"`
			Age  int    `ksql:"age"`
		}
		err := FillStructWith(&user, map[string]interface{}{
			"name": "Breno",
			"age":  22,
		})

		tt.AssertEqual(t, err, nil)
		tt.AssertEqual(t, user.Name, "Breno")
		tt.AssertEqual(t, user.Age, 22)
	})

	t.Run("should fill ptr fields with ptr values", func(t *testing.T) {
		var user struct {
			Name *string `ksql:"name"`
			Age  *int    `ksql:"age"`
		}
		err := FillStructWith(&user, map[string]interface{}{
			"name": nullable.String("Breno"),
			"age":  nullable.Int(22),
		})

		tt.AssertEqual(t, err, nil)
		tt.AssertEqual(t, user.Name, nullable.String("Breno"))
		tt.AssertEqual(t, user.Age, nullable.Int(22))
	})

	t.Run("should fill ptr fields with non-ptr values", func(t *testing.T) {
		var user struct {
			Name *string `ksql:"name"`
			Age  *int    `ksql:"age"`
		}
		err := FillStructWith(&user, map[string]interface{}{
			"name": "Breno",
			"age":  22,
		})

		tt.AssertEqual(t, err, nil)
		tt.AssertEqual(t, user.Name, nullable.String("Breno"))
		tt.AssertEqual(t, user.Age, nullable.Int(22))
	})

	t.Run("should fill non ptr fields with ptr values", func(t *testing.T) {
		var user struct {
			Name string `ksql:"name"`
			Age  int    `ksql:"age"`
		}
		err := FillStructWith(&user, map[string]interface{}{
			"name": nullable.String("Breno"),
			"age":  nullable.Int(22),
		})

		tt.AssertEqual(t, err, nil)
		tt.AssertEqual(t, user.Name, "Breno")
		tt.AssertEqual(t, user.Age, 22)
	})

	t.Run("should fill ptr fields with nil when necessary", func(t *testing.T) {
		var user struct {
			Name *string `ksql:"name"`
			Age  *int    `ksql:"age"`
		}
		err := FillStructWith(&user, map[string]interface{}{
			"name": nil,
			"age":  nil,
		})

		tt.AssertEqual(t, err, nil)
		tt.AssertEqual(t, user.Name, (*string)(nil))
		tt.AssertEqual(t, user.Age, (*int)(nil))
	})

	t.Run("should interpret nil fields as zero values when necessary", func(t *testing.T) {
		var user struct {
			Name string `ksql:"name"`
			Age  int    `ksql:"age"`
		}
		user.Name = "not empty"
		user.Age = 42

		err := FillStructWith(&user, map[string]interface{}{
			"name": nil,
			"age":  nil,
		})

		tt.AssertEqual(t, err, nil)
		tt.AssertEqual(t, user.Name, "")
		tt.AssertEqual(t, user.Age, 0)
	})

	t.Run("should ignore extra or missing fields", func(t *testing.T) {
		var user struct {
			Name    string `ksql:"name"`
			Age     int    `ksql:"age"`
			Missing string `ksql:"missing"`
		}
		user.Missing = "should be untouched"

		err := FillStructWith(&user, map[string]interface{}{
			"name":        "fake name",
			"age":         42,
			"extra_field": "some value",
		})

		tt.AssertEqual(t, nil, err)
		tt.AssertEqual(t, "fake name", user.Name)
		tt.AssertEqual(t, 42, user.Age)
		tt.AssertEqual(t, "should be untouched", user.Missing)
	})

	t.Run("should report error if input is not a pointer", func(t *testing.T) {
		type User struct {
			Name    string `ksql:"name"`
			Age     int    `ksql:"age"`
			Missing string `ksql:"missing"`
		}
		var user User

		err := FillStructWith(user, map[string]interface{}{
			"name":        "fake name",
			"age":         42,
			"extra_field": "some value",
		})

		tt.AssertErrContains(t, err, "FillStructWith", "expected input to be a pointer", "User")
	})

	t.Run("should report error if input is not a pointer to struct", func(t *testing.T) {
		type User struct {
			Name    string `ksql:"name"`
			Age     int    `ksql:"age"`
			Missing string `ksql:"missing"`
		}
		var users []User

		err := FillStructWith(&users, map[string]interface{}{
			"name":        "fake name",
			"age":         42,
			"extra_field": "some value",
		})

		tt.AssertErrContains(t, err, "FillStructWith", "expected input to be a pointer to a struct", "User")
	})

	t.Run("should report error if input and target types are incompatible", func(t *testing.T) {
		type User struct {
			Age int `ksql:"age"`
		}
		var user User

		err := FillStructWith(&user, map[string]interface{}{
			"age": "not compatible with integer type",
		})

		tt.AssertErrContains(t, err, "FillStructWith", "age", "string", "int")
	})
}

func TestFillSliceWith(t *testing.T) {
	t.Run("should fill a list correctly", func(t *testing.T) {
		var users []struct {
			Name string `ksql:"name"`
			Age  int    `ksql:"age"`
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

		tt.AssertEqual(t, err, nil)
		tt.AssertEqual(t, len(users), 3)
		tt.AssertEqual(t, users[0].Name, "Jorge")
		tt.AssertEqual(t, users[1].Name, "Luciana")
		tt.AssertEqual(t, users[2].Name, "Breno")
	})

	t.Run("should report error if input is not a pointer", func(t *testing.T) {
		var users []struct {
			Name string `ksql:"name"`
			Age  int    `ksql:"age"`
		}
		err := FillSliceWith(users, []map[string]interface{}{{
			"name": "Jorge",
		}})

		tt.AssertErrContains(t, err, "FillSliceWith", "expected input to be a pointer")
	})

	t.Run("should report error if input is not a pointer to a slice", func(t *testing.T) {
		var user struct {
			Name string `ksql:"name"`
			Age  int    `ksql:"age"`
		}
		err := FillSliceWith(&user, []map[string]interface{}{{
			"name": "Jorge",
		}})

		tt.AssertErrContains(t, err, "FillSliceWith")
	})
}

func TestCallFunctionWithRows(t *testing.T) {
	t.Run("should call the function correctly", func(t *testing.T) {
		type User struct {
			Name string `ksql:"name"`
			Age  int    `ksql:"age"`
		}

		var inputUsers []User
		fn := func(users []User) error {
			inputUsers = users
			return nil
		}

		err := CallFunctionWithRows(fn, []map[string]interface{}{
			{
				"name": "fake-name1",
				"age":  42,
			},
			{
				"name": "fake-name2",
				"age":  43,
			},
		})
		tt.AssertNoErr(t, err)
		tt.AssertEqual(t, inputUsers, []User{
			{
				Name: "fake-name1",
				Age:  42,
			},
			{
				Name: "fake-name2",
				Age:  43,
			},
		})
	})

	t.Run("should forward errors correctly", func(t *testing.T) {
		type User struct {
			Name string `ksql:"name"`
			Age  int    `ksql:"age"`
		}

		fn := func(users []User) error {
			return fmt.Errorf("fake-error-msg")
		}

		err := CallFunctionWithRows(fn, []map[string]interface{}{{
			"name": "fake-name1",
			"age":  42,
		}})
		tt.AssertErrContains(t, err, "fake-error-msg")
	})

	t.Run("should report error if the input function is invalid", func(t *testing.T) {
		err := CallFunctionWithRows(func() {}, []map[string]interface{}{{
			"name": "fake-name1",
			"age":  42,
		}})
		tt.AssertErrContains(t, err)
	})
}
