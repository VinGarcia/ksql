package modifiers

import (
	"testing"

	tt "github.com/vingarcia/ksql/internal/testtools"
	"github.com/vingarcia/ksql/ksqlmodifiers"
)

func TestRegisterAttrModifier(t *testing.T) {
	t.Run("should register new modifiers correctly", func(t *testing.T) {
		modifier1 := ksqlmodifiers.AttrModifier{
			SkipOnUpdate: true,
		}
		modifier2 := ksqlmodifiers.AttrModifier{
			SkipOnInsert: true,
		}

		RegisterAttrModifier("fakeModifierName1", modifier1)
		RegisterAttrModifier("fakeModifierName2", modifier2)

		mod, err := LoadGlobalModifier("fakeModifierName1")
		tt.AssertNoErr(t, err)
		tt.AssertEqual(t, mod, modifier1)

		mod, err = LoadGlobalModifier("fakeModifierName2")
		tt.AssertNoErr(t, err)
		tt.AssertEqual(t, mod, modifier2)
	})

	t.Run("should panic registering a modifier and the name already exists", func(t *testing.T) {
		modifier1 := ksqlmodifiers.AttrModifier{
			SkipOnUpdate: true,
		}
		modifier2 := ksqlmodifiers.AttrModifier{
			SkipOnInsert: true,
		}

		RegisterAttrModifier("fakeModifierName", modifier1)
		panicPayload := tt.PanicHandler(func() {
			RegisterAttrModifier("fakeModifierName", modifier2)
		})

		err, ok := panicPayload.(error)
		tt.AssertEqual(t, ok, true)
		tt.AssertErrContains(t, err, "KSQL", "fakeModifierName", "name is already in use")
	})

	t.Run("should return an error when loading an inexistent modifier", func(t *testing.T) {
		mod, err := LoadGlobalModifier("nonExistentModifier")
		tt.AssertErrContains(t, err, "nonExistentModifier")
		tt.AssertEqual(t, mod, ksqlmodifiers.AttrModifier{})
	})
}
