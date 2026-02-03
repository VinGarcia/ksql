package tt

import "testing"

// NextResponse works like the [].shift() function on JS:
//
// it removes the first item from the slice and
// returns this element.
// If the slice is empty it will fail the test with an
// appropriate error message.
//
// For example if you have the slice `s := []int{1,2,3}` and
// calls `v := tt.NextResponse(t, &s)`, `s` will contain:
// `[]int{2,3}` and `v` will contain `1`.
//
// This function is meant to be used inside mocks when you
// need to specify a list of return values for sequential calls
// of the same mock function, for example if you want
// a mock function to only return an error on its third call:
//
//	sendErrors := []errors{nil, nil, fmt.Errorf("fakeErrMsg")}
//	myMock := MyMock{
//	    SendSomethingFn: func(something Something) error {
//	        return tt.NextResponse(t, &sendErrors)
//	    },
//	}
func NextResponse[T any](t *testing.T, resps *[]T) T {
	if len(*resps) == 0 {
		t.Fatalf("trying to get next response from an empty list")
	}

	resp := (*resps)[0]
	*resps = (*resps)[1:]
	return resp
}
