package tt

// PanicHandler will run the input function and recover
// from any panics it might generate.
//
// It will then save the panic payload and return it
// so it can be asserted by other functions on the test.
func PanicHandler(fn func()) (panicPayload interface{}) {
	defer func() {
		// Overwrites the panic payload if a pannic actually occurs:
		if r := recover(); r != nil {
			panicPayload = r
		}
	}()

	fn()
	return nil
}
