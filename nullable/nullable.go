package nullable

// Int ...
func Int(i int) *int {
	return &i
}

// Float64 ...
func Float64(f float64) *float64 {
	return &f
}

// String ...
func String(s string) *string {
	return &s
}
