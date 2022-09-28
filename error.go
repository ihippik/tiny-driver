package tinydriver

// https://www.postgresql.org/docs/current/protocol-error-fields.html
type driverError struct {
	Severity string
	Code     string
	Message  string
}

func (d driverError) Error() string {
	return d.Message
}
