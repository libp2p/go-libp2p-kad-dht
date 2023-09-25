package errs

import "fmt"

// A ConfigurationError is returned when a component's configuration is found to be invalid or unusable.
type ConfigurationError struct {
	Component string
	Err       error
}

var _ error = (*ConfigurationError)(nil)

func (e *ConfigurationError) Error() string {
	if e.Err == nil {
		return fmt.Sprintf("configuration error: %s", e.Component)
	}
	return fmt.Sprintf("configuration error: %s: %s", e.Component, e.Err.Error())
}

func (e *ConfigurationError) Unwrap() error {
	return e.Err
}
