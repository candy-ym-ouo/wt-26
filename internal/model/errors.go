package model

import (
	"errors"
	"net/http"
)

// Error is a typed application error that maps cleanly to an HTTP response.
type Error struct {
	Type    string
	Message string
	Status  int
}

func (e *Error) Error() string { return e.Message }

// BadRequest creates a validation or query parsing error.
func BadRequest(message string) error {
	return &Error{Type: "bad_request", Message: message, Status: http.StatusBadRequest}
}

// TooLarge creates a request-size or batch-size error.
func TooLarge(message string) error {
	return &Error{Type: "too_large", Message: message, Status: http.StatusRequestEntityTooLarge}
}

// Internal creates a safe public wrapper for an internal failure.
func Internal(message string) error {
	return &Error{Type: "internal_error", Message: message, Status: http.StatusInternalServerError}
}

// ErrorInfo returns response fields for any application error.
func ErrorInfo(err error) (string, string, int) {
	var appErr *Error
	if errors.As(err, &appErr) {
		return appErr.Type, appErr.Message, appErr.Status
	}
	return "internal_error", "internal server error", http.StatusInternalServerError
}
