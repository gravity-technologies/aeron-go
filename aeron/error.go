package aeron

import (
	"fmt"
)

// Reference Impl:
// https://github.com/real-logic/aeron/blob/master/aeron-client/src/main/java/io/aeron/exceptions/AeronException.java

type AeronErrorCategoryEnum uint8
type AeronErrorCategoryValues struct {
	// Exception indicates a fatal condition. Recommendation is to terminate process immediately to avoid state corruption.
	FATAL AeronErrorCategoryEnum // 0
	//         Exception is an error. Corrective action is recommended if understood, otherwise treat as fatal.
	ERROR AeronErrorCategoryEnum // 1
	// Exception is a warning. Action has been, or will be, taken to handle the condition.
	//  Additional corrective action by the application may be needed.
	WARN AeronErrorCategoryEnum // 2
}

const _AeronErrorCategory_name = "FATALERRORWARN"

var _AeronErrorCategory_index = [...]uint8{0, 5, 10, 14}

func (category AeronErrorCategoryEnum) Name() string {
	return _AeronErrorCategory_name[_AeronErrorCategory_index[category]:_AeronErrorCategory_index[category+1]]
}

var AeronErrorCategory = AeronErrorCategoryValues{0, 1, 2}

type AeronError struct {
	Message  string
	Category AeronErrorCategoryEnum
}

func NewAeronError(message string, category AeronErrorCategoryEnum) *AeronError {
	return &AeronError{
		Message:  message,
		Category: category,
	}
}

func (ae *AeronError) Error() string {
	return fmt.Sprintf("aeron error: %s - %s", ae.Category.Name(), ae.Message)
}
