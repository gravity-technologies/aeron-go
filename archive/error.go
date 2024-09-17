package archive

import (
	"fmt"
)

// Error code is from poller.relevantId()
// https://github.com/real-logic/aeron/blob/1.46.2/aeron-archive/src/main/java/io/aeron/archive/client/AeronArchive.java#L511

// The archive storage is at minimum threshold or exhausted.
const ExceptionSpaceStorage int = 11

type ArchiveError struct {
	CorrelationID int64
	ErrorCode     int
	Message       string
}

func NewArchiveError(correlationID int64, code int, message string) *ArchiveError {
	return &ArchiveError{
		CorrelationID: correlationID,
		ErrorCode:     code,
		Message:       message,
	}
}

func (ae *ArchiveError) Error() string {
	return fmt.Sprintf("archive error: %s", ae.Message)
}
