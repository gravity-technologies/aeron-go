package codecs

import (
	"bytes"

	"github.com/lirm/aeron-go/aeron/atomic"
)

// TODO: Codegen all these

func ScheduleTimerEncoder(
	marshaller *SbeGoMarshaller,
	rangeChecking bool,
	correlationId int64,
	deadline int64,
) ([]byte, error) {
	request := ScheduleTimer{
		CorrelationId: correlationId,
		Deadline:      deadline,
	}

	// Marshal it
	header := MessageHeader{
		BlockLength: request.SbeBlockLength(),
		TemplateId:  request.SbeTemplateId(),
		SchemaId:    request.SbeSchemaId(),
		Version:     request.SbeSchemaVersion(),
	}

	buffer := new(bytes.Buffer)
	if err := header.Encode(marshaller, buffer); err != nil {
		return nil, err
	}
	if err := request.Encode(marshaller, buffer, rangeChecking); err != nil {
		return nil, err
	}

	atomic.MakeBuffer(buffer.Bytes)

	return buffer.Bytes(), nil
}

func CancelTimerEncoder(
	marshaller *SbeGoMarshaller,
	rangeChecking bool,
	correlationId int64,
) ([]byte, error) {
	request := CancelTimer{
		CorrelationId: correlationId,
	}

	// Marshal it
	header := MessageHeader{
		BlockLength: request.SbeBlockLength(),
		TemplateId:  request.SbeTemplateId(),
		SchemaId:    request.SbeSchemaId(),
		Version:     request.SbeSchemaVersion(),
	}

	buffer := new(bytes.Buffer)
	if err := header.Encode(marshaller, buffer); err != nil {
		return nil, err
	}
	if err := request.Encode(marshaller, buffer, rangeChecking); err != nil {
		return nil, err
	}

	atomic.MakeBuffer(buffer.Bytes)

	return buffer.Bytes(), nil
}

func ClusterMembersQueryEncoder(
	marshaller *SbeGoMarshaller,
	rangeChecking bool,
	correlationId int64,
	extended BooleanTypeEnum,
) ([]byte, error) {
	request := ClusterMembersQuery{
		CorrelationId: correlationId,
		Extended:      extended,
	}

	// Marshal it
	header := MessageHeader{
		BlockLength: request.SbeBlockLength(),
		TemplateId:  request.SbeTemplateId(),
		SchemaId:    request.SbeSchemaId(),
		Version:     request.SbeSchemaVersion(),
	}

	buffer := new(bytes.Buffer)
	if err := header.Encode(marshaller, buffer); err != nil {
		return nil, err
	}
	if err := request.Encode(marshaller, buffer, rangeChecking); err != nil {
		return nil, err
	}

	atomic.MakeBuffer(buffer.Bytes)

	return buffer.Bytes(), nil
}
