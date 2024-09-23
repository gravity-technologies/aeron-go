package codecs

import "bytes"

func ArchiveIdPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64) ([]byte, error) {
	var request ArchiveIdRequest
	request.ControlSessionId = controlSessionId
	request.CorrelationId = correlationId

	// Marshal it
	header := MessageHeader{BlockLength: request.SbeBlockLength(), TemplateId: request.SbeTemplateId(), SchemaId: request.SbeSchemaId(), Version: request.SbeSchemaVersion()}
	buffer := new(bytes.Buffer)
	if err := header.Encode(marshaller, buffer); err != nil {
		return nil, err
	}
	if err := request.Encode(marshaller, buffer, rangeChecking); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func MaxRecordedPositionPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64, recordingId int64) ([]byte, error) {
	var request PurgeRecordingRequest
	request.ControlSessionId = controlSessionId
	request.CorrelationId = correlationId
	request.RecordingId = recordingId

	// Marshal it
	header := MessageHeader{BlockLength: request.SbeBlockLength(), TemplateId: request.SbeTemplateId(), SchemaId: request.SbeSchemaId(), Version: request.SbeSchemaVersion()}
	buffer := new(bytes.Buffer)
	if err := header.Encode(marshaller, buffer); err != nil {
		return nil, err
	}
	if err := request.Encode(marshaller, buffer, rangeChecking); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}
