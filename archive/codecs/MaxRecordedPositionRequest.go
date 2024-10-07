// Generated SBE (Simple Binary Encoding) message codec

package codecs

import (
	"fmt"
	"io"
	"io/ioutil"
	"math"
)

type MaxRecordedPositionRequest struct {
	ControlSessionId int64
	CorrelationId    int64
	RecordingId      int64
}

func (m *MaxRecordedPositionRequest) Encode(_m *SbeGoMarshaller, _w io.Writer, doRangeCheck bool) error {
	if doRangeCheck {
		if err := m.RangeCheck(m.SbeSchemaVersion(), m.SbeSchemaVersion()); err != nil {
			return err
		}
	}
	if err := _m.WriteInt64(_w, m.ControlSessionId); err != nil {
		return err
	}
	if err := _m.WriteInt64(_w, m.CorrelationId); err != nil {
		return err
	}
	if err := _m.WriteInt64(_w, m.RecordingId); err != nil {
		return err
	}
	return nil
}

func (m *MaxRecordedPositionRequest) Decode(_m *SbeGoMarshaller, _r io.Reader, actingVersion uint16, blockLength uint16, doRangeCheck bool) error {
	if !m.ControlSessionIdInActingVersion(actingVersion) {
		m.ControlSessionId = m.ControlSessionIdNullValue()
	} else {
		if err := _m.ReadInt64(_r, &m.ControlSessionId); err != nil {
			return err
		}
	}
	if !m.CorrelationIdInActingVersion(actingVersion) {
		m.CorrelationId = m.CorrelationIdNullValue()
	} else {
		if err := _m.ReadInt64(_r, &m.CorrelationId); err != nil {
			return err
		}
	}
	if !m.RecordingIdInActingVersion(actingVersion) {
		m.RecordingId = m.RecordingIdNullValue()
	} else {
		if err := _m.ReadInt64(_r, &m.RecordingId); err != nil {
			return err
		}
	}
	if actingVersion > m.SbeSchemaVersion() && blockLength > m.SbeBlockLength() {
		io.CopyN(ioutil.Discard, _r, int64(blockLength-m.SbeBlockLength()))
	}
	if doRangeCheck {
		if err := m.RangeCheck(actingVersion, m.SbeSchemaVersion()); err != nil {
			return err
		}
	}
	return nil
}

func (m *MaxRecordedPositionRequest) RangeCheck(actingVersion uint16, schemaVersion uint16) error {
	if m.ControlSessionIdInActingVersion(actingVersion) {
		if m.ControlSessionId < m.ControlSessionIdMinValue() || m.ControlSessionId > m.ControlSessionIdMaxValue() {
			return fmt.Errorf("Range check failed on m.ControlSessionId (%v < %v > %v)", m.ControlSessionIdMinValue(), m.ControlSessionId, m.ControlSessionIdMaxValue())
		}
	}
	if m.CorrelationIdInActingVersion(actingVersion) {
		if m.CorrelationId < m.CorrelationIdMinValue() || m.CorrelationId > m.CorrelationIdMaxValue() {
			return fmt.Errorf("Range check failed on m.CorrelationId (%v < %v > %v)", m.CorrelationIdMinValue(), m.CorrelationId, m.CorrelationIdMaxValue())
		}
	}
	if m.RecordingIdInActingVersion(actingVersion) {
		if m.RecordingId < m.RecordingIdMinValue() || m.RecordingId > m.RecordingIdMaxValue() {
			return fmt.Errorf("Range check failed on m.RecordingId (%v < %v > %v)", m.RecordingIdMinValue(), m.RecordingId, m.RecordingIdMaxValue())
		}
	}
	return nil
}

func MaxRecordedPositionRequestInit(m *MaxRecordedPositionRequest) {
	return
}

func (*MaxRecordedPositionRequest) SbeBlockLength() (blockLength uint16) {
	return 24
}

func (*MaxRecordedPositionRequest) SbeTemplateId() (templateId uint16) {
	return 67
}

func (*MaxRecordedPositionRequest) SbeSchemaId() (schemaId uint16) {
	return 101
}

func (*MaxRecordedPositionRequest) SbeSchemaVersion() (schemaVersion uint16) {
	return 9
}

func (*MaxRecordedPositionRequest) SbeSemanticType() (semanticType []byte) {
	return []byte("")
}

func (*MaxRecordedPositionRequest) SbeSemanticVersion() (semanticVersion string) {
	return "5.2"
}

func (*MaxRecordedPositionRequest) ControlSessionIdId() uint16 {
	return 1
}

func (*MaxRecordedPositionRequest) ControlSessionIdSinceVersion() uint16 {
	return 0
}

func (m *MaxRecordedPositionRequest) ControlSessionIdInActingVersion(actingVersion uint16) bool {
	return actingVersion >= m.ControlSessionIdSinceVersion()
}

func (*MaxRecordedPositionRequest) ControlSessionIdDeprecated() uint16 {
	return 0
}

func (*MaxRecordedPositionRequest) ControlSessionIdMetaAttribute(meta int) string {
	switch meta {
	case 1:
		return ""
	case 2:
		return ""
	case 3:
		return ""
	case 4:
		return "required"
	}
	return ""
}

func (*MaxRecordedPositionRequest) ControlSessionIdMinValue() int64 {
	return math.MinInt64 + 1
}

func (*MaxRecordedPositionRequest) ControlSessionIdMaxValue() int64 {
	return math.MaxInt64
}

func (*MaxRecordedPositionRequest) ControlSessionIdNullValue() int64 {
	return math.MinInt64
}

func (*MaxRecordedPositionRequest) CorrelationIdId() uint16 {
	return 2
}

func (*MaxRecordedPositionRequest) CorrelationIdSinceVersion() uint16 {
	return 0
}

func (m *MaxRecordedPositionRequest) CorrelationIdInActingVersion(actingVersion uint16) bool {
	return actingVersion >= m.CorrelationIdSinceVersion()
}

func (*MaxRecordedPositionRequest) CorrelationIdDeprecated() uint16 {
	return 0
}

func (*MaxRecordedPositionRequest) CorrelationIdMetaAttribute(meta int) string {
	switch meta {
	case 1:
		return ""
	case 2:
		return ""
	case 3:
		return ""
	case 4:
		return "required"
	}
	return ""
}

func (*MaxRecordedPositionRequest) CorrelationIdMinValue() int64 {
	return math.MinInt64 + 1
}

func (*MaxRecordedPositionRequest) CorrelationIdMaxValue() int64 {
	return math.MaxInt64
}

func (*MaxRecordedPositionRequest) CorrelationIdNullValue() int64 {
	return math.MinInt64
}

func (*MaxRecordedPositionRequest) RecordingIdId() uint16 {
	return 3
}

func (*MaxRecordedPositionRequest) RecordingIdSinceVersion() uint16 {
	return 0
}

func (m *MaxRecordedPositionRequest) RecordingIdInActingVersion(actingVersion uint16) bool {
	return actingVersion >= m.RecordingIdSinceVersion()
}

func (*MaxRecordedPositionRequest) RecordingIdDeprecated() uint16 {
	return 0
}

func (*MaxRecordedPositionRequest) RecordingIdMetaAttribute(meta int) string {
	switch meta {
	case 1:
		return ""
	case 2:
		return ""
	case 3:
		return ""
	case 4:
		return "required"
	}
	return ""
}

func (*MaxRecordedPositionRequest) RecordingIdMinValue() int64 {
	return math.MinInt64 + 1
}

func (*MaxRecordedPositionRequest) RecordingIdMaxValue() int64 {
	return math.MaxInt64
}

func (*MaxRecordedPositionRequest) RecordingIdNullValue() int64 {
	return math.MinInt64
}
