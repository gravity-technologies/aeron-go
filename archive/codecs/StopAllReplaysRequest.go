// Generated SBE (Simple Binary Encoding) message codec

package codecs

import (
	"fmt"
	"io"
	"io/ioutil"
	"math"
)

type StopAllReplaysRequest struct {
	ControlSessionId int64
	CorrelationId    int64
	RecordingId      int64
}

func (s *StopAllReplaysRequest) Encode(_m *SbeGoMarshaller, _w io.Writer, doRangeCheck bool) error {
	if doRangeCheck {
		if err := s.RangeCheck(s.SbeSchemaVersion(), s.SbeSchemaVersion()); err != nil {
			return err
		}
	}
	if err := _m.WriteInt64(_w, s.ControlSessionId); err != nil {
		return err
	}
	if err := _m.WriteInt64(_w, s.CorrelationId); err != nil {
		return err
	}
	if err := _m.WriteInt64(_w, s.RecordingId); err != nil {
		return err
	}
	return nil
}

func (s *StopAllReplaysRequest) Decode(_m *SbeGoMarshaller, _r io.Reader, actingVersion uint16, blockLength uint16, doRangeCheck bool) error {
	if !s.ControlSessionIdInActingVersion(actingVersion) {
		s.ControlSessionId = s.ControlSessionIdNullValue()
	} else {
		if err := _m.ReadInt64(_r, &s.ControlSessionId); err != nil {
			return err
		}
	}
	if !s.CorrelationIdInActingVersion(actingVersion) {
		s.CorrelationId = s.CorrelationIdNullValue()
	} else {
		if err := _m.ReadInt64(_r, &s.CorrelationId); err != nil {
			return err
		}
	}
	if !s.RecordingIdInActingVersion(actingVersion) {
		s.RecordingId = s.RecordingIdNullValue()
	} else {
		if err := _m.ReadInt64(_r, &s.RecordingId); err != nil {
			return err
		}
	}
	if actingVersion > s.SbeSchemaVersion() && blockLength > s.SbeBlockLength() {
		io.CopyN(ioutil.Discard, _r, int64(blockLength-s.SbeBlockLength()))
	}
	if doRangeCheck {
		if err := s.RangeCheck(actingVersion, s.SbeSchemaVersion()); err != nil {
			return err
		}
	}
	return nil
}

func (s *StopAllReplaysRequest) RangeCheck(actingVersion uint16, schemaVersion uint16) error {
	if s.ControlSessionIdInActingVersion(actingVersion) {
		if s.ControlSessionId < s.ControlSessionIdMinValue() || s.ControlSessionId > s.ControlSessionIdMaxValue() {
			return fmt.Errorf("Range check failed on s.ControlSessionId (%v < %v > %v)", s.ControlSessionIdMinValue(), s.ControlSessionId, s.ControlSessionIdMaxValue())
		}
	}
	if s.CorrelationIdInActingVersion(actingVersion) {
		if s.CorrelationId < s.CorrelationIdMinValue() || s.CorrelationId > s.CorrelationIdMaxValue() {
			return fmt.Errorf("Range check failed on s.CorrelationId (%v < %v > %v)", s.CorrelationIdMinValue(), s.CorrelationId, s.CorrelationIdMaxValue())
		}
	}
	if s.RecordingIdInActingVersion(actingVersion) {
		if s.RecordingId < s.RecordingIdMinValue() || s.RecordingId > s.RecordingIdMaxValue() {
			return fmt.Errorf("Range check failed on s.RecordingId (%v < %v > %v)", s.RecordingIdMinValue(), s.RecordingId, s.RecordingIdMaxValue())
		}
	}
	return nil
}

func StopAllReplaysRequestInit(s *StopAllReplaysRequest) {
	return
}

func (*StopAllReplaysRequest) SbeBlockLength() (blockLength uint16) {
	return 24
}

func (*StopAllReplaysRequest) SbeTemplateId() (templateId uint16) {
	return 19
}

func (*StopAllReplaysRequest) SbeSchemaId() (schemaId uint16) {
	return 101
}

func (*StopAllReplaysRequest) SbeSchemaVersion() (schemaVersion uint16) {
	return 9
}

func (*StopAllReplaysRequest) SbeSemanticType() (semanticType []byte) {
	return []byte("")
}

func (*StopAllReplaysRequest) SbeSemanticVersion() (semanticVersion string) {
	return "5.2"
}

func (*StopAllReplaysRequest) ControlSessionIdId() uint16 {
	return 1
}

func (*StopAllReplaysRequest) ControlSessionIdSinceVersion() uint16 {
	return 0
}

func (s *StopAllReplaysRequest) ControlSessionIdInActingVersion(actingVersion uint16) bool {
	return actingVersion >= s.ControlSessionIdSinceVersion()
}

func (*StopAllReplaysRequest) ControlSessionIdDeprecated() uint16 {
	return 0
}

func (*StopAllReplaysRequest) ControlSessionIdMetaAttribute(meta int) string {
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

func (*StopAllReplaysRequest) ControlSessionIdMinValue() int64 {
	return math.MinInt64 + 1
}

func (*StopAllReplaysRequest) ControlSessionIdMaxValue() int64 {
	return math.MaxInt64
}

func (*StopAllReplaysRequest) ControlSessionIdNullValue() int64 {
	return math.MinInt64
}

func (*StopAllReplaysRequest) CorrelationIdId() uint16 {
	return 2
}

func (*StopAllReplaysRequest) CorrelationIdSinceVersion() uint16 {
	return 0
}

func (s *StopAllReplaysRequest) CorrelationIdInActingVersion(actingVersion uint16) bool {
	return actingVersion >= s.CorrelationIdSinceVersion()
}

func (*StopAllReplaysRequest) CorrelationIdDeprecated() uint16 {
	return 0
}

func (*StopAllReplaysRequest) CorrelationIdMetaAttribute(meta int) string {
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

func (*StopAllReplaysRequest) CorrelationIdMinValue() int64 {
	return math.MinInt64 + 1
}

func (*StopAllReplaysRequest) CorrelationIdMaxValue() int64 {
	return math.MaxInt64
}

func (*StopAllReplaysRequest) CorrelationIdNullValue() int64 {
	return math.MinInt64
}

func (*StopAllReplaysRequest) RecordingIdId() uint16 {
	return 3
}

func (*StopAllReplaysRequest) RecordingIdSinceVersion() uint16 {
	return 0
}

func (s *StopAllReplaysRequest) RecordingIdInActingVersion(actingVersion uint16) bool {
	return actingVersion >= s.RecordingIdSinceVersion()
}

func (*StopAllReplaysRequest) RecordingIdDeprecated() uint16 {
	return 0
}

func (*StopAllReplaysRequest) RecordingIdMetaAttribute(meta int) string {
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

func (*StopAllReplaysRequest) RecordingIdMinValue() int64 {
	return math.MinInt64 + 1
}

func (*StopAllReplaysRequest) RecordingIdMaxValue() int64 {
	return math.MaxInt64
}

func (*StopAllReplaysRequest) RecordingIdNullValue() int64 {
	return math.MinInt64
}
