// Generated SBE (Simple Binary Encoding) message codec

package codecs

import (
	"fmt"
	"io"
	"io/ioutil"
	"math"
)

type ArchiveIdRequest struct {
	ControlSessionId int64
	CorrelationId    int64
}

func (a *ArchiveIdRequest) Encode(_m *SbeGoMarshaller, _w io.Writer, doRangeCheck bool) error {
	if doRangeCheck {
		if err := a.RangeCheck(a.SbeSchemaVersion(), a.SbeSchemaVersion()); err != nil {
			return err
		}
	}
	if err := _m.WriteInt64(_w, a.ControlSessionId); err != nil {
		return err
	}
	if err := _m.WriteInt64(_w, a.CorrelationId); err != nil {
		return err
	}
	return nil
}

func (a *ArchiveIdRequest) Decode(_m *SbeGoMarshaller, _r io.Reader, actingVersion uint16, blockLength uint16, doRangeCheck bool) error {
	if !a.ControlSessionIdInActingVersion(actingVersion) {
		a.ControlSessionId = a.ControlSessionIdNullValue()
	} else {
		if err := _m.ReadInt64(_r, &a.ControlSessionId); err != nil {
			return err
		}
	}
	if !a.CorrelationIdInActingVersion(actingVersion) {
		a.CorrelationId = a.CorrelationIdNullValue()
	} else {
		if err := _m.ReadInt64(_r, &a.CorrelationId); err != nil {
			return err
		}
	}
	if actingVersion > a.SbeSchemaVersion() && blockLength > a.SbeBlockLength() {
		io.CopyN(ioutil.Discard, _r, int64(blockLength-a.SbeBlockLength()))
	}
	if doRangeCheck {
		if err := a.RangeCheck(actingVersion, a.SbeSchemaVersion()); err != nil {
			return err
		}
	}
	return nil
}

func (a *ArchiveIdRequest) RangeCheck(actingVersion uint16, schemaVersion uint16) error {
	if a.ControlSessionIdInActingVersion(actingVersion) {
		if a.ControlSessionId < a.ControlSessionIdMinValue() || a.ControlSessionId > a.ControlSessionIdMaxValue() {
			return fmt.Errorf("Range check failed on a.ControlSessionId (%v < %v > %v)", a.ControlSessionIdMinValue(), a.ControlSessionId, a.ControlSessionIdMaxValue())
		}
	}
	if a.CorrelationIdInActingVersion(actingVersion) {
		if a.CorrelationId < a.CorrelationIdMinValue() || a.CorrelationId > a.CorrelationIdMaxValue() {
			return fmt.Errorf("Range check failed on a.CorrelationId (%v < %v > %v)", a.CorrelationIdMinValue(), a.CorrelationId, a.CorrelationIdMaxValue())
		}
	}
	return nil
}

func ArchiveIdRequestInit(a *ArchiveIdRequest) {
	return
}

func (*ArchiveIdRequest) SbeBlockLength() (blockLength uint16) {
	return 16
}

func (*ArchiveIdRequest) SbeTemplateId() (templateId uint16) {
	return 68
}

func (*ArchiveIdRequest) SbeSchemaId() (schemaId uint16) {
	return 101
}

func (*ArchiveIdRequest) SbeSchemaVersion() (schemaVersion uint16) {
	return 9
}

func (*ArchiveIdRequest) SbeSemanticType() (semanticType []byte) {
	return []byte("")
}

func (*ArchiveIdRequest) SbeSemanticVersion() (semanticVersion string) {
	return "5.2"
}

func (*ArchiveIdRequest) ControlSessionIdId() uint16 {
	return 1
}

func (*ArchiveIdRequest) ControlSessionIdSinceVersion() uint16 {
	return 0
}

func (a *ArchiveIdRequest) ControlSessionIdInActingVersion(actingVersion uint16) bool {
	return actingVersion >= a.ControlSessionIdSinceVersion()
}

func (*ArchiveIdRequest) ControlSessionIdDeprecated() uint16 {
	return 0
}

func (*ArchiveIdRequest) ControlSessionIdMetaAttribute(meta int) string {
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

func (*ArchiveIdRequest) ControlSessionIdMinValue() int64 {
	return math.MinInt64 + 1
}

func (*ArchiveIdRequest) ControlSessionIdMaxValue() int64 {
	return math.MaxInt64
}

func (*ArchiveIdRequest) ControlSessionIdNullValue() int64 {
	return math.MinInt64
}

func (*ArchiveIdRequest) CorrelationIdId() uint16 {
	return 2
}

func (*ArchiveIdRequest) CorrelationIdSinceVersion() uint16 {
	return 0
}

func (a *ArchiveIdRequest) CorrelationIdInActingVersion(actingVersion uint16) bool {
	return actingVersion >= a.CorrelationIdSinceVersion()
}

func (*ArchiveIdRequest) CorrelationIdDeprecated() uint16 {
	return 0
}

func (*ArchiveIdRequest) CorrelationIdMetaAttribute(meta int) string {
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

func (*ArchiveIdRequest) CorrelationIdMinValue() int64 {
	return math.MinInt64 + 1
}

func (*ArchiveIdRequest) CorrelationIdMaxValue() int64 {
	return math.MaxInt64
}

func (*ArchiveIdRequest) CorrelationIdNullValue() int64 {
	return math.MinInt64
}
