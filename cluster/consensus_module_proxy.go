package cluster

import (
	"fmt"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/cluster/codecs"
)

const (
	scheduleTimerBlockLength = 16
	cancelTimerBlockLength   = 8
)

// Proxy class for encapsulating encoding and sending of control protocol messages to a cluster
type consensusModuleProxy struct {
	marshaller    *codecs.SbeGoMarshaller // currently shared as we're not reentrant (but could be here)
	rangeChecking bool
	publication   *aeron.Publication
	buffer        *atomic.Buffer
}

func newConsensusModuleProxy(
	options *Options,
	publication *aeron.Publication,
) *consensusModuleProxy {
	return &consensusModuleProxy{
		marshaller:    codecs.NewSbeGoMarshaller(),
		rangeChecking: options.RangeChecking,
		publication:   publication,
		buffer:        atomic.MakeBuffer(make([]byte, 500)),
	}
}

// From here we have all the functions that create a data packet and send it on the
// publication. Responses will be processed on the control

// ConnectRequest packet and send
func (proxy *consensusModuleProxy) ackOffer(
	logPosition int64,
	timestamp int64,
	ackID int64,
	relevantID int64,
	serviceID int32,
) (bool, error) {
	// Create a packet and send it
	bytes, err := codecs.ServiceAckRequestPacket(
		proxy.marshaller,
		proxy.rangeChecking,
		logPosition,
		timestamp,
		ackID,
		relevantID,
		serviceID,
	)
	if err != nil {
		return false, err
	}

	buffer := atomic.MakeBuffer(bytes)
	return proxy.offerAndCheck(buffer, 0, buffer.Capacity())
}

func (proxy *consensusModuleProxy) closeSessionOffer(
	clusterSessionId int64,
) (bool, error) {
	// Create a packet and send it
	bytes, err := codecs.CloseSessionRequestPacket(
		proxy.marshaller,
		proxy.rangeChecking,
		clusterSessionId,
	)
	if err != nil {
		return false, err
	}
	buffer := atomic.MakeBuffer(bytes)
	return proxy.offerAndCheck(buffer, 0, buffer.Capacity())
}

func (proxy *consensusModuleProxy) scheduleTimerOffer(correlationId int64, deadline int64) (bool, error) {
	buf := proxy.initBuffer(scheduleTimerTemplateId, scheduleTimerBlockLength)
	buf.PutInt64(SBEHeaderLength, correlationId)
	buf.PutInt64(SBEHeaderLength+8, deadline)
	return proxy.offerAndCheck(buf, 0, SBEHeaderLength+scheduleTimerBlockLength)
}

func (proxy *consensusModuleProxy) cancelTimerOffer(correlationId int64) (bool, error) {
	buf := proxy.initBuffer(cancelTimerTemplateId, cancelTimerBlockLength)
	buf.PutInt64(SBEHeaderLength, correlationId)
	return proxy.offerAndCheck(buf, 0, SBEHeaderLength+cancelTimerBlockLength)
}

func (proxy *consensusModuleProxy) initBuffer(templateId uint16, blockLength uint16) *atomic.Buffer {
	buf := proxy.buffer
	buf.PutUInt16(0, blockLength)
	buf.PutUInt16(2, templateId)
	buf.PutUInt16(4, ClusterSchemaId)
	buf.PutUInt16(6, ClusterSchemaVersion)
	return buf
}

func (proxy *consensusModuleProxy) offer(buffer *atomic.Buffer, offset, length int32) int64 {
	result := proxy.publication.Offer(buffer, offset, length, nil)
	return result
}

func (proxy *consensusModuleProxy) offerAndCheck(buffer *atomic.Buffer, offset, length int32) (bool, error) {
	position := proxy.publication.Offer(buffer, offset, length, nil)
	// TODO: GoLang is >= or Java > ?
	if position > 0 {
		return true, nil
	}
	if err := checkResult(position, proxy.publication); err != nil {
		return false, err
	}
	return false, nil
}

func checkResult(position int64, publication *aeron.Publication) error {
	if aeron.NotConnected == position {
		return &ClusterError{"publication is not connected"}
	}

	if aeron.PublicationClosed == position {
		return &ClusterError{"publication is closed"}
	}

	if aeron.MaxPositionExceeded == position {
		return &ClusterError{fmt.Sprintf("publication at max position: term-length=%d", publication.TermBufferLength())}
	}

	return nil
}

func (proxy *consensusModuleProxy) Offer2(
	bufferOne *atomic.Buffer, offsetOne int32, lengthOne int32,
	bufferTwo *atomic.Buffer, offsetTwo int32, lengthTwo int32,
) (int64, error) {
	result := proxy.publication.Offer2(bufferOne, offsetOne, lengthOne, bufferTwo, offsetTwo, lengthTwo, nil)
	if result < 0 {
		if err := checkResult(result, proxy.publication); err != nil {
			return NullValue, err
		}
	}
	return result, nil
}
