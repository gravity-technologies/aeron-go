package cluster

import (
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/cluster/codecs"
)

// CodecIds stores static value from codegened SBE, so weo don't allocating object every time
type CodecIds struct {
	MessageHeader struct {
		EncodedLength int64
	}
	ServiceAck struct {
		BlockLength uint16
	}
	SessionMessageHeader struct {
		BlockLength uint16
	}
	CloseSession struct {
		BlockLength uint16
	}
	CancelTimer struct {
		BlockLength uint16
	}
	ScheduleTimer struct {
		BlockLength uint16
	}
	ClusterMembersQuery struct {
		BlockLength uint16
	}
}

var codecIds CodecIds

func init() {
	codecsMessageHeader := new(codecs.MessageHeader)
	codecsServiceAck := new(codecs.ServiceAck)
	codecsSessionMessageHeader := new(codecs.SessionMessageHeader)
	codecsCloseSession := new(codecs.CloseSession)
	codecsCancelTimer := new(codecs.CancelTimer)
	codecsScheduleTimer := new(codecs.ScheduleTimer)
	codecsClusterMembersQuery := new(codecs.ClusterMembersQuery)

	codecIds.MessageHeader.EncodedLength = codecsMessageHeader.EncodedLength()
	codecIds.ServiceAck.BlockLength = codecsServiceAck.SbeBlockLength()
	codecIds.SessionMessageHeader.BlockLength = codecsSessionMessageHeader.SbeBlockLength()
	codecIds.CloseSession.BlockLength = codecsCloseSession.SbeBlockLength()
	codecIds.CancelTimer.BlockLength = codecsCancelTimer.SbeBlockLength()
	codecIds.ScheduleTimer.BlockLength = codecsScheduleTimer.SbeBlockLength()
	codecIds.ClusterMembersQuery.BlockLength = codecsClusterMembersQuery.SbeBlockLength()
}

// -----------------------------------------------------------------------------
// Java compat implementation and using TryClaim for better performance
// https://github.com/real-logic/aeron/blob/release/1.46.x/aeron-cluster/src/main/java/io/aeron/cluster/service/ConsensusModuleProxy.java
// -----------------------------------------------------------------------------

// https://github.com/real-logic/aeron/blob/release/1.46.x/aeron-cluster/src/main/java/io/aeron/cluster/service/ConsensusModuleProxy.java#L146
func (proxy *consensusModuleProxy) ack(
	logPosition int64,
	timestamp int64,
	ackID int64,
	relevantID int64,
	serviceID int32,
) (bool, error) {
	length := int32(codecIds.MessageHeader.EncodedLength + int64(codecIds.ServiceAck.BlockLength))
	position := proxy.publication.TryClaim(length, &proxy.bufferClaim)
	if position > 0 {
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
			proxy.bufferClaim.Abort()
			return false, err
		}

		proxy.bufferClaim.Buffer().PutBytesArray(proxy.bufferClaim.Offset(), &bytes, 0, length)
		proxy.bufferClaim.Commit()
		return true, nil
	}

	if err := checkResult(position, proxy.publication); err != nil {
		proxy.bufferClaim.Abort()
		return false, err
	}
	return false, nil
}

func (proxy *consensusModuleProxy) closeSession(
	clusterSessionId int64,
) (bool, error) {
	length := int32(codecIds.MessageHeader.EncodedLength + int64(codecIds.CloseSession.BlockLength))
	position := proxy.publication.TryClaim(length, &proxy.bufferClaim)
	if position > 0 {
		bytes, err := codecs.CloseSessionRequestPacket(
			proxy.marshaller,
			proxy.rangeChecking,
			clusterSessionId,
		)
		if err != nil {
			proxy.bufferClaim.Abort()
			return false, err
		}

		proxy.bufferClaim.Buffer().PutBytesArray(proxy.bufferClaim.Offset(), &bytes, 0, length)
		proxy.bufferClaim.Commit()
		return true, nil
	}

	if err := checkResult(position, proxy.publication); err != nil {
		proxy.bufferClaim.Abort()
		return false, err
	}
	return false, nil
}

func (proxy *consensusModuleProxy) scheduleTimer(correlationId int64, deadline int64) (bool, error) {
	length := int32(codecIds.MessageHeader.EncodedLength + int64(codecIds.ScheduleTimer.BlockLength))
	position := proxy.publication.TryClaim(length, &proxy.bufferClaim)
	if position > 0 {
		bytes, err := codecs.ScheduleTimerEncoder(
			proxy.marshaller,
			proxy.rangeChecking,
			correlationId,
			deadline,
		)
		if err != nil {
			proxy.bufferClaim.Abort()
			return false, err
		}

		proxy.bufferClaim.Buffer().PutBytesArray(proxy.bufferClaim.Offset(), &bytes, 0, length)
		proxy.bufferClaim.Commit()
		return true, nil
	}

	if err := checkResult(position, proxy.publication); err != nil {
		proxy.bufferClaim.Abort()
		return false, err
	}
	return false, nil
}

func (proxy *consensusModuleProxy) cancelTimer(correlationId int64) (bool, error) {
	length := int32(codecIds.MessageHeader.EncodedLength + int64(codecIds.CancelTimer.BlockLength))
	position := proxy.publication.TryClaim(length, &proxy.bufferClaim)
	if position > 0 {
		bytes, err := codecs.CancelTimerEncoder(
			proxy.marshaller,
			proxy.rangeChecking,
			correlationId,
		)
		if err != nil {
			proxy.bufferClaim.Abort()
			return false, err
		}

		proxy.bufferClaim.Buffer().PutBytesArray(proxy.bufferClaim.Offset(), &bytes, 0, length)
		proxy.bufferClaim.Commit()
		return true, nil
	}

	if err := checkResult(position, proxy.publication); err != nil {
		proxy.bufferClaim.Abort()
		return false, err
	}
	return false, nil
}

func (proxy *consensusModuleProxy) clusterMembersQuery(correlationId int64) (bool, error) {
	length := int32(codecIds.MessageHeader.EncodedLength + int64(codecIds.ClusterMembersQuery.BlockLength))
	position := proxy.publication.TryClaim(length, &proxy.bufferClaim)
	if position > 0 {
		bytes, err := codecs.ClusterMembersQueryEncoder(
			proxy.marshaller,
			proxy.rangeChecking,
			correlationId,
			codecs.BooleanType.TRUE,
		)
		if err != nil {
			proxy.bufferClaim.Abort()
			return false, err
		}

		proxy.bufferClaim.Buffer().PutBytesArray(proxy.bufferClaim.Offset(), &bytes, 0, length)
		proxy.bufferClaim.Commit()
		return true, nil
	}

	if err := checkResult(position, proxy.publication); err != nil {
		proxy.bufferClaim.Abort()
		return false, err
	}
	return false, nil
}

// https://github.com/real-logic/aeron/blob/release/1.46.x/aeron-cluster/src/main/java/io/aeron/cluster/service/ConsensusModuleProxy.java#L131
func (proxy *consensusModuleProxy) TryClaim(length int32, bufferClaim *logbuffer.Claim, sessionHeader *atomic.Buffer) (int64, error) {
	sessionHeaderLength := codecIds.MessageHeader.EncodedLength + int64(codecIds.SessionMessageHeader.BlockLength)
	position := proxy.publication.TryClaim(length, bufferClaim)
	if position > 0 {
		bufferClaim.Buffer().PutBytes(int32(logbuffer.DataFrameHeader.Length), sessionHeader, 0, int32(sessionHeaderLength))
	} else {
		if err := checkResult(position, proxy.publication); err != nil {
			bufferClaim.Abort()
			return NullValue, err
		}
	}

	return position, nil
}

func (proxy *consensusModuleProxy) Close() {
	if err := proxy.publication.Close(); err != nil {
		logger.Errorf("failed to close publication: %v", err)
	}
}
