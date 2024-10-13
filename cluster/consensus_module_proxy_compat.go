package cluster

import (
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/cluster/codecs"
)

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
	length := int32(new(codecs.MessageHeader).EncodedLength() + int64(new(codecs.ServiceAck).SbeBlockLength()))
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
	length := int32(new(codecs.MessageHeader).EncodedLength() + int64(new(codecs.CloseSession).SbeBlockLength()))
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
	length := int32(new(codecs.MessageHeader).EncodedLength() + int64(new(codecs.ScheduleTimer).SbeBlockLength()))
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
	length := int32(new(codecs.MessageHeader).EncodedLength() + int64(new(codecs.CancelTimer).SbeBlockLength()))
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
	length := int32(new(codecs.MessageHeader).EncodedLength() + int64(new(codecs.ClusterMembersQuery).SbeBlockLength()))
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

// TODO: fix proper constant and namespace
var SESSION_HEADER_LENGTH = int32(new(codecs.MessageHeader).EncodedLength() + int64(new(codecs.SessionMessageHeader).SbeBlockLength()))

// https://github.com/real-logic/aeron/blob/release/1.46.x/aeron-cluster/src/main/java/io/aeron/cluster/service/ConsensusModuleProxy.java#L131
func (proxy *consensusModuleProxy) TryClaim(length int32, bufferClaim *logbuffer.Claim, sessionHeader *atomic.Buffer) (int64, error) {
	position := proxy.publication.TryClaim(length, bufferClaim)
	if position > 0 {
		bufferClaim.Buffer().PutBytes(int32(logbuffer.DataFrameHeader.Length), sessionHeader, 0, SESSION_HEADER_LENGTH)
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
