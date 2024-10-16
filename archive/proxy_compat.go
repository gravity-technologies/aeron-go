package archive

import (
	"fmt"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/archive/codecs"
)

// TODO: should be configurable for Proxy
const PROXY_DEFAULT_RETRY_ATTEMPTS = 3

// Offer to our request publication with a retry to allow time for the image establishment, some back pressure etc
// https://github.com/real-logic/aeron/blob/release/1.46.x/aeron-archive/src/main/java/io/aeron/archive/client/ArchiveProxy.java#L1478
func (proxy *Proxy) Offer(buffer *atomic.Buffer, offset int32, length int32, reservedValueSupplier term.ReservedValueSupplier) (bool, error) {

	proxy.retryIdleStrategy.Reset()
	attempts := PROXY_DEFAULT_RETRY_ATTEMPTS
	var position int64
	for {
		position = proxy.Publication.Offer(buffer, offset, length, reservedValueSupplier)
		if position > 0 {
			return true, nil
		}

		if position == aeron.PublicationClosed {
			return false, NewArchiveError(-1, -1, "Proxy.Offer :: connection to the archive has been closed")
		}

		if position == aeron.NotConnected {
			return false, NewArchiveError(-1, -1, "Proxy.Offer :: connection to the archive is no longer available")
		}

		if position == aeron.MaxPositionExceeded {
			return false, NewArchiveError(-1, -1, fmt.Sprintf("Proxy.Offer :: offer failed due to max position being reached: term-length=%d", proxy.Publication.TermBufferLength()))
		}

		attempts--
		if attempts <= 0 {
			logger.Debugf("Proxy.Offer :: reached max attempts, offer failed [%d]", position)
			return false, nil
		}

		proxy.retryIdleStrategy.Idle(0)
	}
}

// OfferWithTimeout ...
// adapts from https://github.com/real-logic/aeron/blob/release/1.46.x/aeron-archive/src/main/java/io/aeron/archive/client/ArchiveProxy.java#L1516
func (proxy *Proxy) OfferWithTimeout(buffer *atomic.Buffer, offset int32, length int32, reservedValueSupplier term.ReservedValueSupplier) (bool, error) {
	proxy.retryIdleStrategy.Reset()

	deadline := time.Now().Add(proxy.timeout)
	var position int64
	for {
		position = proxy.Publication.Offer(buffer, offset, length, reservedValueSupplier)
		if position > 0 {
			return true, nil
		}

		if position == aeron.PublicationClosed {
			return false, NewArchiveError(-1, -1, "Proxy.OfferWithTimeout :: connection to the archive has been closed")
		}

		if position == aeron.MaxPositionExceeded {
			return false, NewArchiveError(-1, -1, fmt.Sprintf("Proxy.OfferWithTimoeut :: offer failed due to max position being reached: term-length=%d", proxy.Publication.TermBufferLength()))
		}

		if time.Now().After(deadline) {
			// Give up, returning the last failure
			logger.Debugf("Proxy.OfferWithTimeout timing out [%d]", position)
			return false, nil
		}

		proxy.retryIdleStrategy.Idle(0)
	}
}

// OfferOnce ...
func (proxy *Proxy) OfferOnce(buffer *atomic.Buffer, offset int32, length int32, reservedValueSupplier term.ReservedValueSupplier) int64 {
	return proxy.Publication.Offer(buffer, offset, length, reservedValueSupplier)
}

// ArchiveId Get the id of the archive
func (proxy *Proxy) ArchiveId(correlationId, controlSessionId int64) (bool, error) {
	bytes, err := codecs.ArchiveIdPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationId)
	if err != nil {
		return false, err
	}

	return proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
}

// TryChallengeResponse Try and send a ChallengeResponse to an archive on its control interface providing the credentials. Only one attempt will be made to offer the request.
// https://github.com/real-logic/aeron/blob/release/1.46.x/aeron-archive/src/main/java/io/aeron/archive/client/ArchiveProxy.java#L260
func (proxy *Proxy) TryChallengeResponse(encodedCredentials []uint8, correlationId, controlSessionId int64) (bool, error) {
	// Create a packet and send it
	bytes, err := codecs.ChallengeResponsePacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationId, encodedCredentials)
	if err != nil {
		return false, err
	}

	ret := proxy.OfferOnce(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
	if ret < 0 {
		logger.Debugf("Proxy.TryChallengeResponse :: Offer failed: %d", ret)
	}

	return ret > 0, nil
}

// TryConnect Try and connect to an archive on its control interface providing the response stream details. Only one attempt will be made to offer the request.
// https://github.com/real-logic/aeron/blob/release/1.46.x/aeron-archive/src/main/java/io/aeron/archive/client/ArchiveProxy.java#L170
func (proxy *Proxy) TryConnect(responseChannel string, responseStreamId int32, correlationID int64) (bool, error) {

	// Create a packet and send it
	bytes, err := codecs.ConnectRequestPacket(proxy.marshaller, proxy.rangeChecking, correlationID, responseStreamId, responseChannel)
	if err != nil {
		return false, err
	}

	ret := proxy.OfferOnce(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
	if ret < 0 {
		logger.Debugf("Proxy.TryConnect :: Offer failed: %d", ret)
	}
	return ret > 0, nil
}

// CloseSession close this control session with the archive
func (proxy *Proxy) CloseSession(controlSessionId int64) (bool, error) {
	bytes, err := codecs.CloseSessionRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId)
	if err != nil {
		return false, err
	}

	return proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
}

// Replay a recording from a given position.
func (proxy *Proxy) Replay(correlationID int64, recordingID int64, position int64, length int64, replayChannel string, replayStream int32, controlSessionId int64) (bool, error) {
	bytes, err := codecs.ReplayRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, recordingID, position, length, replayStream, replayChannel)
	if err != nil {
		return false, err
	}

	return proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
}

// BoundedReplay Replay a recording from a given position bounded by a position counter.
func (proxy *Proxy) BoundedReplay(correlationID int64, recordingID int64, position int64, length int64, limitCounterID int32, replayStream int32, replayChannel string, controlSessionId int64) (bool, error) {
	bytes, err := codecs.BoundedReplayPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, recordingID, position, length, limitCounterID, replayStream, replayChannel)
	if err != nil {
		return false, err
	}

	return proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
}

// StopReplay stop an existing replay session
func (proxy *Proxy) StopReplay(correlationID int64, replaySessionID int64, controlSessionId int64) (bool, error) {
	bytes, err := codecs.StopReplayRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, replaySessionID)
	if err != nil {
		return false, err
	}

	return proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
}

// StopAllReplays Stop any existing replay sessions for recording id or all replay sessions regardless of recording id.
func (proxy *Proxy) StopAllReplays(correlationID int64, recordingID int64, controlSessionId int64) (bool, error) {
	bytes, err := codecs.StopAllReplaysPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, recordingID)
	if err != nil {
		return false, err
	}

	return proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
}

// ListRecording List a recording descriptor for a given recording id.
func (proxy *Proxy) ListRecording(correlationID int64, fromRecordingID int64, controlSessionId int64) (bool, error) {
	bytes, err := codecs.ListRecordingRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, fromRecordingID)
	if err != nil {
		return false, err
	}

	return proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
}

// ListRecordingsRequest list a range of recording descriptors
// Lists up to recordCount recordings starting at fromRecordingID
func (proxy *Proxy) ListRecordings(correlationID int64, fromRecordingID int64, recordCount int32, controlSessionId int64) (bool, error) {
	bytes, err := codecs.ListRecordingsRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, fromRecordingID, recordCount)
	if err != nil {
		return false, err
	}

	return proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
}

// ListRecordingsForUri List a range of recording descriptors which match a channel URI fragment and stream id.
// Lists up to recordCount recordings that match the channel and stream
func (proxy *Proxy) ListRecordingsForUri(correlationID int64, fromRecordingID int64, recordCount int32, stream int32, channel string, controlSessionId int64) (bool, error) {
	bytes, err := codecs.ListRecordingsForUriRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, fromRecordingID, recordCount, stream, channel)
	if err != nil {
		return false, err
	}

	return proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
}

// ExtendRecording Extend an existing, non-active, recorded stream for the same channel and stream id.
// Uses the more recent protocol addition ExtendRecordingRequest2 which added autoStop
func (proxy *Proxy) ExtendRecording(correlationID int64, recordingID int64, stream int32, sourceLocation codecs.SourceLocationEnum, autoStop bool, channel string, controlSessionId int64) (bool, error) {
	bytes, err := codecs.ExtendRecordingRequest2Packet(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, recordingID, stream, sourceLocation, autoStop, channel)
	if err != nil {
		return false, err
	}

	return proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
}

// GetRecordingPosition Get the recorded position of an active recording.
func (proxy *Proxy) GetRecordingPosition(correlationID int64, recordingID int64, controlSessionId int64) (bool, error) {
	bytes, err := codecs.RecordingPositionRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, recordingID)
	if err != nil {
		return false, err
	}

	return proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
}

// GetStopPosition Get the stop position of a recording.
func (proxy *Proxy) GetStopPosition(correlationID int64, recordingID int64, controlSessionId int64) (bool, error) {
	bytes, err := codecs.StopPositionPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, recordingID)
	if err != nil {
		return false, err
	}

	return proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
}

// GetStartPosition Get the start position of a recording.
func (proxy *Proxy) GetStartPosition(correlationID int64, recordingID int64, controlSessionId int64) (bool, error) {
	bytes, err := codecs.StartPositionRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, recordingID)
	if err != nil {
		return false, err
	}

	return proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
}

// FindLastMatchingRecording Find the last recording that matches the given criteria.
func (proxy *Proxy) FindLastMatchingRecording(correlationID int64, minRecordingID int64, sessionID int32, stream int32, channel string, controlSessionId int64) (bool, error) {
	bytes, err := codecs.FindLastMatchingRecordingPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, minRecordingID, sessionID, stream, channel)
	if err != nil {
		return false, err
	}

	return proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
}

// ListRecordingSubscriptions List registered subscriptions in the archive which have been used to record streams.
func (proxy *Proxy) ListRecordingSubscriptions(correlationID int64, pseudoIndex int32, subscriptionCount int32, applyStreamID bool, stream int32, channel string, controlSessionId int64) (bool, error) {
	bytes, err := codecs.ListRecordingSubscriptionsPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, pseudoIndex, subscriptionCount, applyStreamID, stream, channel)
	if err != nil {
		return false, err
	}

	return proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
}

// ReplicateReplicate a recording from a source archive to a destination which can be considered a backup for a primary
// archive. The source recording will be replayed via the provided replay channel and use the original stream id.
// If the destination recording id is {@link io.aeron.Aeron#NULL_VALUE} then a new destination recording is created,
// otherwise the provided destination recording id will be extended. The details of the source recording
// descriptor will be replicated.
func (proxy *Proxy) Replicate(correlationID int64, srcRecordingID int64, dstRecordingID int64, srcControlStreamID int32, srcControlChannel string, liveDestination string, controlSessionId int64) (bool, error) {
	bytes, err := codecs.ReplicateRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, srcRecordingID, dstRecordingID, srcControlStreamID, srcControlChannel, liveDestination)
	if err != nil {
		return false, err
	}

	return proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
}

// Replicate2 ...
func (proxy *Proxy) Replicate2(correlationID int64, srcRecordingID int64, dstRecordingID int64, stopPosition int64, channelTagID int64, srcControlStreamID int32, srcControlChannel string, liveDestination string, replicationChannel string, controlSessionId int64) (bool, error) {
	bytes, err := codecs.ReplicateRequest2Packet(
		proxy.marshaller,
		proxy.rangeChecking,
		controlSessionId,
		correlationID,
		srcRecordingID,
		dstRecordingID,
		stopPosition,
		channelTagID,
		srcControlStreamID,
		srcControlChannel,
		liveDestination,
		replicationChannel)
	if err != nil {
		return false, err
	}

	return proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
}

// TaggedReplicate ...
// Replicate a recording from a source archive to a destination which can be considered a backup for a primary
// archive. The source recording will be replayed via the provided replay channel and use the original stream id.
// If the destination recording id is io.aeron.Aeron#NULL_VALUE then a new destination recording is created,
// otherwise the provided destination recording id will be extended. The details of the source recording
// descriptor will be replicated. The subscription used in the archive will be tagged with the provided tags.
// For a source recording that is still active the replay can merge with the live stream and then follow it
// directly and no longer require the replay from the source. This would require a multicast live destination.
// Errors will be reported asynchronously and can be checked for with  AeronArchive#pollForErrorResponse()
// or AeronArchive#checkForErrorResponse().
func (proxy *Proxy) TaggedReplicate(correlationID int64, srcRecordingID int64, dstRecordingID int64, channelTagID int64, subscriptionTagID int64, srcControlStreamID int32, srcControlChannel string, liveDestination string, controlSessionId int64) (bool, error) {
	bytes, err := codecs.TaggedReplicateRequestPacket(
		proxy.marshaller,
		proxy.rangeChecking,
		controlSessionId,
		correlationID,
		srcRecordingID,
		dstRecordingID,
		channelTagID,
		subscriptionTagID,
		srcControlStreamID,
		srcControlChannel,
		liveDestination)
	if err != nil {
		return false, err
	}

	return proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
}

// StopReplication Stop an active replication by the registration id it was registered with.
func (proxy *Proxy) StopReplication(correlationID int64, replicationID int64, controlSessionId int64) (bool, error) {
	bytes, err := codecs.StopReplicationRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, replicationID)
	if err != nil {
		return false, err
	}

	return proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
}

// DetachSegments from the beginning of a recording up to the provided new start position.
// The new start position must be first byte position of a segment after the existing start position.
// It is not possible to detach segments which are active for recording or being replayed.
func (proxy *Proxy) DetachSegments(correlationID int64, recordingID int64, newStartPosition int64, controlSessionId int64) (bool, error) {
	bytes, err := codecs.DetachSegmentsRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, recordingID, newStartPosition)
	if err != nil {
		return false, err
	}

	return proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
}

// DeleteDetachedSegments Delete segments which have been previously detached from a recording.
func (proxy *Proxy) DeleteDetachedSegments(correlationID int64, recordingID int64, controlSessionId int64) (bool, error) {
	bytes, err := codecs.DeleteDetachedSegmentsRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, recordingID)
	if err != nil {
		return false, err
	}

	return proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
}

// PurgeSegments Purge (detach and delete) segments from the beginning of a recording up to the provided new start position.
// The new start position must be first byte position of a segment after the existing start position.
// It is not possible to purge segments which are active for recording or being replayed.
func (proxy *Proxy) PurgeSegments(correlationID int64, recordingID int64, newStartPosition int64, controlSessionId int64) (bool, error) {
	// Create a packet and send it
	bytes, err := codecs.PurgeSegmentsRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, recordingID, newStartPosition)
	if err != nil {
		return false, err
	}

	return proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
}

// AttachSegments Attach segments to the beginning of a recording to restore history that was previously detached.
// Segment files must match the existing recording and join exactly to the start position of the recording
// they are being attached to.
func (proxy *Proxy) AttachSegments(correlationID int64, recordingID int64, controlSessionId int64) (bool, error) {
	bytes, err := codecs.AttachSegmentsRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, recordingID)
	if err != nil {
		return false, err
	}

	return proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
}

// MigrateSegments Migrate segments from a source recording and attach them to the beginning or end of a destination recording.
// The source recording must match the destination recording for segment length, term length, mtu length, and
// stream id. The source recording must join to the destination recording on a segment boundary and without gaps,
// i.e., the stop position and term id of one must match the start position and term id of the other.
// The source recording must be stopped. The destination recording must be stopped if migrating segments
// to the end of the destination recording.
// The source recording will be effectively truncated back to its start position after the migration.
func (proxy *Proxy) MigrateSegments(correlationID int64, srcRecordingID int64, destRecordingID int64, controlSessionId int64) (bool, error) {
	bytes, err := codecs.MigrateSegmentsRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, srcRecordingID, destRecordingID)
	if err != nil {
		return false, err
	}

	return proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
}

// StartRecording Start recording streams for a given channel and stream id pairing.
// Uses the more recent protocol addition StartdRecordingRequest2 which added autoStop
func (proxy *Proxy) StartRecording(correlationID int64, stream int32, isLocal bool, autoStop bool, channel string, controlSessionId int64) (bool, error) {
	bytes, err := codecs.StartRecordingRequest2Packet(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, stream, isLocal, autoStop, channel)
	if err != nil {
		return false, err
	}

	return proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
}

// StopRecording Stop an active recording
func (proxy *Proxy) StopRecording(correlationID int64, stream int32, channel string, controlSessionId int64) (bool, error) {
	bytes, err := codecs.StopRecordingRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, stream, channel)
	if err != nil {
		return false, err
	}

	return proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
}

// StopRecordingBySubscription Stop a recording by the Subscription#registrationId() it was registered with.
func (proxy *Proxy) StopRecordingBySubscriptionId(correlationID int64, subscriptionID int64, controlSessionId int64) (bool, error) {
	// Create a packet and send it
	bytes, err := codecs.StopRecordingSubscriptionPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, subscriptionID)
	if err != nil {
		return false, err
	}

	return proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
}

// Stop an active recording by the recording id. This is not the Subscription#registrationId()
func (proxy *Proxy) StopRecordingByIdentity(correlationID int64, recordingID int64, controlSessionId int64) (bool, error) {
	bytes, err := codecs.StopRecordingByIdentityPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, recordingID)
	if err != nil {
		return false, err
	}

	return proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
}

// TruncateRecording Truncate a stopped recording to a given position that is less than the stopped position.
// The provided position must be on a fragment boundary. Truncating a recording to the start position effectively deletes the recording.
func (proxy *Proxy) TruncateRecording(correlationID int64, recordingID int64, position int64, controlSessionId int64) (bool, error) {
	bytes, err := codecs.TruncateRecordingRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, recordingID, position)
	if err != nil {
		return false, err
	}

	return proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
}

// PurgeRecording Purge a stopped recording, i.e. mark recording as RecordingState#INVALID
// and delete the corresponding segment files.
// The space in the Catalog will be reclaimed upon compaction.
func (proxy *Proxy) PurgeRecording(correlationID int64, replaySessionID int64, controlSessionId int64) (bool, error) {
	bytes, err := codecs.PurgeRecordingRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, replaySessionID)
	if err != nil {
		return false, err
	}

	return proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
}

// KeepAlive Keep this archive session alive by notifying the archive.
func (proxy *Proxy) KeepAlive(controlSessionId, correlationId int64) (bool, error) {
	bytes, err := codecs.KeepAliveRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationId)
	if err != nil {
		return false, err
	}

	return proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
}

// Connect packet and offer
// https://github.com/real-logic/aeron/blob/release/1.46.x/aeron-archive/src/main/java/io/aeron/archive/client/ArchiveProxy.java#L145
func (proxy *Proxy) Connect(correlationID int64, responseStream int32, responseChannel string) (bool, error) {
	bytes, err := codecs.ConnectRequestPacket(proxy.marshaller, proxy.rangeChecking, correlationID, responseStream, responseChannel)
	if err != nil {
		return false, err
	}

	return proxy.OfferWithTimeout(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
}
