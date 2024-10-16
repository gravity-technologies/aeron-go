package archive

import (
	"fmt"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/archive/codecs"
)

// Connect to an Aeron archive by providing a ArchiveContext. This will create a control session.
func Connect(ctx *ArchiveContext) (aeronArchive *Archive, err error) {
	asyncConnect, err := NewAsyncConnect(ctx)
	if err != nil {
		asyncConnect.Close()
		return nil, err
	}

	previousStep := asyncConnect.State

	for aeronArchive == nil {
		aeronArchive, err = asyncConnect.Poll()
		if err != nil {
			asyncConnect.Close()
			return nil, err
		}

		if asyncConnect.State == previousStep {
			asyncConnect.Ctx.IdleStrategy.Idle(0)
		} else {
			logger.Infof("archive asyncConnect state transition: %d -> %d", previousStep, asyncConnect.State)
			asyncConnect.Ctx.IdleStrategy.Reset()
			previousStep = asyncConnect.State
		}
	}

	return
}

// StartRecording a channel/stream
//
// Channels that include sessionId parameters are considered different
// than channels without sessionIds. If a publication matches both a
// sessionId specific channel recording and a non-sessionId specific
// recording, it will be recorded twice.
//
// Returns (subscriptionId, nil) or (0, error) on failure.  The
// SubscriptionId can be used in StopRecordingBySubscription()
func (archive *Archive) StartRecording(channel string, stream int32, isLocal bool, autoStop bool) (int64, error) {
	archive.mtx.Lock()
	defer archive.mtx.Unlock()

	if err := archive.ensureOpen(); err != nil {
		return aeron.NullValue, err
	}
	if err := archive.ensureNotReentrant(); err != nil {
		return aeron.NullValue, err
	}

	lastCorrelationID := archive.ArchiveContext.Aeron.NextCorrelationID()
	logger.Debugf("StartRecording(%s:%d), lastCorrelationID:%d", channel, stream, lastCorrelationID)

	offered, offeredErr := archive.Proxy.StartRecording(lastCorrelationID, stream, isLocal, autoStop, channel, archive.SessionID)
	if err := archive.handleProxyOffered(offered, offeredErr, "failed to send start recording request"); err != nil {
		return aeron.NullValue, err
	}

	return archive.Control.PollForResponse(lastCorrelationID, archive.SessionID)
}

// StopRecording can be performed by RecordingID, by SubscriptionId,
// by Publication, or by a channel/stream pairing (default)

// StopRecording by Channel and Stream
//
// Channels that include sessionId parameters are considered different than channels without sessionIds. Stopping
// recording on a channel without a sessionId parameter will not stop the recording of any sessionId specific
// recordings that use the same channel and streamID.
func (archive *Archive) StopRecording(channel string, stream int32) error {
	archive.mtx.Lock()
	defer archive.mtx.Unlock()

	if err := archive.ensureOpen(); err != nil {
		return err
	}
	if err := archive.ensureNotReentrant(); err != nil {
		return err
	}

	lastCorrelationID := archive.ArchiveContext.Aeron.NextCorrelationID()
	logger.Debugf("StopRecording(%s:%d), lastCorrelationID:%d", channel, stream, lastCorrelationID)

	offered, offeredErr := archive.Proxy.StopRecording(lastCorrelationID, stream, channel, archive.SessionID)
	if err := archive.handleProxyOffered(offered, offeredErr, "failed to send stop recording request"); err != nil {
		return err
	}

	_, err := archive.Control.PollForResponse(lastCorrelationID, archive.SessionID)
	return err
}

// TryStopRecordingByIdentity Try stop an active recording by its recording id.
// Return true if the recording was stopped or false if the recording is not currently active.
func (archive *Archive) TryStopRecordingByIdentity(recordingID int64) (bool, error) {
	archive.mtx.Lock()
	defer archive.mtx.Unlock()

	if err := archive.ensureOpen(); err != nil {
		return false, err
	}
	if err := archive.ensureNotReentrant(); err != nil {
		return false, err
	}

	lastCorrelationID := archive.ArchiveContext.Aeron.NextCorrelationID()
	logger.Debugf("StopRecordingByIdentity(%d), lastCorrelationID:%d", recordingID, lastCorrelationID)

	offered, offeredErr := archive.Proxy.StopRecordingByIdentity(lastCorrelationID, recordingID, archive.SessionID)
	if err := archive.handleProxyOffered(offered, offeredErr, "failed to send stop recording request"); err != nil {
		return false, err
	}

	res, err := archive.Control.PollForResponse(lastCorrelationID, archive.SessionID)
	if err != nil {
		logger.Errorf("TryStopRecordingByIdentity :: PollForResponse failed : %s", err)
		return false, err
	}
	return res != 0, nil
}

// StopRecordingBySubscriptionId as returned by StartRecording
//
// Channels that include sessionId parameters are considered different than channels without sessionIds. Stopping
// recording on a channel without a sessionId parameter will not stop the recording of any sessionId specific
// recordings that use the same channel and streamID.
//
// Returns error on failure, nil on success
func (archive *Archive) StopRecordingBySubscriptionId(subscriptionID int64) error {
	archive.mtx.Lock()
	defer archive.mtx.Unlock()

	if err := archive.ensureOpen(); err != nil {
		return err
	}
	if err := archive.ensureNotReentrant(); err != nil {
		return err
	}

	lastCorrelationID := archive.ArchiveContext.Aeron.NextCorrelationID()
	logger.Debugf("StopRecordingBySubscriptionId(%d), lastCorrelationID:%d", subscriptionID, lastCorrelationID)

	offered, offeredErr := archive.Proxy.StopRecordingBySubscriptionId(lastCorrelationID, subscriptionID, archive.SessionID)
	if err := archive.handleProxyOffered(offered, offeredErr, "failed to send stop recording by subscriptionId request"); err != nil {
		return err
	}

	_, err := archive.Control.PollForResponse(lastCorrelationID, archive.SessionID)
	return err
}

// StopRecordingByPublication to stop recording a sessionId specific
// recording that pertains to the given Publication
//
// Returns error on failure, nil on success
func (archive *Archive) StopRecordingByPublication(publication aeron.Publication) error {
	channel, err := AddSessionIdToChannel(publication.Channel(), publication.SessionID())
	if err != nil {
		return err
	}
	return archive.StopRecording(channel, publication.StreamID())
}

// AddRecordedPublication Add a Publication and set it up to be recorded.
// If this is not the first, i.e. Publication#isOriginal() is true, then an ArchiveError
// will be thrown and the recording not initiated.
//
// This is a sessionId specific recording.
func (archive *Archive) AddRecordedPublication(channel string, streamId int32) (publication *aeron.Publication, err error) {
	archive.mtx.Lock()
	defer archive.mtx.Unlock()

	if err := archive.ensureOpen(); err != nil {
		return nil, err
	}
	if err := archive.ensureNotReentrant(); err != nil {
		return nil, err
	}

	// This can fail in aeron via log.Fatalf(), not much we can do
	publication, err = archive.AddPublication(channel, streamId)
	if err != nil {
		logger.Errorf("failed to add recorded publication")
		return nil, err
	}
	if !publication.IsOriginal() {
		return nil, &ArchiveError{Message: fmt.Sprintf("publication already added for channel=%s streamId=%d", channel, streamId)}
	}

	lastCorrelationID := archive.ArchiveContext.Aeron.NextCorrelationID()
	logger.Debugf("AddRecordedPublication(), lastCorrelationID:%d", lastCorrelationID)

	sessionChannel, err := AddSessionIdToChannel(publication.Channel(), publication.SessionID())
	if err != nil {
		publication.Close()
		return nil, err
	}

	if _, err := archive.Proxy.StartRecording(lastCorrelationID, streamId, true, false, sessionChannel, archive.SessionID); err != nil {
		publication.Close()
		return nil, err
	}

	return publication, nil
}

// AddRecordedExclusivePublication Add an ExclusivePublication and set it up to be recorded.
//
// This is a sessionId specific recording.
func (archive *Archive) AddRecordedExclusivePublication(channel string, streamId int32) (publication *aeron.Publication, err error) {
	archive.mtx.Lock()
	defer archive.mtx.Unlock()

	if err := archive.ensureOpen(); err != nil {
		return nil, err
	}
	if err := archive.ensureNotReentrant(); err != nil {
		return nil, err
	}

	// This can fail in aeron via log.Fatalf(), not much we can do
	publication, err = archive.AddExclusivePublication(channel, streamId)
	if err != nil {
		logger.Errorf("failed to add recorded publication")
		return nil, err
	}

	lastCorrelationID := archive.ArchiveContext.Aeron.NextCorrelationID()
	logger.Debugf("AddRecordedExclusivePublication(), lastCorrelationID:%d", lastCorrelationID)

	sessionChannel, err := AddSessionIdToChannel(publication.Channel(), publication.SessionID())
	if err != nil {
		publication.Close()
		return nil, err
	}

	// TODO: unsure about the sematic of autoStop=false or true
	if _, err := archive.Proxy.StartRecording(lastCorrelationID, streamId, true, false, sessionChannel, archive.SessionID); err != nil {
		publication.Close()
		return nil, err
	}

	return publication, nil
}

// TODO: weird go semantic - should pass in consumer + return count
//
// ListRecordings up to recordCount recording descriptors
// returns the number of descriptors found and consumed.
func (archive *Archive) ListRecordings(fromRecordingID int64, recordCount int32) ([]*codecs.RecordingDescriptor, error) {
	archive.mtx.Lock()
	defer archive.mtx.Unlock()

	if err := archive.ensureOpen(); err != nil {
		return nil, err
	}
	if err := archive.ensureNotReentrant(); err != nil {
		return nil, err
	}

	archive.isInCallback = true
	defer func() { archive.isInCallback = false }()

	lastCorrelationID := archive.ArchiveContext.Aeron.NextCorrelationID()
	logger.Debugf("ListRecordings(%d, %d), lastCorrelationID:%d", fromRecordingID, recordCount, lastCorrelationID)

	offered, offeredErr := archive.Proxy.ListRecordings(lastCorrelationID, fromRecordingID, recordCount, archive.SessionID)
	if err := archive.handleProxyOffered(offered, offeredErr, "failed to send list recordings request"); err != nil {
		return nil, err
	}

	var descriptors []*codecs.RecordingDescriptor
	if _, err := archive.Control.PollForDescriptors(lastCorrelationID, 1, archive.Control.AppendingRecordingDescriptorConsumer(&descriptors)); err != nil {
		return nil, err
	}
	return descriptors, nil
}

// TODO: weird go semantic - should pass in consumer + return count
//
// ListRecordingsForUri will list up to recordCount recording descriptors from fromRecordingID
// with a limit of recordCount for a given channel and stream.
//
// Returns the number of descriptors consumed. If fromRecordingID is
// greater than the largest known we return 0.
func (archive *Archive) ListRecordingsForUri(fromRecordingID int64, recordCount int32, channelFragment string, stream int32) ([]*codecs.RecordingDescriptor, error) {
	archive.mtx.Lock()
	defer archive.mtx.Unlock()

	if err := archive.ensureOpen(); err != nil {
		return nil, err
	}
	if err := archive.ensureNotReentrant(); err != nil {
		return nil, err
	}

	archive.isInCallback = true
	defer func() { archive.isInCallback = false }()

	lastCorrelationID := archive.ArchiveContext.Aeron.NextCorrelationID()
	logger.Debugf("ListRecordingsForUri(%d, %d, %s, %d), lastCorrelationID:%d", fromRecordingID, recordCount, channelFragment, stream, lastCorrelationID)

	offered, offeredErr := archive.Proxy.ListRecordingsForUri(lastCorrelationID, fromRecordingID, recordCount, stream, channelFragment, archive.SessionID)
	if err := archive.handleProxyOffered(offered, offeredErr, "failed to send list recordings request for uri"); err != nil {
		return nil, err
	}

	var descriptors []*codecs.RecordingDescriptor
	if _, err := archive.Control.PollForDescriptors(lastCorrelationID, 1, archive.Control.AppendingRecordingDescriptorConsumer(&descriptors)); err != nil {
		return nil, err
	}

	return descriptors, nil
}

// TODO: weird go semantic - should pass in consumer + return count
// ListRecording will fetch the recording descriptor for a recordingID
//
// Returns a single recording descriptor or nil if there was no match
func (archive *Archive) ListRecording(recordingID int64) (*codecs.RecordingDescriptor, error) {
	archive.mtx.Lock()
	defer archive.mtx.Unlock()

	if err := archive.ensureOpen(); err != nil {
		return nil, err
	}
	if err := archive.ensureNotReentrant(); err != nil {
		return nil, err
	}

	archive.isInCallback = true
	defer func() { archive.isInCallback = false }()

	lastCorrelationID := archive.ArchiveContext.Aeron.NextCorrelationID()
	logger.Debugf("ListRecording(%d), lastCorrelationID:%d", recordingID, lastCorrelationID)

	offered, offeredErr := archive.Proxy.ListRecording(lastCorrelationID, recordingID, archive.SessionID)
	if err := archive.handleProxyOffered(offered, offeredErr, "failed to send list recording request"); err != nil {
		return nil, err
	}

	var descriptors []*codecs.RecordingDescriptor
	count, err := archive.Control.PollForDescriptors(lastCorrelationID, 1, archive.Control.AppendingRecordingDescriptorConsumer(&descriptors))
	if err != nil {
		return nil, err
	}
	// Otherwise we can return our results
	if count > 0 {
		return descriptors[0], nil
	}
	return nil, nil
}

// StartReplay for a length in bytes of a recording from a position.
//
// If the position is RecordingPositionNull (-1) then the stream will
// be replayed from the start.
//
// If the length is RecordingLengthMax (2^31-1) the replay will follow
// a live recording.
//
// If the length is RecordingLengthNull (-1) the replay will
// replay the whole stream of unknown length.
//
// The lower 32-bits of the returned value contains the ImageSessionID() of the received replay. All
// 64-bits are required to uniquely identify the replay when calling StopReplay(). The lower 32-bits
// can be obtained by casting the int64 value to an int32. See ReplaySessionIdToSessionId() helper.
//
// Returns a ReplaySessionID - the id of the replay session which will be the same as the Image sessionId
// of the received replay for correlation with the matching channel and stream id in the lower 32 bits
func (archive *Archive) StartReplay(recordingID int64, position int64, length int64, replayChannel string, replayStream int32) (int64, error) {
	archive.mtx.Lock()
	defer archive.mtx.Unlock()

	if err := archive.ensureOpen(); err != nil {
		return aeron.NullValue, err
	}
	if err := archive.ensureNotReentrant(); err != nil {
		return aeron.NullValue, err
	}

	lastCorrelationID := archive.ArchiveContext.Aeron.NextCorrelationID()
	logger.Debugf("StartReplay(%d, %d, %d, %s, %d), lastCorrelationID:%d", recordingID, position, length, replayChannel, replayStream, lastCorrelationID)

	offered, offeredErr := archive.Proxy.Replay(lastCorrelationID, recordingID, position, length, replayChannel, replayStream, archive.SessionID)
	if err := archive.handleProxyOffered(offered, offeredErr, "failed to send replay request"); err != nil {
		return aeron.NullValue, err
	}

	return archive.Control.PollForResponse(lastCorrelationID, archive.SessionID)
}

// BoundedReplay to start a replay for a length in bytes of a
// recording from a position bounded by a position counter.
//
// If the position is RecordingPositionNull (-1) then the stream will
// be replayed from the start.
//
// If the length is RecordingLengthMax (2^31-1) the replay will follow
// a live recording.
//
// If the length is RecordingLengthNull (-1) the replay will
// replay the whole stream of unknown length.
//
// The lower 32-bits of the returned value contains the ImageSessionID() of the received replay. All
// 64-bits are required to uniquely identify the replay when calling StopReplay(). The lower 32-bits
// can be obtained by casting the int64 value to an int32. See ReplaySessionIdToSessionId() helper.
//
// Returns a ReplaySessionID - the id of the replay session which will be the same as the Image sessionId
// of the received replay for correlation with the matching channel and stream id in the lower 32 bits
func (archive *Archive) StartBoundedReplay(recordingID int64, position int64, length int64, limitCounterID int32, replayStream int32, replayChannel string) (int64, error) {
	archive.mtx.Lock()
	defer archive.mtx.Unlock()

	if err := archive.ensureOpen(); err != nil {
		return aeron.NullValue, err
	}
	if err := archive.ensureNotReentrant(); err != nil {
		return aeron.NullValue, err
	}

	lastCorrelationID := archive.ArchiveContext.Aeron.NextCorrelationID()
	logger.Debugf("BoundedReplay(%d, %d, %d, %d, %d, %s), lastCorrelationID:%d", recordingID, position, length, limitCounterID, replayStream, lastCorrelationID)

	offered, offeredErr := archive.Proxy.BoundedReplay(lastCorrelationID, recordingID, position, length, limitCounterID, replayStream, replayChannel, archive.SessionID)
	if err := archive.handleProxyOffered(offered, offeredErr, "failed to send bounded replay request"); err != nil {
		return aeron.NullValue, err
	}

	return archive.Control.PollForResponse(lastCorrelationID, archive.SessionID)
}

// StopReplay for a  session.
//
// Returns error on failure, nil on success
func (archive *Archive) StopReplay(replaySessionID int64) error {
	archive.mtx.Lock()
	defer archive.mtx.Unlock()

	if err := archive.ensureOpen(); err != nil {
		return err
	}
	if err := archive.ensureNotReentrant(); err != nil {
		return err
	}

	lastCorrelationID := archive.ArchiveContext.Aeron.NextCorrelationID()
	logger.Debugf("StopReplay(%d), lastCorrelationID:%d", replaySessionID, lastCorrelationID)

	offered, offeredErr := archive.Proxy.StopReplay(lastCorrelationID, replaySessionID, archive.SessionID)
	if err := archive.handleProxyOffered(offered, offeredErr, "failed to send stop replay request"); err != nil {
		return err
	}

	_, err := archive.Control.PollForResponse(lastCorrelationID, archive.SessionID)
	return err
}

// StopAllReplays for a given recordingID
//
// Returns error on failure, nil on success
func (archive *Archive) StopAllReplays(recordingID int64) error {
	archive.mtx.Lock()
	defer archive.mtx.Unlock()

	if err := archive.ensureOpen(); err != nil {
		return err
	}
	if err := archive.ensureNotReentrant(); err != nil {
		return err
	}

	lastCorrelationID := archive.ArchiveContext.Aeron.NextCorrelationID()
	logger.Debugf("StopAllReplays(%d), lastCorrelationID:%d", recordingID, lastCorrelationID)

	offered, offeredErr := archive.Proxy.StopAllReplays(lastCorrelationID, recordingID, archive.SessionID)
	if err := archive.handleProxyOffered(offered, offeredErr, "failed to send stop all replays request"); err != nil {
		return err
	}

	_, err := archive.Control.PollForResponse(lastCorrelationID, archive.SessionID)
	return err
}

// ExtendRecording to extend an existing non-active recording of a channel and stream pairing.
//
// The channel must be configured for the initial position from which it will be extended.
//
// Returns the subscriptionId of the recording that can be passed to StopRecording()
func (archive *Archive) ExtendRecording(recordingID int64, stream int32, sourceLocation codecs.SourceLocationEnum, autoStop bool, channel string) (int64, error) {
	archive.mtx.Lock()
	defer archive.mtx.Unlock()

	if err := archive.ensureOpen(); err != nil {
		return aeron.NullValue, err
	}
	if err := archive.ensureNotReentrant(); err != nil {
		return aeron.NullValue, err
	}

	lastCorrelationID := archive.ArchiveContext.Aeron.NextCorrelationID()
	logger.Debugf("ExtendRecording(%d, %d, %d, %t, %s), lastCorrelationID:%d", recordingID, stream, sourceLocation, autoStop, channel, lastCorrelationID)

	offered, offeredErr := archive.Proxy.GetRecordingPosition(lastCorrelationID, recordingID, archive.SessionID)
	if err := archive.handleProxyOffered(offered, offeredErr, "failed to send extend recording request"); err != nil {
		return aeron.NullValue, err
	}

	return archive.Control.PollForResponse(lastCorrelationID, archive.SessionID)
}

// TruncateRecording of a stopped recording to a given position that
// is less than the stopped position. The provided position must be on
// a fragment boundary. Truncating a recording to the start position
// effectively deletes the recording. If the truncate operation will
// result in deleting segments then this will occur
// asynchronously. Before extending a truncated recording which has
// segments being asynchronously being deleted then you should await
// completion via the RecordingSignal Delete
//
// Returns nil on success, error on failre
func (archive *Archive) TruncateRecording(recordingID int64, position int64) (int64, error) {
	archive.mtx.Lock()
	defer archive.mtx.Unlock()

	if err := archive.ensureOpen(); err != nil {
		return aeron.NullValue, err
	}
	if err := archive.ensureNotReentrant(); err != nil {
		return aeron.NullValue, err
	}

	lastCorrelationID := archive.ArchiveContext.Aeron.NextCorrelationID()
	logger.Debugf("TruncateRecording(%d %d), lastCorrelationID:%d", recordingID, position, lastCorrelationID)

	offered, offeredErr := archive.Proxy.TruncateRecording(lastCorrelationID, recordingID, position, archive.SessionID)
	if err := archive.handleProxyOffered(offered, offeredErr, "failed to send truncate recording request"); err != nil {
		return aeron.NullValue, err
	}

	return archive.Control.PollForResponse(lastCorrelationID, archive.SessionID)
}

// GetStartPosition for a recording.
//
// Return the start position of the recording or (0, error) on failure
func (archive *Archive) GetStartPosition(recordingID int64) (int64, error) {
	archive.mtx.Lock()
	defer archive.mtx.Unlock()

	if err := archive.ensureOpen(); err != nil {
		return aeron.NullValue, err
	}
	if err := archive.ensureNotReentrant(); err != nil {
		return aeron.NullValue, err
	}

	lastCorrelationID := archive.ArchiveContext.Aeron.NextCorrelationID()
	logger.Debugf("GetStartPosition(%d), lastCorrelationID:%d", recordingID, lastCorrelationID)

	offered, offeredErr := archive.Proxy.GetStartPosition(lastCorrelationID, recordingID, archive.SessionID)
	if err := archive.handleProxyOffered(offered, offeredErr, "failed to send get start request"); err != nil {
		return aeron.NullValue, err
	}

	return archive.Control.PollForResponse(lastCorrelationID, archive.SessionID)
}

// GetStopPosition for a recording.
//
// Return the stop position, or RecordingPositionNull if still active.
func (archive *Archive) GetStopPosition(recordingID int64) (int64, error) {
	archive.mtx.Lock()
	defer archive.mtx.Unlock()

	if err := archive.ensureOpen(); err != nil {
		return aeron.NullValue, err
	}
	if err := archive.ensureNotReentrant(); err != nil {
		return aeron.NullValue, err
	}

	lastCorrelationID := archive.ArchiveContext.Aeron.NextCorrelationID()
	logger.Debugf("GetStopPosition(%d), lastCorrelationID:%d", recordingID, lastCorrelationID)

	offered, offeredErr := archive.Proxy.GetStopPosition(lastCorrelationID, recordingID, archive.SessionID)
	if err := archive.handleProxyOffered(offered, offeredErr, "failed to send get stop position request"); err != nil {
		return aeron.NullValue, err
	}

	return archive.Control.PollForResponse(lastCorrelationID, archive.SessionID)
}

// FindLastMatchingRecording that matches the given criteria.
//
// Returns the RecordingID or RecordingIdNullValue if no match
func (archive *Archive) FindLastMatchingRecording(minRecordingID int64, sessionID int32, stream int32, channel string) (int64, error) {
	archive.mtx.Lock()
	defer archive.mtx.Unlock()

	if err := archive.ensureOpen(); err != nil {
		return aeron.NullValue, err
	}
	if err := archive.ensureNotReentrant(); err != nil {
		return aeron.NullValue, err
	}

	lastCorrelationID := archive.ArchiveContext.Aeron.NextCorrelationID()
	logger.Debugf("FindLastMatchingRecording(%d, %d, %d, %s), lastCorrelationID:%d", minRecordingID, sessionID, stream, lastCorrelationID)

	offered, offeredErr := archive.Proxy.FindLastMatchingRecording(lastCorrelationID, minRecordingID, sessionID, stream, channel, archive.SessionID)
	if err := archive.handleProxyOffered(offered, offeredErr, "failed to send get stop position request"); err != nil {
		return aeron.NullValue, err
	}

	return archive.Control.PollForResponse(lastCorrelationID, archive.SessionID)
}

// TODO: weird go semantic - should pass in consumer + return count
//
// ListRecordingSubscriptions to list the active recording
// subscriptions in the archive create via StartRecording or
// ExtendRecording.
//
// Returns a (possibly empty) list of RecordingSubscriptionDescriptors
func (archive *Archive) ListRecordingSubscriptions(pseudoIndex int32, subscriptionCount int32, applyStreamID bool, stream int32, channelFragment string) ([]*codecs.RecordingSubscriptionDescriptor, error) {
	archive.mtx.Lock()
	defer archive.mtx.Unlock()

	if err := archive.ensureOpen(); err != nil {
		return nil, err
	}
	if err := archive.ensureNotReentrant(); err != nil {
		return nil, err
	}

	archive.isInCallback = true
	defer func() { archive.isInCallback = false }()

	lastCorrelationID := archive.ArchiveContext.Aeron.NextCorrelationID()
	logger.Debugf("ListRecordingSubscriptions(%, %d, %t, %d, %sd), lastCorrelationID:%d", pseudoIndex, subscriptionCount, applyStreamID, stream, channelFragment, lastCorrelationID)

	offered, offeredErr := archive.Proxy.ListRecordingSubscriptions(lastCorrelationID, pseudoIndex, subscriptionCount, applyStreamID, stream, channelFragment, archive.SessionID)
	if err := archive.handleProxyOffered(offered, offeredErr, "failed to send list recording subscriptions request"); err != nil {
		return nil, err
	}

	var descriptors []*codecs.RecordingSubscriptionDescriptor
	if _, err := archive.Control.PollForSubscriptionDescriptors(lastCorrelationID, subscriptionCount, archive.Control.AppendRecordingSubscriptionDescriptorConsumer(&descriptors)); err != nil {
		return nil, err
	}

	return descriptors, nil
}

// DetachSegments from the beginning of a recording up to the
// provided new start position. The new start position must be first
// byte position of a segment after the existing start position.  It
// is not possible to detach segments which are active for recording
// or being replayed.
//
// Returns error on failure, nil on success
func (archive *Archive) DetachSegments(recordingID int64, newStartPosition int64) error {
	archive.mtx.Lock()
	defer archive.mtx.Unlock()

	if err := archive.ensureOpen(); err != nil {
		return err
	}
	if err := archive.ensureNotReentrant(); err != nil {
		return err
	}

	lastCorrelationID := archive.ArchiveContext.Aeron.NextCorrelationID()
	logger.Debugf("DetachSegments(%d, %d), lastCorrelationID:%d", recordingID, lastCorrelationID)

	offered, offeredErr := archive.Proxy.DetachSegments(lastCorrelationID, recordingID, newStartPosition, archive.SessionID)
	if err := archive.handleProxyOffered(offered, offeredErr, "failed to send detach segments request"); err != nil {
		return err
	}

	_, err := archive.Control.PollForResponse(lastCorrelationID, archive.SessionID)
	return err
}

// DeleteDetachedSegments which have been previously detached from a recording.
//
// Returns the count of deleted segment files.
func (archive *Archive) DeleteDetachedSegments(recordingID int64) (int64, error) {
	archive.mtx.Lock()
	defer archive.mtx.Unlock()

	if err := archive.ensureOpen(); err != nil {
		return aeron.NullValue, err
	}
	if err := archive.ensureNotReentrant(); err != nil {
		return aeron.NullValue, err
	}

	lastCorrelationID := archive.ArchiveContext.Aeron.NextCorrelationID()
	logger.Debugf("DeleteDetachedSegments(%d), lastCorrelationID:%d", recordingID, lastCorrelationID)

	offered, offeredErr := archive.Proxy.DeleteDetachedSegments(lastCorrelationID, recordingID, archive.SessionID)
	if err := archive.handleProxyOffered(offered, offeredErr, "failed to send delete detach segments request"); err != nil {
		return aeron.NullValue, err
	}

	return archive.Control.PollForResponse(lastCorrelationID, archive.SessionID)
}

// PurgeSegments (detach and delete) to segments from the beginning of
// a recording up to the provided new start position. The new start
// position must be first byte position of a segment after the
// existing start position. It is not possible to detach segments
// which are active for recording or being replayed.
//
// Returns the count of deleted segment files.
func (archive *Archive) PurgeSegments(recordingID int64, newStartPosition int64) (int64, error) {
	archive.mtx.Lock()
	defer archive.mtx.Unlock()

	if err := archive.ensureOpen(); err != nil {
		return aeron.NullValue, err
	}
	if err := archive.ensureNotReentrant(); err != nil {
		return aeron.NullValue, err
	}

	lastCorrelationID := archive.ArchiveContext.Aeron.NextCorrelationID()
	logger.Debugf("PurgeSegments(%d, %d), lastCorrelationID:%d", recordingID, newStartPosition, lastCorrelationID)

	offered, offeredErr := archive.Proxy.PurgeSegments(lastCorrelationID, recordingID, newStartPosition, archive.SessionID)
	if err := archive.handleProxyOffered(offered, offeredErr, "failed to send purge segments request"); err != nil {
		return aeron.NullValue, err
	}

	return archive.Control.PollForResponse(lastCorrelationID, archive.SessionID)
}

// AttachSegments to the beginning of a recording to restore history
// that was previously detached.
// Segment files must match the existing recording and join exactly to
// the start position of the recording they are being attached to.
//
// Returns the count of attached segment files.
func (archive *Archive) AttachSegments(recordingID int64) (int64, error) {
	archive.mtx.Lock()
	defer archive.mtx.Unlock()

	if err := archive.ensureOpen(); err != nil {
		return aeron.NullValue, err
	}
	if err := archive.ensureNotReentrant(); err != nil {
		return aeron.NullValue, err
	}

	lastCorrelationID := archive.ArchiveContext.Aeron.NextCorrelationID()
	logger.Debugf("AttachSegments(%d), lastCorrelationID:%d", recordingID, lastCorrelationID)

	offered, offeredErr := archive.Proxy.AttachSegments(lastCorrelationID, recordingID, archive.SessionID)
	if err := archive.handleProxyOffered(offered, offeredErr, "failed to send attach segments request"); err != nil {
		return aeron.NullValue, err
	}

	return archive.Control.PollForResponse(lastCorrelationID, archive.SessionID)

}

// MigrateSegments from a source recording and attach them to the
// beginning of a destination recording.
//
// The source recording must match the destination recording for
// segment length, term length, mtu length, stream id, plus the stop
// position and term id of the source must join with the start
// position of the destination and be on a segment boundary.
//
// The source recording will be effectively truncated back to its
// start position after the migration.  Returns the count of attached
// segment files.
//
// Returns the count of attached segment files.
func (archive *Archive) MigrateSegments(recordingID int64, position int64) (int64, error) {
	archive.mtx.Lock()
	defer archive.mtx.Unlock()

	if err := archive.ensureOpen(); err != nil {
		return aeron.NullValue, err
	}
	if err := archive.ensureNotReentrant(); err != nil {
		return aeron.NullValue, err
	}

	lastCorrelationID := archive.ArchiveContext.Aeron.NextCorrelationID()
	logger.Debugf("MigrateSegments(%d, %d), lastCorrelationID:%d", recordingID, position, lastCorrelationID)

	offered, offeredErr := archive.Proxy.MigrateSegments(lastCorrelationID, recordingID, position, archive.SessionID)
	if err := archive.handleProxyOffered(offered, offeredErr, "failed to send migrate segments request"); err != nil {
		return aeron.NullValue, err
	}

	return archive.Control.PollForResponse(lastCorrelationID, archive.SessionID)
}

// Replicate a recording from a source archive to a destination which
// can be considered a backup for a primary archive. The source
// recording will be replayed via the provided replay channel and use
// the original stream id.  If the destination recording id is
// RecordingIdNullValue (-1) then a new destination recording is
// created, otherwise the provided destination recording id will be
// extended. The details of the source recording descriptor will be
// replicated.
//
// For a source recording that is still active the replay can merge
// with the live stream and then follow it directly and no longer
// require the replay from the source. This would require a multicast
// live destination.
//
// srcRecordingID     recording id which must exist in the source archive.
// dstRecordingID     recording to extend in the destination, otherwise {@link io.aeron.Aeron#NULL_VALUE}.
// srcControlStreamID remote control stream id for the source archive to instruct the replay on.
// srcControlChannel  remote control channel for the source archive to instruct the replay on.
// liveDestination    destination for the live stream if merge is required. nil for no merge.
//
// Returns the replication session id which can be passed StopReplication()
func (archive *Archive) Replicate(srcRecordingID int64, dstRecordingID int64, srcControlStreamID int32, srcControlChannel string, liveDestination string) (int64, error) {
	archive.mtx.Lock()
	defer archive.mtx.Unlock()

	if err := archive.ensureOpen(); err != nil {
		return aeron.NullValue, err
	}
	if err := archive.ensureNotReentrant(); err != nil {
		return aeron.NullValue, err
	}

	lastCorrelationID := archive.ArchiveContext.Aeron.NextCorrelationID()
	logger.Debugf("Replicate(%d, %d, %d, %s, %s), lastCorrelationID:%d", srcRecordingID, dstRecordingID, srcControlStreamID, srcControlChannel, liveDestination, lastCorrelationID)

	offered, offeredErr := archive.Proxy.Replicate(lastCorrelationID, srcRecordingID, dstRecordingID, srcControlStreamID, srcControlChannel, liveDestination, archive.SessionID)
	if err := archive.handleProxyOffered(offered, offeredErr, "failed to send replicate request"); err != nil {
		return aeron.NullValue, err
	}
	return archive.Control.PollForResponse(lastCorrelationID, archive.SessionID)
}

// Replicate2 will replicate a recording from a source archive to a
// destination which can be considered a backup for a primary
// archive. The source recording will be replayed via the provided
// replay channel and use the original stream id.  If the destination
// recording id is RecordingIdNullValue (-1) then a new destination
// recording is created, otherwise the provided destination recording
// id will be extended. The details of the source recording descriptor
// will be replicated.
//
// For a source recording that is still active the replay can merge
// with the live stream and then follow it directly and no longer
// require the replay from the source. This would require a multicast
// live destination.
//
// srcRecordingID     recording id which must exist in the source archive.
// dstRecordingID     recording to extend in the destination, otherwise {@link io.aeron.Aeron#NULL_VALUE}.
// stopPosition       position to stop the replication. RecordingPositionNull to stop at end of current recording.
// srcControlStreamID remote control stream id for the source archive to instruct the replay on.
// srcControlChannel  remote control channel for the source archive to instruct the replay on.
// liveDestination    destination for the live stream if merge is required. nil for no merge.
// replicationChannel channel over which the replication will occur. Empty or null for default channel.
//
// Returns the replication session id which can be passed StopReplication()
func (archive *Archive) Replicate2(srcRecordingID int64, dstRecordingID int64, stopPosition int64, channelTagID int64, srcControlStreamID int32, srcControlChannel string, liveDestination string, replicationChannel string) (int64, error) {
	archive.mtx.Lock()
	defer archive.mtx.Unlock()

	if err := archive.ensureOpen(); err != nil {
		return aeron.NullValue, err
	}
	if err := archive.ensureNotReentrant(); err != nil {
		return aeron.NullValue, err
	}

	lastCorrelationID := archive.ArchiveContext.Aeron.NextCorrelationID()
	logger.Debugf("Replicate2(%d, %d, %d, %d, %d, %s, %s, %s), lastCorrelationID:%d", srcRecordingID, dstRecordingID, stopPosition, channelTagID, srcControlStreamID, srcControlChannel, liveDestination, replicationChannel, lastCorrelationID)

	offered, offeredErr := archive.Proxy.Replicate2(lastCorrelationID, srcRecordingID, dstRecordingID, stopPosition, channelTagID, srcControlStreamID, srcControlChannel, liveDestination, replicationChannel, archive.SessionID)
	if err := archive.handleProxyOffered(offered, offeredErr, "failed to send replicate2 request"); err != nil {
		return aeron.NullValue, err
	}

	return archive.Control.PollForResponse(lastCorrelationID, archive.SessionID)
}

// TaggedReplicate to replicate a recording from a source archive to a
// destination which can be considered a backup for a primary
// archive. The source recording will be replayed via the provided
// replay channel and use the original stream id.  If the destination
// recording id is RecordingIdNullValue (-1) then a new destination
// recording is created, otherwise the provided destination recording
// id will be extended. The details of the source recording descriptor
// will be replicated.
//
// The subscription used in the archive will be tagged
// with the provided tags. For a source recording that is still active
// the replay can merge with the live stream and then follow it
// directly and no longer require the replay from the source. This
// would require a multicast live destination.
//
// srcRecordingID     recording id which must exist in the source archive.
// dstRecordingID     recording to extend in the destination, otherwise {@link io.aeron.Aeron#NULL_VALUE}.
// channelTagID       used to tag the replication subscription.
// subscriptionTagID  used to tag the replication subscription.
// srcControlStreamID remote control stream id for the source archive to instruct the replay on.
// srcControlChannel  remote control channel for the source archive to instruct the replay on.
// liveDestination    destination for the live stream if merge is required. nil for no merge.
//
// Returns the replication session id which can be passed StopReplication()
func (archive *Archive) TaggedReplicate(srcRecordingID int64, dstRecordingID int64, channelTagID int64, subscriptionTagID int64, srcControlStreamID int32, srcControlChannel string, liveDestination string) (int64, error) {
	archive.mtx.Lock()
	defer archive.mtx.Unlock()

	if err := archive.ensureOpen(); err != nil {
		return aeron.NullValue, err
	}
	if err := archive.ensureNotReentrant(); err != nil {
		return aeron.NullValue, err
	}

	lastCorrelationID := archive.ArchiveContext.Aeron.NextCorrelationID()
	logger.Debugf("TaggedReplicate(%d, %d, %d, %d, %d, %s, %s), lastCorrelationID:%d", srcRecordingID, dstRecordingID, channelTagID, subscriptionTagID, srcControlStreamID, srcControlChannel, liveDestination, lastCorrelationID)

	offered, offeredErr := archive.Proxy.TaggedReplicate(lastCorrelationID, srcRecordingID, dstRecordingID, channelTagID, subscriptionTagID, srcControlStreamID, srcControlChannel, liveDestination, archive.SessionID)
	if err := archive.handleProxyOffered(offered, offeredErr, "failed to send tagged replicate request"); err != nil {
		return 0, err
	}

	return archive.Control.PollForResponse(lastCorrelationID, archive.SessionID)
}

// StopReplication of a replication request
//
// Returns error on failure, nil on success
func (archive *Archive) StopReplication(replicationID int64) (err error) {
	archive.mtx.Lock()
	defer archive.mtx.Unlock()

	if err = archive.ensureOpen(); err != nil {
		return err
	}
	if err = archive.ensureNotReentrant(); err != nil {
		return err
	}

	lastCorrelationID := archive.ArchiveContext.Aeron.NextCorrelationID()
	logger.Debugf("StopReplication(%d), lastCorrelationID:%d", replicationID, lastCorrelationID)

	offered, offeredErr := archive.Proxy.StopReplication(lastCorrelationID, replicationID, archive.SessionID)
	if err = archive.handleProxyOffered(offered, offeredErr, "failed to send stop replication request"); err != nil {
		return err
	}

	_, err = archive.Control.PollForResponse(lastCorrelationID, archive.SessionID)
	return err
}

// PurgeRecording of a stopped recording, i.e. mark recording as
// Invalid and delete the corresponding segment files. The space in
// the Catalog will be reclaimed upon compaction.
//
// Returns error on failure, deletedSegmentCount on success
func (archive *Archive) PurgeRecording(recordingID int64) (deletedSegmentCount int64, err error) {
	archive.mtx.Lock()
	defer archive.mtx.Unlock()

	if err := archive.ensureOpen(); err != nil {
		return aeron.NullValue, err
	}
	if err := archive.ensureNotReentrant(); err != nil {
		return aeron.NullValue, err
	}

	lastCorrelationID := archive.ArchiveContext.Aeron.NextCorrelationID()
	logger.Debugf("PurgeRecording(%d), lastCorrelationID:%d", recordingID, lastCorrelationID)

	offered, offeredErr := archive.Proxy.PurgeRecording(lastCorrelationID, recordingID, archive.SessionID)
	if err = archive.handleProxyOffered(offered, offeredErr, "failed to send invalidate recording request"); err != nil {
		return aeron.NullValue, err
	}

	return archive.Control.PollForResponse(lastCorrelationID, archive.SessionID)
}

func (archive *Archive) handleProxyOffered(offered bool, offeredError error, offeredFailedMessage string) error {
	if offeredError != nil {
		return offeredError
	}
	if !offered {
		return aeron.NewAeronError(offeredFailedMessage, aeron.AeronErrorCategory.ERROR)
	}
	return nil
}

func (archive *Archive) ensureOpen() error {
	if archive.isClosed {
		return &ArchiveError{Message: "client is closed"}
	}
	return nil
}

func (archive *Archive) ensureNotReentrant() error {
	if archive.isInCallback {
		// TODO: AeronException is a RunTimeException - thus this is very critical
		return aeron.NewAeronError("reentrant calls not permitted during callbacks", aeron.AeronErrorCategory.ERROR)
	}
	return nil
}
