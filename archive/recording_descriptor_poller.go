package archive

import (
	"bytes"
	"fmt"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/archive/codecs"
)

// RecordingDescriptorPoller encapsulate the polling, decoding, dispatching of recording descriptors from an archive.
// https://github.com/real-logic/aeron/blob/1.46.2/aeron-archive/src/main/java/io/aeron/archive/client/RecordingDescriptorPoller.java
type RecordingDescriptorPoller struct {
	ControlSessionId     int64
	CorrelationId        int64
	RemainingRecordCount int32
	IsDispatchComplete   bool
	Subscription         *aeron.Subscription
	FragmentLimit        int

	fragmentAssembler           *aeron.ControlledFragmentAssembler
	errorHandler                func(error)
	recordingDescriptorConsumer func(*codecs.RecordingDescriptor)
	recordingSignalConsumer     func(*codecs.RecordingSignalEvent)

	// NOTE: GO Specific
	ArchiveError error
}

func NewRecordingDescriptorPoller(
	subbscription *aeron.Subscription,
	errorHandler func(error),
	recordingSignalConsumer func(*codecs.RecordingSignalEvent),
	controlSessionId int64,
	fragmentLimit int,
) *RecordingDescriptorPoller {
	poller := &RecordingDescriptorPoller{}
	poller.ControlSessionId = controlSessionId
	poller.FragmentLimit = fragmentLimit
	poller.fragmentAssembler = aeron.NewControlledFragmentAssembler(
		poller.OnFragment, aeron.DefaultFragmentAssemblyBufferLength)
	poller.errorHandler = errorHandler
	poller.recordingSignalConsumer = recordingSignalConsumer

	if errorHandler == nil {
		poller.errorHandler = LoggingErrorListener
	}
	if recordingSignalConsumer == nil {
		poller.recordingSignalConsumer = LoggingRecordingSignalListener
	}
	return poller
}

func (poller *RecordingDescriptorPoller) Poll() (workcount int) {
	if poller.IsDispatchComplete {
		poller.IsDispatchComplete = false
	}
	return poller.Subscription.ControlledPoll(poller.fragmentAssembler.OnFragment, poller.FragmentLimit)
}

// Reset the poller to dispatch the descriptors returned from a query.
func (poller *RecordingDescriptorPoller) Reset(correlationId int64, recordCount int32, consumer func(*codecs.RecordingDescriptor)) {
	poller.CorrelationId = correlationId
	poller.RemainingRecordCount = recordCount
	poller.IsDispatchComplete = false
	poller.recordingDescriptorConsumer = consumer
	// NOTE: Go Specific
	poller.ArchiveError = nil
}

func (poller *RecordingDescriptorPoller) OnFragment(
	buffer *atomic.Buffer,
	offset int32,
	length int32,
	header *logbuffer.Header,
) term.ControlledPollAction {

	if poller.IsDispatchComplete {
		return term.ControlledPollActionAbort
	}

	var hdr codecs.SbeGoMessageHeader

	buf := new(bytes.Buffer)
	buffer.WriteBytes(buf, offset, length)

	marshaller := codecs.NewSbeGoMarshaller()
	if err := hdr.Decode(marshaller, buf); err != nil {
		poller.errorHandler(fmt.Errorf("RecordingDescriptorPoller::onFragment() failed to decode poller message header: %w", err))
		return term.ControlledPollActionContinue
	}

	if hdr.SchemaId != codecIds.schemaId {
		// NOTE: not sure how to handle this, in Java an exception is thrown
		archiveErr := NewArchiveError(-1, -1, fmt.Sprintf("expected schemaId=%d, actual=%d", codecIds.schemaId, hdr.SchemaId))
		poller.ArchiveError = archiveErr
		poller.errorHandler(archiveErr)
		// TODO: should we abort/break or cont flow here ???
		return term.ControlledPollActionBreak
	}

	switch hdr.TemplateId {
	case codecIds.controlResponse:
		var controlResponse = new(codecs.ControlResponse)
		logger.Debugf("Received controlResponse: length %d", buf.Len())
		if err := controlResponse.Decode(marshaller, buf, hdr.Version, hdr.BlockLength, rangeChecking); err != nil {
			// Not much to be done here as we can't correlate
			err = fmt.Errorf("failed to decode controlResponse: %w", err)
			poller.errorHandler(err)
			return term.ControlledPollActionContinue
		}

		if controlResponse.ControlSessionId == poller.ControlSessionId {
			code := controlResponse.Code
			responseCorrelationId := controlResponse.CorrelationId

			if code == codecs.ControlResponseCode.RECORDING_UNKNOWN &&
				responseCorrelationId == poller.CorrelationId {
				poller.IsDispatchComplete = true
				return term.ControlledPollActionBreak
			}

			if code == codecs.ControlResponseCode.ERROR {
				err := NewArchiveError(
					responseCorrelationId,
					int(controlResponse.RelevantId),
					fmt.Sprintf("response for correlationId=%d, error: %s", poller.CorrelationId, controlResponse.ErrorMessage))

				if responseCorrelationId == poller.CorrelationId {
					poller.ArchiveError = err
				} else if poller.errorHandler != nil {
					poller.errorHandler(err)

				}
				// TODO: should we abort/break or cont flow here ???
				return term.ControlledPollActionBreak
			}
		}

	case codecIds.recordingDescriptor:
		var recordingDescriptor = new(codecs.RecordingDescriptor)
		if err := recordingDescriptor.Decode(marshaller, buf, hdr.Version, hdr.BlockLength, rangeChecking); err != nil {
			// Not much to be done here as we can't correlate
			err = fmt.Errorf("failed to decode recordingDescriptor: %w", err)
			poller.errorHandler(err)
			return term.ControlledPollActionContinue
		}

		if recordingDescriptor.ControlSessionId == poller.ControlSessionId &&
			recordingDescriptor.CorrelationId == poller.CorrelationId {
			poller.recordingDescriptorConsumer(
				&codecs.RecordingDescriptor{
					ControlSessionId:  poller.ControlSessionId,
					CorrelationId:     poller.CorrelationId,
					RecordingId:       recordingDescriptor.RecordingId,
					StartTimestamp:    recordingDescriptor.StartTimestamp,
					StopTimestamp:     recordingDescriptor.StopTimestamp,
					StartPosition:     recordingDescriptor.StartPosition,
					StopPosition:      recordingDescriptor.StopPosition,
					InitialTermId:     recordingDescriptor.InitialTermId,
					SegmentFileLength: recordingDescriptor.SegmentFileLength,
					TermBufferLength:  recordingDescriptor.TermBufferLength,
					MtuLength:         recordingDescriptor.MtuLength,
					SessionId:         recordingDescriptor.SessionId,
					StreamId:          recordingDescriptor.StreamId,
					StrippedChannel:   recordingDescriptor.StrippedChannel,
					OriginalChannel:   recordingDescriptor.OriginalChannel,
					SourceIdentity:    recordingDescriptor.SourceIdentity,
				})

			poller.RemainingRecordCount--
			if 0 == poller.RemainingRecordCount {
				poller.IsDispatchComplete = true
				return term.ControlledPollActionBreak
			}
		}

	case codecIds.recordingSignalEvent:
		var recordingSignalEvent = new(codecs.RecordingSignalEvent)
		if err := recordingSignalEvent.Decode(marshaller, buf, hdr.Version, hdr.BlockLength, rangeChecking); err != nil {
			// Not much to be done here as we can't correlate
			err = fmt.Errorf("failed to decode recordingSignalEvent: %w", err)
			poller.errorHandler(err)
			return term.ControlledPollActionContinue
		}

		if poller.ControlSessionId == recordingSignalEvent.ControlSessionId {
			poller.recordingSignalConsumer(
				&codecs.RecordingSignalEvent{
					ControlSessionId: recordingSignalEvent.ControlSessionId,
					CorrelationId:    recordingSignalEvent.CorrelationId,
					RecordingId:      recordingSignalEvent.RecordingId,
					SubscriptionId:   recordingSignalEvent.SubscriptionId,
					Position:         recordingSignalEvent.Position,
					Signal:           recordingSignalEvent.Signal,
				})
		}
	}

	return term.ControlledPollActionContinue
}
