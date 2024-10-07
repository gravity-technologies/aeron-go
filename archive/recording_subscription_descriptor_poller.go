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

// RecordingSubscriptionDescriptorPoller Encapsulate the polling, decoding, dispatching of recording descriptors from an archive.
// Java - https://github.com/real-logic/aeron/blob/master/aeron-archive/src/main/java/io/aeron/archive/client/RecordingSubscriptionDescriptorPoller.java
type RecordingSubscriptionDescriptorPoller struct {
	ControlSessionId           int64
	CorrelationId              int64
	RemainingSubscriptionCount int32
	IsDispatchComplete         bool

	Subscription  *aeron.Subscription
	FragmentLimit int

	fragmentAssembler              *aeron.ControlledFragmentAssembler
	errorHandler                   func(error)
	recordingSignalConsumer        func(*codecs.RecordingSignalEvent)
	subscriptionDescriptorConsumer func(*codecs.RecordingSubscriptionDescriptor)

	// NOTE: Go Specific to deal with throwing exception - terminal error
	ArchiveError error
}

func NewRecordingSubscriptionDescriptorPoller(
	subscription *aeron.Subscription,
	errorHandler func(error),
	recordingSignalConsumer func(*codecs.RecordingSignalEvent),
	controlSessionId int64,
	fragmentLimit int) *RecordingSubscriptionDescriptorPoller {
	poller := &RecordingSubscriptionDescriptorPoller{}
	poller.Subscription = subscription
	poller.ControlSessionId = controlSessionId
	poller.FragmentLimit = fragmentLimit
	poller.fragmentAssembler = aeron.NewControlledFragmentAssembler(
		poller.OnFragment, aeron.DefaultFragmentAssemblyBufferLength)
	poller.errorHandler = errorHandler
	poller.recordingSignalConsumer = recordingSignalConsumer
	return poller
}

func (poller *RecordingSubscriptionDescriptorPoller) Poll() (workcount int) {
	if poller.IsDispatchComplete {
		poller.IsDispatchComplete = false
	}
	return poller.Subscription.ControlledPoll(poller.fragmentAssembler.OnFragment, poller.FragmentLimit)
}

// Reset the poller to dispatch the descriptors returned from a query.
func (poller *RecordingSubscriptionDescriptorPoller) Reset(correlationId int64, subscriptionCount int32, consumer func(*codecs.RecordingSubscriptionDescriptor)) {
	poller.CorrelationId = correlationId
	poller.RemainingSubscriptionCount = subscriptionCount
	poller.IsDispatchComplete = false
	poller.subscriptionDescriptorConsumer = consumer
	// NOTE: Go Specific
	poller.ArchiveError = nil
}

func (poller *RecordingSubscriptionDescriptorPoller) OnFragment(
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
		poller.errorHandler(fmt.Errorf("RecordingSubscriptionDescriptorPoller::onFragment() failed to decode poller message header: %w", err))
		return term.ControlledPollActionContinue
	}

	if hdr.SchemaId != codecIds.schemaId {
		// NOTE: not sure how to handle this, in Java an exception is thrown
		archiveErr := NewArchiveError(-1, -1, fmt.Sprintf("expected schemaId=%d, actual=%d", codecIds.schemaId, hdr.SchemaId))
		poller.ArchiveError = archiveErr
		poller.errorHandler(archiveErr)
		// TODO: should we abort/break here ???
		return term.ControlledPollActionBreak
	}

	switch hdr.TemplateId {
	case codecIds.controlResponse:
		var controlResponse = new(codecs.ControlResponse)
		logger.Debugf("Received controlResponse: length %d", buf.Len())
		if err := controlResponse.Decode(marshaller, buf, hdr.Version, hdr.BlockLength, rangeChecking); err != nil {
			// Not much to be done here as we can't correlate
			err = fmt.Errorf("failed to decode poller response: %w", err)
			poller.errorHandler(err)
			return term.ControlledPollActionContinue
		}

		if controlResponse.ControlSessionId == poller.ControlSessionId {
			code := controlResponse.Code
			responseCorrelationId := controlResponse.CorrelationId

			if code == codecs.ControlResponseCode.SUBSCRIPTION_UNKNOWN &&
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
				// TODO: should we abort/break here ???
				return term.ControlledPollActionBreak
			}
		}

	case codecIds.recordingSubscriptionDescriptor:
		var recordingSubscriptionDescriptor = new(codecs.RecordingSubscriptionDescriptor)
		if err := recordingSubscriptionDescriptor.Decode(marshaller, buf, hdr.Version, hdr.BlockLength, rangeChecking); err != nil {
			// Not much to be done here as we can't correlate
			err = fmt.Errorf("failed to decode recordingSubscriptionDescriptor: %w", err)
			poller.errorHandler(err)
			return term.ControlledPollActionContinue
		}

		if recordingSubscriptionDescriptor.ControlSessionId == poller.ControlSessionId &&
			recordingSubscriptionDescriptor.CorrelationId == poller.CorrelationId {
			poller.subscriptionDescriptorConsumer(
				&codecs.RecordingSubscriptionDescriptor{
					ControlSessionId: poller.ControlSessionId,
					CorrelationId:    poller.CorrelationId,
					SubscriptionId:   recordingSubscriptionDescriptor.SubscriptionId,
					StreamId:         recordingSubscriptionDescriptor.StreamId,
					StrippedChannel:  recordingSubscriptionDescriptor.StrippedChannel,
				})

			poller.RemainingSubscriptionCount--
			if 0 == poller.RemainingSubscriptionCount {
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
