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

const ControlFragmentLimit = 10

// ControlResponsePoller Encapsulate the polling and decoding of archive control protocol response messages.
// Java - https://github.com/real-logic/aeron/blob/1.46.2/aeron-archive/src/main/java/io/aeron/archive/client/ControlResponsePoller.java
type ControlResponsePoller struct {
	// ControlResponse
	ControlSessionId int64
	CorrelationId    int64
	RelevantId       int64
	TemplateId       int16
	Version          int32
	Code             codecs.ControlResponseCodeEnum
	ErrorMessage     string

	// RecordingSignal
	RecordingSignal codecs.RecordingSignalEnum
	RecordingId     int64
	SubscriptionId  int64
	Position        int64

	// Challenge
	EncodedChallenge []byte

	IsPollComplete bool

	Subscription  *aeron.Subscription
	FragmentLimit int

	fragmentAssembler *aeron.ControlledFragmentAssembler
	errorHandler      func(error)

	// NOTE: Go Specific to deal with throwing exception - terminal error
	ArchiveError error
}

func NewControlResponsePoller(subscription *aeron.Subscription, fragmentLimit int) *ControlResponsePoller {
	poller := &ControlResponsePoller{}
	poller.resetValues()
	poller.Subscription = subscription
	poller.FragmentLimit = fragmentLimit
	poller.fragmentAssembler = aeron.NewControlledFragmentAssembler(
		poller.OnFragment, aeron.DefaultFragmentAssemblyBufferLength)
	poller.errorHandler = LoggingErrorListener
	return poller
}

func (poller *ControlResponsePoller) resetValues() {
	poller.ControlSessionId = aeron.NullValue
	poller.CorrelationId = aeron.NullValue
	poller.RelevantId = aeron.NullValue
	poller.TemplateId = aeron.NullValue
	poller.Version = 0
	poller.Code = codecs.ControlResponseCode.NullValue
	poller.ErrorMessage = ""
	poller.RecordingSignal = codecs.RecordingSignal.NullValue
	poller.RecordingId = aeron.NullValue
	poller.SubscriptionId = aeron.NullValue
	poller.Position = aeron.NullValue
	poller.EncodedChallenge = nil
	poller.IsPollComplete = false
	poller.ArchiveError = nil
}

func (poller *ControlResponsePoller) Poll() (workCount int) {
	if poller.IsPollComplete {
		poller.resetValues()
	}
	return poller.Subscription.ControlledPoll(poller.fragmentAssembler.OnFragment, poller.FragmentLimit)
}

func (poller *ControlResponsePoller) OnFragment(
	buffer *atomic.Buffer,
	offset int32,
	length int32,
	header *logbuffer.Header,
) term.ControlledPollAction {

	if poller.IsPollComplete {
		return term.ControlledPollActionAbort
	}

	var hdr codecs.SbeGoMessageHeader

	buf := new(bytes.Buffer)
	buffer.WriteBytes(buf, offset, length)

	marshaller := codecs.NewSbeGoMarshaller()
	if err := hdr.Decode(marshaller, buf); err != nil {
		poller.errorHandler(fmt.Errorf("ControlResponsePoller::onFragment() failed to decode poller message header: %w", err))
		return term.ControlledPollActionContinue
	}

	if hdr.SchemaId != codecIds.schemaId {
		// NOTE: not sure how to handle this, in Java an exception is thrown
		archiveErr := NewArchiveError(-1, -1, fmt.Sprintf("expected schemaId=%d, actual=%d", codecIds.schemaId, hdr.SchemaId))
		poller.ArchiveError = archiveErr
		poller.errorHandler(archiveErr)
	}

	poller.TemplateId = int16(hdr.TemplateId)

	switch uint16(poller.TemplateId) {
	case codecIds.controlResponse:
		var controlResponse = new(codecs.ControlResponse)
		logger.Debugf("Received pollerResponse: length %d", buf.Len())
		if err := controlResponse.Decode(marshaller, buf, hdr.Version, hdr.BlockLength, rangeChecking); err != nil {
			// Not much to be done here as we can't correlate
			err = fmt.Errorf("failed to decode poller response: %w", err)
			poller.errorHandler(err)
			return term.ControlledPollActionContinue
		}
		poller.CorrelationId = controlResponse.CorrelationId
		poller.ControlSessionId = controlResponse.ControlSessionId
		poller.RelevantId = controlResponse.RelevantId
		poller.Code = controlResponse.Code
		poller.Version = controlResponse.Version
		poller.ErrorMessage = string(controlResponse.ErrorMessage)
		poller.IsPollComplete = true
		return term.ControlledPollActionBreak

	case codecIds.challenge:
		var challenge = new(codecs.Challenge)
		if err := challenge.Decode(marshaller, buf, hdr.Version, hdr.BlockLength, rangeChecking); err != nil {
			// Not much to be done here as we can't correlate
			err = fmt.Errorf("failed to decode challenge: %w", err)
			poller.errorHandler(err)
			return term.ControlledPollActionContinue
		}
		poller.ControlSessionId = challenge.ControlSessionId
		poller.CorrelationId = challenge.CorrelationId
		poller.RelevantId = aeron.NullValue
		poller.Code = codecs.ControlResponseCode.NullValue
		poller.Version = challenge.Version
		poller.ErrorMessage = ""
		poller.EncodedChallenge = challenge.EncodedChallenge
		poller.IsPollComplete = true
		return term.ControlledPollActionBreak

	case codecIds.recordingSignalEvent:
		var recordingSignalEvent = new(codecs.RecordingSignalEvent)
		if err := recordingSignalEvent.Decode(marshaller, buf, hdr.Version, hdr.BlockLength, rangeChecking); err != nil {
			// Not much to be done here as we can't correlate
			err = fmt.Errorf("failed to decode recording signal: %w", err)
			poller.errorHandler(err)
			return term.ControlledPollActionContinue
		}
		poller.ControlSessionId = recordingSignalEvent.ControlSessionId
		poller.CorrelationId = recordingSignalEvent.CorrelationId
		poller.RecordingId = recordingSignalEvent.RecordingId
		poller.SubscriptionId = recordingSignalEvent.SubscriptionId
		poller.Position = recordingSignalEvent.Position
		poller.RecordingSignal = recordingSignalEvent.Signal
		poller.IsPollComplete = true
		return term.ControlledPollActionBreak

	default:
		logger.Debug("ControlResponsePoller: Insert decoder for type: %d", hdr.TemplateId)
	}
	return term.ControlledPollActionContinue
}

func (poller *ControlResponsePoller) WasChallenged() bool {
	return poller.EncodedChallenge != nil
}
