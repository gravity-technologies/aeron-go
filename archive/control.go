// Copyright (C) 2021-2022 Talos, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package archive

import (
	"bytes"
	"fmt"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/archive/codecs"
)

const controlFragmentLimit = 10

// Control contains everything required for the archive subscription/response side
type Control struct {
	Subscription *aeron.Subscription
	State        controlState

	// Polling results
	Results ControlResults

	archive *Archive // link to parent

	controlResponsePoller                 *ControlResponsePoller
	recordingDescriptorPoller             *RecordingDescriptorPoller
	recordingSubscriptionDescriptorPoller *RecordingSubscriptionDescriptorPoller
}

// ControlResults for holding state over a Control request/response
// The polling mechanism is not parameterized so we need to set state for the results as we go
// These pieces are filled out by various ResponsePollers which will set IsPollComplete to true
type ControlResults struct {
	// ControlResponse
	ControlSessionId int64
	CorrelationId    int64
	RelevantId       int64
	Code             codecs.ControlResponseCodeEnum
	Version          int32
	ErrorMessage     []uint8

	// RecordingSignal
	RecordingSignal codecs.RecordingSignalEnum
	RecordingId     int64
	SubscriptionId  int64
	Position        int64

	// Challenge
	EncodedChallenge []uint8

	// Poller complete status
	TemplateId     int16
	IsPollComplete bool

	ControlResponse                  *codecs.ControlResponse
	RecordingDescriptors             []*codecs.RecordingDescriptor
	RecordingSubscriptionDescriptors []*codecs.RecordingSubscriptionDescriptor
	FragmentsReceived                int
	ErrorResponse                    error // Used by PollForErrorResponse
}

// PollContext contains the information we'll need in the image Poll()
// callback to match against our request or for async events to invoke
// the appropriate listener
type PollContext struct {
	control       *Control
	correlationID int64
}

// Archive Connection State used internally for connection establishment
const (
	ControlStateError              = -1
	ControlStateNew                = iota
	ControlStateConnectRequestSent = iota
	ControlStateChallenged         = iota
	ControlStateConnectRequestOk   = iota
	ControlStateConnected          = iota
	ControlStateTimedOut           = iota
)

var ErrNotConnected = fmt.Errorf("not connected")

// Used internally to handle connection state
type controlState struct {
	state int
	err   error
}

// CodecIds stops us allocating every object when we need only one
// Arguably SBE should give us a static value
type CodecIds struct {
	controlResponse                 uint16
	challenge                       uint16
	recordingDescriptor             uint16
	recordingSubscriptionDescriptor uint16
	recordingSignalEvent            uint16
	recordingStarted                uint16
	recordingProgress               uint16
	recordingStopped                uint16
	schemaId                        uint16
}

var codecIds CodecIds

func init() {
	var controlResponse codecs.ControlResponse
	var challenge codecs.Challenge
	var recordingDescriptor codecs.RecordingDescriptor
	var recordingSubscriptionDescriptor codecs.RecordingSubscriptionDescriptor
	var recordingSignalEvent codecs.RecordingSignalEvent
	var recordingStarted = new(codecs.RecordingStarted)
	var recordingProgress = new(codecs.RecordingProgress)
	var recordingStopped = new(codecs.RecordingStopped)

	codecIds.controlResponse = controlResponse.SbeTemplateId()
	codecIds.challenge = challenge.SbeTemplateId()
	codecIds.recordingDescriptor = recordingDescriptor.SbeTemplateId()
	codecIds.recordingSubscriptionDescriptor = recordingSubscriptionDescriptor.SbeTemplateId()
	codecIds.recordingSignalEvent = recordingSignalEvent.SbeTemplateId()
	codecIds.recordingStarted = recordingStarted.SbeTemplateId()
	codecIds.recordingProgress = recordingProgress.SbeTemplateId()
	codecIds.recordingStopped = recordingStopped.SbeTemplateId()
	codecIds.schemaId = controlResponse.SbeSchemaId()
}

func controlFragmentHandler(context interface{}, buffer *atomic.Buffer, offset int32, length int32, header *logbuffer.Header) (action term.ControlledPollAction) {
	action = term.ControlledPollActionContinue

	pollContext, ok := context.(*PollContext)
	if !ok {
		logger.Errorf("context conversion failed")
		return
	}

	if pollContext.control.Results.IsPollComplete {
		return term.ControlledPollActionAbort
	}

	logger.Debugf("controlFragmentHandler: correlationID:%d offset:%d length:%d header:%#v", pollContext.correlationID, offset, length, header)
	var hdr codecs.SbeGoMessageHeader

	buf := new(bytes.Buffer)
	buffer.WriteBytes(buf, offset, length)

	marshaller := codecs.NewSbeGoMarshaller()
	if err := hdr.Decode(marshaller, buf); err != nil {
		// Not much to be done here as we can't really tell what went wrong
		err2 := fmt.Errorf("controlFragmentHandler() failed to decode control message header: %w", err)
		// Call the global error handler, ugly but it's all we've got
		if pollContext.control.archive.Listeners.ErrorListener != nil {
			pollContext.control.archive.Listeners.ErrorListener(err2)
		}
		return
	}

	// Look up our control
	c, ok := correlations.Load(pollContext.correlationID)
	if !ok {
		// something has gone horribly wrong and we can't correlate
		if pollContext.control.archive.Listeners.ErrorListener != nil {
			pollContext.control.archive.Listeners.ErrorListener(fmt.Errorf("failed to locate control via correlationID %d", pollContext.correlationID))
		}
		logger.Debugf("failed to locate control via correlationID %d", pollContext.correlationID)
		return
	}
	control := c.(*Control)

	switch hdr.TemplateId {
	case codecIds.controlResponse:
		var controlResponse = new(codecs.ControlResponse)
		logger.Debugf("controlFragmentHandler/controlResponse: Received controlResponse: length %d", buf.Len())
		if err := controlResponse.Decode(marshaller, buf, hdr.Version, hdr.BlockLength, rangeChecking); err != nil {
			// Not much to be done here as we can't see what's gone wrong
			err2 := fmt.Errorf("controlFragmentHandler failed to decode control response:%w", err)
			// Call the global error handler, ugly but it's all we've got
			if pollContext.control.archive.Listeners.ErrorListener != nil {
				pollContext.control.archive.Listeners.ErrorListener(err2)
			}
			return
		}

		// Check this was for us
		if controlResponse.ControlSessionId == control.archive.SessionID && controlResponse.CorrelationId == pollContext.correlationID {
			// Set our state to let the caller of Poll() which triggered this know they have something
			// We're basically finished so prepare our OOB return values and log some info if we can
			logger.Debugf("controlFragmentHandler/controlResponse: received for sessionID:%d, correlationID:%d", controlResponse.ControlSessionId, controlResponse.CorrelationId)
			control.Results.ControlResponse = controlResponse
			control.Results.IsPollComplete = true

			return term.ControlledPollActionBreak
		} else {
			logger.Debugf("controlFragmentHandler/controlResponse ignoring sessionID:%d, correlationID:%d", controlResponse.ControlSessionId, controlResponse.CorrelationId)
		}

	case codecIds.recordingSignalEvent:
		var recordingSignalEvent = new(codecs.RecordingSignalEvent)

		if err := recordingSignalEvent.Decode(marshaller, buf, hdr.Version, hdr.BlockLength, rangeChecking); err != nil {
			// Not much to be done here as we can't really tell what went wrong
			err2 := fmt.Errorf("ControlFragmentHandler failed to decode recording signal: %w", err)
			if pollContext.control.archive.Listeners.ErrorListener != nil {
				pollContext.control.archive.Listeners.ErrorListener(err2)
			}
		}
		if pollContext.control.archive.Listeners.RecordingSignalListener != nil {
			pollContext.control.archive.Listeners.RecordingSignalListener(recordingSignalEvent)
		}

	// These can happen when testing/reconnecting or if multiple clients are on the same channel/stream
	case codecIds.recordingDescriptor:
		logger.Debugf("controlFragmentHandler: ignoring RecordingDescriptor type %d", hdr.TemplateId)
	case codecIds.recordingSubscriptionDescriptor:
		logger.Debugf("controlFragmentHandler: ignoring RecordingSubscriptionDescriptor type %d", hdr.TemplateId)

	default:
		// This can happen when testing/adding new functionality
		fmt.Printf("controlFragmentHandler: Unexpected message type %d\n", hdr.TemplateId)
	}
	return
}

// ConnectionControlFragmentHandler is the connection handling specific fragment handler.
// This mechanism only alows us to pass results back via global state which we do in control.State
func ConnectionControlFragmentHandler(context *PollContext, buffer *atomic.Buffer, offset int32, length int32, header *logbuffer.Header) {
	logger.Debugf("ConnectionControlFragmentHandler: correlationID:%d offset:%d length: %d header: %#v", context.correlationID, offset, length, header)

	var hdr codecs.SbeGoMessageHeader

	buf := new(bytes.Buffer)
	buffer.WriteBytes(buf, offset, length)

	marshaller := codecs.NewSbeGoMarshaller()
	if err := hdr.Decode(marshaller, buf); err != nil {
		// Not much to be done here as we can't correlate
		err2 := fmt.Errorf("ConnectionControlFragmentHandler() failed to decode control message header: %w", err)
		// Call the global error handler, ugly but it's all we've got
		if context.control.archive.Listeners.ErrorListener != nil {
			context.control.archive.Listeners.ErrorListener(err2)
		}
	}

	switch hdr.TemplateId {
	case codecIds.controlResponse:
		var controlResponse = new(codecs.ControlResponse)
		logger.Debugf("Received controlResponse: length %d", buf.Len())
		if err := controlResponse.Decode(marshaller, buf, hdr.Version, hdr.BlockLength, rangeChecking); err != nil {
			// Not much to be done here as we can't correlate
			err2 := fmt.Errorf("ConnectionControlFragmentHandler failed to decode control response: %w", err)
			if context.control.archive.Listeners.ErrorListener != nil {
				context.control.archive.Listeners.ErrorListener(err2)
			}
			logger.Debugf("ConnectionControlFragmentHandler failed to decode control response: %w", err)
			return
		}

		// Look this message up
		c, ok := correlations.Load(controlResponse.CorrelationId)
		if !ok {
			logger.Debugf("connectionControlFragmentHandler/controlResponse: ignoring correlationID=%d [%s]\n%#v", controlResponse.CorrelationId, string(controlResponse.ErrorMessage), controlResponse)
			return
		}
		control := c.(*Control)

		// Check this was for us
		if controlResponse.CorrelationId == context.correlationID {
			// Check result
			if controlResponse.Code != codecs.ControlResponseCode.OK {
				control.State.state = ControlStateError
				control.State.err = fmt.Errorf("Control Response failure: %s", controlResponse.ErrorMessage)
				if context.control.archive.Listeners.ErrorListener != nil {
					context.control.archive.Listeners.ErrorListener(control.State.err)
				}
				return
			}

			// assert state change
			if control.State.state != ControlStateConnectRequestSent {
				control.State.state = ControlStateError
				control.State.err = fmt.Errorf("Control Response not expecting response")
				if context.control.archive.Listeners.ErrorListener != nil {
					context.control.archive.Listeners.ErrorListener(control.State.err)
				}
			}

			// Looking good, so update state and store the SessionID
			control.State.state = ControlStateConnected
			control.State.err = nil
			control.archive.SessionID = controlResponse.ControlSessionId
		} else {
			// It's conceivable if the same application is making concurrent connection attempts using
			// the same channel/stream that we can reach here which is our parent's problem
			logger.Debugf("connectionControlFragmentHandler/controlResponse: ignoring correlationID=%d", controlResponse.CorrelationId)
		}

	case codecIds.challenge:
		var challenge = new(codecs.Challenge)

		if err := challenge.Decode(marshaller, buf, hdr.Version, hdr.BlockLength, rangeChecking); err != nil {
			// Not much to be done here as we can't correlate
			err2 := fmt.Errorf("ControlFragmentHandler failed to decode challenge: %w", err)
			if context.control.archive.Listeners.ErrorListener != nil {
				context.control.archive.Listeners.ErrorListener(err2)
			}
		}

		logger.Infof("ControlFragmentHandler: challenge:%s, session:%d, correlationID:%d", challenge.EncodedChallenge, challenge.ControlSessionId, challenge.CorrelationId)

		// Look this message up
		c, ok := correlations.Load(challenge.CorrelationId)
		if !ok {
			logger.Debugf("connectionControlFragmentHandler/controlResponse: ignoring correlationID=%d", challenge.CorrelationId)
			return
		}
		control := c.(*Control)

		// Check this was for us
		if challenge.CorrelationId == context.correlationID {

			// Check the challenge is expected iff our option for this is not nil
			if control.archive.Options.AuthChallenge != nil {
				if !bytes.Equal(control.archive.Options.AuthChallenge, challenge.EncodedChallenge) {
					control.State.err = fmt.Errorf("ChallengeResponse Unexpected: expected:%v received:%v", control.archive.Options.AuthChallenge, challenge.EncodedChallenge)
					return
				}
			}

			// Send the response
			// Looking good, so update state and store the SessionID
			control.State.state = ControlStateChallenged
			control.State.err = nil
			control.archive.SessionID = challenge.ControlSessionId
			control.archive.Proxy.ChallengeResponse(challenge.CorrelationId, control.archive.Options.AuthResponse)
		} else {
			// It's conceivable if the same application is making concurrent connection attempts using
			// the same channel/stream that we can reach here which is our parent's problem
			logger.Debugf("connectionControlFragmentHandler/challengr: ignoring correlationID=%d", challenge.CorrelationId)
		}

	// These can happen when testing/reconnecting or if multiple clients are on the same channel/stream
	case codecIds.recordingDescriptor:
		logger.Debugf("connectionControlFragmentHandler: ignoring RecordingDescriptor type %d", hdr.TemplateId)
	case codecIds.recordingSubscriptionDescriptor:
		logger.Debugf("connectionControlFragmentHandler: ignoring RecordingSubscriptionDescriptor type %d", hdr.TemplateId)
	case codecIds.recordingSignalEvent:
		logger.Debugf("connectionControlFragmentHandler: ignoring recordingSignalEvent type %d", hdr.TemplateId)

	default:
		fmt.Printf("ConnectionControlFragmentHandler: Insert decoder for type: %d", hdr.TemplateId)
	}
}

// DescriptorFragmentHandler is used to poll for descriptors (both recording and subscription)
// The current subscription handler doesn't provide a mechanism for passing a context
// so we return data via the control's Results
func DescriptorFragmentHandler(context interface{}, buffer *atomic.Buffer, offset int32, length int32, header *logbuffer.Header) {
	pollContext, ok := context.(*PollContext)
	if !ok {
		logger.Errorf("context conversion failed")
		return
	}

	// logger.Debugf("DescriptorFragmentHandler: correlationID:%d offset:%d length: %d header: %#v\n", pollContext.correlationID, offset, length, header)

	var hdr codecs.SbeGoMessageHeader

	buf := new(bytes.Buffer)
	buffer.WriteBytes(buf, offset, length)

	marshaller := codecs.NewSbeGoMarshaller()
	if err := hdr.Decode(marshaller, buf); err != nil {
		// Not much to be done here as we can't correlate
		err2 := fmt.Errorf("DescriptorFragmentHandler() failed to decode control message header: %w", err)
		// Call the global error handler, ugly but it's all we've got
		if pollContext.control.archive.Listeners.ErrorListener != nil {
			pollContext.control.archive.Listeners.ErrorListener(err2)
		}
		return
	}

	// Look up our control
	c, ok := correlations.Load(pollContext.correlationID)
	if !ok {
		// something has gone horribly wrong and we can't correlate
		if pollContext.control.archive.Listeners.ErrorListener != nil {
			pollContext.control.archive.Listeners.ErrorListener(fmt.Errorf("failed to locate control via correlationID %d", pollContext.correlationID))
		}
		logger.Debugf("failed to locate control via correlationID %d", pollContext.correlationID)
		return
	}
	control := c.(*Control)

	switch hdr.TemplateId {
	case codecIds.recordingDescriptor:
		var recordingDescriptor = new(codecs.RecordingDescriptor)
		logger.Debugf("Received RecordingDescriptor: length %d", buf.Len())
		if err := recordingDescriptor.Decode(marshaller, buf, hdr.Version, hdr.BlockLength, rangeChecking); err != nil {
			// Not much to be done here as we can't correlate
			err2 := fmt.Errorf("failed to decode RecordingDescriptor: %w", err)
			if pollContext.control.archive.Listeners.ErrorListener != nil {
				pollContext.control.archive.Listeners.ErrorListener(err2)
			}
			return
		}
		logger.Debugf("RecordingDescriptor: %#v", recordingDescriptor)

		// Check this was for us
		if recordingDescriptor.ControlSessionId == control.archive.SessionID && recordingDescriptor.CorrelationId == pollContext.correlationID {
			// Set our state to let the caller of Poll() which triggered this know they have something
			control.Results.RecordingDescriptors = append(control.Results.RecordingDescriptors, recordingDescriptor)
			control.Results.FragmentsReceived++
		} else {
			logger.Debugf("descriptorFragmentHandler/recordingDescriptor ignoring sessionID:%d, pollContext.correlationID:%d", recordingDescriptor.ControlSessionId, recordingDescriptor.CorrelationId)
		}

	case codecIds.recordingSubscriptionDescriptor:
		logger.Debugf("Received RecordingSubscriptionDescriptor: length %d", buf.Len())
		var recordingSubscriptionDescriptor = new(codecs.RecordingSubscriptionDescriptor)
		if err := recordingSubscriptionDescriptor.Decode(marshaller, buf, hdr.Version, hdr.BlockLength, rangeChecking); err != nil {
			// Not much to be done here as we can't correlate
			err2 := fmt.Errorf("failed to decode RecordingSubscriptioDescriptor: %w", err)
			if pollContext.control.archive.Listeners.ErrorListener != nil {
				pollContext.control.archive.Listeners.ErrorListener(err2)
			}
			return
		}

		// Check this was for us
		if recordingSubscriptionDescriptor.ControlSessionId == control.archive.SessionID && recordingSubscriptionDescriptor.CorrelationId == pollContext.correlationID {
			// Set our state to let the caller of Poll() which triggered this know they have something
			control.Results.RecordingSubscriptionDescriptors = append(control.Results.RecordingSubscriptionDescriptors, recordingSubscriptionDescriptor)
			control.Results.FragmentsReceived++
		} else {
			logger.Debugf("descriptorFragmentHandler/recordingSubscriptionDescriptor ignoring sessionID:%d, correlationID:%d", recordingSubscriptionDescriptor.ControlSessionId, recordingSubscriptionDescriptor.CorrelationId)
		}

	case codecIds.controlResponse:
		var controlResponse = new(codecs.ControlResponse)
		logger.Debugf("Received controlResponse: length %d", buf.Len())
		if err := controlResponse.Decode(marshaller, buf, hdr.Version, hdr.BlockLength, rangeChecking); err != nil {
			// Not much to be done here as we can't correlate
			err2 := fmt.Errorf("failed to decode control response: %w", err)
			if pollContext.control.archive.Listeners.ErrorListener != nil {
				pollContext.control.archive.Listeners.ErrorListener(err2)
			}
			return
		}

		// Check this was for us
		if controlResponse.ControlSessionId == control.archive.SessionID {
			// RECORDING_UNKNOWN expected if there are no more results
			if controlResponse.CorrelationId == pollContext.correlationID && controlResponse.Code == codecs.ControlResponseCode.RECORDING_UNKNOWN {
				// Set our state to let the caller of Poll() which triggered this know they have something
				// We're basically finished so prepare our OOB return values and log some info if we can
				logger.Debugf("descriptorFragmentHandler/controlResponse: received for sessionID:%d, correlationID:%d", controlResponse.ControlSessionId, controlResponse.CorrelationId)
				control.Results.ControlResponse = controlResponse
				control.Results.IsPollComplete = true
				return
			} else if controlResponse.Code == codecs.ControlResponseCode.ERROR {
				// Unexpected so log but we deal with it in the parent
				logger.Debugf("ControlResponse error ERROR: %s\n%#v", controlResponse.ErrorMessage, controlResponse)
				control.Results.ControlResponse = controlResponse
				control.Results.IsPollComplete = true
				return
			} else {
				logger.Debugf("descriptorFragmentHandler/controlResponse ignoring sessionID:%d, correlationID:%d", controlResponse.ControlSessionId, controlResponse.CorrelationId)
			}
		}

	case codecIds.recordingSignalEvent:
		var recordingSignalEvent = new(codecs.RecordingSignalEvent)
		if err := recordingSignalEvent.Decode(marshaller, buf, hdr.Version, hdr.BlockLength, rangeChecking); err != nil {
			// Not much to be done here as we can't correlate
			err2 := fmt.Errorf("failed to decode recording signal: %w", err)
			if pollContext.control.archive.Listeners.ErrorListener != nil {
				pollContext.control.archive.Listeners.ErrorListener(err2)
			}
			return

		}
		if pollContext.control.archive.Listeners.RecordingSignalListener != nil {
			pollContext.control.archive.Listeners.RecordingSignalListener(recordingSignalEvent)
		}

	default:
		logger.Debug("descriptorFragmentHandler: Insert decoder for type: %d", hdr.TemplateId)
	}
}

// PollForDescriptors to poll for recording descriptors, adding them to the set in the control
func (control *Control) PollForDescriptors(correlationID int64, sessionID int64, fragmentsWanted int32) error {

	// Update our globals in case they've changed so we use the current state in our callback
	rangeChecking = control.archive.Options.RangeChecking

	control.Results.ControlResponse = nil                  // Clear old results
	control.Results.IsPollComplete = false                 // Clear completion flag
	control.Results.RecordingDescriptors = nil             // Clear previous results
	control.Results.RecordingSubscriptionDescriptors = nil // Clear previous results
	control.Results.FragmentsReceived = 0                  // Reset our loop results count

	start := time.Now()
	descriptorCount := 0
	pollContext := PollContext{control, correlationID}

	for !control.Results.IsPollComplete {
		logger.Debugf("PollForDescriptors(%d:%d, %d)", correlationID, sessionID, int(fragmentsWanted)-descriptorCount)
		fragments := control.poll(
			func(buf *atomic.Buffer, offset int32, length int32, header *logbuffer.Header) term.ControlledPollAction {
				DescriptorFragmentHandler(&pollContext, buf, offset, length, header)
				return term.ControlledPollActionContinue
			}, int(fragmentsWanted)-descriptorCount)
		logger.Debugf("Poll(%d:%d) returned %d fragments", correlationID, sessionID, fragments)
		descriptorCount = len(control.Results.RecordingDescriptors) + len(control.Results.RecordingSubscriptionDescriptors)

		// A control response may have told us we're complete or we may have all we asked for
		if control.Results.IsPollComplete || descriptorCount >= int(fragmentsWanted) {

			logger.Debugf("PollNextDescriptor(%d:%d) complete", correlationID, sessionID)
			return nil
		}

		// Check wer're live
		if control.Subscription.IsClosed() {
			return fmt.Errorf("response channel from archive is not connected")
		}

		// Check timeout
		if time.Since(start) > control.archive.Options.Timeout {
			return fmt.Errorf("PollNextDescriptor timeout waiting for correlationID %d", correlationID)
		}

		// If we received something then loop straight away
		if fragments > 0 {
			logger.Debugf("PollForDescriptors(%d:%d) looping with %d of %d", correlationID, sessionID, control.Results.FragmentsReceived, fragmentsWanted)
			continue
		}

		// If we are yet to receive anything then idle
		if descriptorCount == 0 {
			logger.Debugf("PollForDescriptors(%d:%d) idling with %d of %d", correlationID, sessionID, control.Results.FragmentsReceived, fragmentsWanted)
			control.archive.Options.IdleStrategy.Idle(0)
		}

	}
	return nil
}

// ----- Rewrite from Java ---- //

// PollForErrorResponse polls the response stream once for an error
// If another message is present then it will be skipped over
// so only call when not expecting another response. If not connected then return NOT_CONNECTED_MSG
// return the error otherwise nil if no error is found.
// Java - https://github.com/real-logic/aeron/blob/1.46.2/aeron-archive/src/main/java/io/aeron/archive/client/AeronArchive.java#L440
func (control *Control) PollForErrorResponse() (int, error) {
	poller := control.controlResponsePoller

	if !poller.Subscription.IsConnected() {
		return aeron.NullValue, ErrNotConnected
	}

	if poller.Poll() != 0 && poller.IsPollComplete {
		if poller.ControlSessionId == control.archive.SessionID {
			if poller.Code == codecs.ControlResponseCode.ERROR {
				archiveErr := NewArchiveError(poller.CorrelationId, int(poller.RelevantId), fmt.Sprintf("PollForErrorResponse received a ControlResponse (correlationId:%d Code:ERROR error=\"%s\")", poller.CorrelationId, poller.ErrorMessage))
				return aeron.NullValue, archiveErr
			} else if poller.TemplateId == int16(codecIds.recordingSignalEvent) {
				control.dispatchRecordingSignal()
			}
		}
	}

	// If we polled and did nothing then return
	return aeron.NullValue, nil
}

// PollForResponse polls for a specific correlationId
// Returns (relevantId, nil) on success, (-1 or relevantId, error) on failure
// Java - https://github.com/real-logic/aeron/blob/1.46.2/aeron-archive/src/main/java/io/aeron/archive/client/AeronArchive.java#L2345
func (control *Control) PollForResponse(correlationId int64, controlSessionId int64) (int64, error) {
	logger.Debugf("PollForResponse(%d:%d)", correlationId, controlSessionId)

	deadline := time.Now().Add(control.archive.Options.Timeout)
	poller := control.controlResponsePoller

	for {
		err := control.pollNextResponse(correlationId, deadline, poller, controlSessionId)
		if err != nil {
			LoggingErrorListener(err)
			return aeron.NullValue, err
		}

		if poller.ControlSessionId != controlSessionId {
			continue
		}

		code := poller.Code
		if code == codecs.ControlResponseCode.ERROR {
			archiveErr := NewArchiveError(correlationId, int(poller.RelevantId), fmt.Sprintf("response for correlationId=%d, error: %s", correlationId, poller.ErrorMessage))

			if poller.CorrelationId == correlationId {
				return aeron.NullValue, archiveErr
			} else if control.archive.Listeners.ErrorListener != nil {
				control.archive.Listeners.ErrorListener(archiveErr)
			}
		} else if poller.CorrelationId == correlationId {
			if code != codecs.ControlResponseCode.OK {
				archiveErr := NewArchiveError(correlationId, int(control.Results.RelevantId), "unexpected response code: "+string(control.Results.Code))
				return aeron.NullValue, archiveErr
			}
			return poller.RelevantId, nil
		}
	}
}

// Java - https://github.com/real-logic/aeron/blob/1.46.2/aeron-archive/src/main/java/io/aeron/archive/client/AeronArchive.java#L2309
func (control *Control) pollNextResponse(correlationId int64, deadline time.Time, poller *ControlResponsePoller, controlSessionId int64) error {
	control.archive.Options.IdleStrategy.Idle(1)

	for {
		fragments := poller.Poll()

		// NOTE: Go Specific
		if poller.ArchiveError != nil {
			return poller.ArchiveError
		}

		if poller.IsPollComplete {
			if poller.TemplateId == int16(codecIds.recordingSignalEvent) &&
				poller.ControlSessionId == controlSessionId {
				control.dispatchRecordingSignal()
				continue
			}

			break
		}

		if fragments > 0 {
			continue
		}

		if !poller.Subscription.IsConnected() {
			return fmt.Errorf("response channel from archive is not connected")
		}

		if err := control.checkDeadline(deadline, "awaiting subscription descriptors", correlationId); err != nil {
			return err
		}

		control.archive.Options.IdleStrategy.Idle(0)
	}
	return nil
}

// PollForSubscriptionDescriptors ...
// Java - https://github.com/real-logic/aeron/blob/1.46.2/aeron-archive/src/main/java/io/aeron/archive/client/AeronArchive.java#L2482
func (control *Control) PollForSubscriptionDescriptors(correlationId int64, count int32, consumer func(*codecs.RecordingSubscriptionDescriptor)) (int32, error) {
	existingRemainCount := count
	deadline := time.Now().Add(control.archive.Options.Timeout)
	poller := control.recordingSubscriptionDescriptorPoller
	poller.Reset(correlationId, count, consumer)
	control.archive.Options.IdleStrategy.Idle(1)

	for {
		fragment := poller.Poll()
		remainingSubscriptionCount := poller.RemainingSubscriptionCount

		// NOTE: Go Specific
		if poller.ArchiveError != nil {
			return aeron.NullValue, poller.ArchiveError
		}

		if poller.IsDispatchComplete {
			return count - remainingSubscriptionCount, nil
		}

		if remainingSubscriptionCount != existingRemainCount {
			existingRemainCount = remainingSubscriptionCount
			deadline = time.Now().Add(control.archive.Options.Timeout)
		}

		if fragment > 0 {
			continue
		}

		if !poller.Subscription.IsConnected() {
			return aeron.NullValue, fmt.Errorf("response channel from archive is not connected")
		}

		if err := control.checkDeadline(deadline, "awaiting subscription descriptors", correlationId); err != nil {
			return aeron.NullValue, err
		}

		control.archive.Options.IdleStrategy.Idle(0)
	}
}

// PollForDescriptors to poll for recording descriptors and act on it with consumer
// Java - https://github.com/real-logic/aeron/blob/1.46.2/aeron-archive/src/main/java/io/aeron/archive/client/AeronArchive.java#L2440
func (control *Control) PollForDescriptors(correlationId int64, count int32, consumer func(*codecs.RecordingDescriptor)) (int32, error) {
	existingRemainCount := count
	deadline := time.Now().Add(control.archive.Options.Timeout)
	poller := control.recordingDescriptorPoller
	poller.Reset(correlationId, count, consumer)
	control.archive.Options.IdleStrategy.Idle(1)

	for {
		fragment := poller.Poll()
		remainingRecordCount := poller.RemainingRecordCount

		// NOTE: Go Specific
		if poller.ArchiveError != nil {
			return aeron.NullValue, poller.ArchiveError
		}

		if poller.IsDispatchComplete {
			return count - remainingRecordCount, nil
		}

		if remainingRecordCount != existingRemainCount {
			existingRemainCount = remainingRecordCount
			deadline = time.Now().Add(control.archive.Options.Timeout)
		}

		if fragment > 0 {
			continue
		}

		if !poller.Subscription.IsConnected() {
			return aeron.NullValue, fmt.Errorf("response channel from archive is not connected")
		}

		if err := control.checkDeadline(deadline, "awaiting subscription descriptors", correlationId); err != nil {
			return aeron.NullValue, err
		}

		control.archive.Options.IdleStrategy.Idle(0)
	}
}

// Helper to consume descriptors by append it to a slice
func (control *Control) AppendingRecordingDescriptorConsumer(descriptors *[]*codecs.RecordingDescriptor) func(*codecs.RecordingDescriptor) {
	return func(descriptor *codecs.RecordingDescriptor) {
		*descriptors = append(*descriptors, descriptor)
	}
}

// Helper to consume descriptors by append it to a slice
func (control *Control) AppendRecordingSubscriptionDescriptorConsumer(descriptors *[]*codecs.RecordingSubscriptionDescriptor) func(*codecs.RecordingSubscriptionDescriptor) {
	return func(descriptor *codecs.RecordingSubscriptionDescriptor) {
		*descriptors = append(*descriptors, descriptor)
	}
}

// dispatchRecordingSignal ...
// Java - https://github.com/real-logic/aeron/blob/1.46.2/aeron-archive/src/main/java/io/aeron/archive/client/AeronArchive.java#L2524
func (control *Control) dispatchRecordingSignal() {
	// NOTE: not 100% sure on this cos in Java version this points to
	// aeron context recording signal consumer
	if control.archive.Listeners.RecordingSignalListener != nil {
		control.archive.Listeners.RecordingSignalListener(&codecs.RecordingSignalEvent{
			ControlSessionId: control.Results.ControlSessionId,
			CorrelationId:    control.Results.CorrelationId,
			RecordingId:      control.Results.RecordingId,
			SubscriptionId:   control.Results.SubscriptionId,
			Position:         control.Results.Position,
			Signal:           control.Results.RecordingSignal,
		})
	}
}

// Java - https://github.com/real-logic/aeron/blob/1.46.2/aeron-archive/src/main/java/io/aeron/archive/client/AeronArchive.java#L2295
func (control *Control) checkDeadline(deadline time.Time, errorMessage string, correlationId int64) error {
	if time.Now().After(deadline) {
		return fmt.Errorf("%s - correlationId=%d messageTimeout=%s", errorMessage, correlationId, control.archive.Options.Timeout)
	}
	return nil
}
