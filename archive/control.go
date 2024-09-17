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

	archive           *Archive // link to parent
	fragmentAssembler *aeron.ControlledFragmentAssembler

	errorFragmentHandler term.ControlledFragmentHandler
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

// errorResponseFragmentHandler is used to check for errors and async events on an idle control
// session. Essentially:
//
//	ignore messages not on our session ID
//	process recordingSignalEvents
//	Log a warning if we have interrupted a synchronous event
func (control *Control) errorResponseFragmentHandler(buffer *atomic.Buffer, offset int32, length int32, header *logbuffer.Header) (action term.ControlledPollAction) {
	action = term.ControlledPollActionContinue

	pollContext := PollContext{control, 0}

	if control.Results.ErrorResponse != nil {
		return term.ControlledPollActionAbort
	}

	logger.Debugf("errorResponseFragmentHandler: offset:%d length: %d", offset, length)

	var hdr codecs.SbeGoMessageHeader

	buf := new(bytes.Buffer)
	buffer.WriteBytes(buf, offset, length)

	marshaller := codecs.NewSbeGoMarshaller()
	if err := hdr.Decode(marshaller, buf); err != nil {
		// Not much to be done here as we can't correlate
		err2 := fmt.Errorf("ConnectionControlFragmentHandler() failed to decode control message header: %w", err)
		// Call the global error handler, ugly, but it's all we've got
		if pollContext.control.archive.Listeners.ErrorListener != nil {
			pollContext.control.archive.Listeners.ErrorListener(err2)
		}
	}

	switch hdr.TemplateId {
	case codecIds.controlResponse:
		var controlResponse = new(codecs.ControlResponse)
		pollContext.control.Results.ControlResponse = controlResponse
		logger.Debugf("controlFragmentHandler/controlResponse: Received controlResponse")
		if err := controlResponse.Decode(marshaller, buf, hdr.Version, hdr.BlockLength, rangeChecking); err != nil {
			// Not much to be done here as we can't see what's gone wrong
			err2 := fmt.Errorf("errorResponseFragmentHandler failed to decode control response:%w", err)
			// Call the global error handler, ugly, but it's all we've got
			if pollContext.control.archive.Listeners.ErrorListener != nil {
				pollContext.control.archive.Listeners.ErrorListener(err2)
			}
			return
		}

		// If this was for us then check for errors
		if controlResponse.ControlSessionId == pollContext.control.archive.SessionID {
			if controlResponse.Code == codecs.ControlResponseCode.ERROR {
				archiveErr := NewArchiveError(controlResponse.CorrelationId, int(controlResponse.RelevantId), fmt.Sprintf("PollForErrorResponse received a ControlResponse (correlationId:%d Code:ERROR error=\"%s\"", controlResponse.CorrelationId, controlResponse.ErrorMessage))
				pollContext.control.Results.ErrorResponse = archiveErr
				pollContext.control.Results.IsPollComplete = true
				return term.ControlledPollActionBreak
			}
		}
		return

	case codecIds.challenge:
		var challenge = new(codecs.Challenge)

		if err := challenge.Decode(marshaller, buf, hdr.Version, hdr.BlockLength, rangeChecking); err != nil {
			// Not much to be done here as we can't correlate
			err2 := fmt.Errorf("errorResponseFragmentHandler failed to decode challenge: %w", err)
			if pollContext.control.archive.Listeners.ErrorListener != nil {
				pollContext.control.archive.Listeners.ErrorListener(err2)
			}
		}

		// If this was for us then that's bad
		if challenge.ControlSessionId == pollContext.control.archive.SessionID {
			pollContext.control.Results.ErrorResponse = fmt.Errorf("Received and ignoring challenge  (correlationID:%d). EerrorResponse should not be called on in parallel with sync operations", challenge.CorrelationId)
			logger.Warning(pollContext.control.Results.ErrorResponse)

			pollContext.control.Results.IsPollComplete = true
			return term.ControlledPollActionBreak
			// return
		}

	case codecIds.recordingDescriptor:
		var rd = new(codecs.RecordingDescriptor)

		if err := rd.Decode(marshaller, buf, hdr.Version, hdr.BlockLength, rangeChecking); err != nil {
			// Not much to be done here as we can't correlate
			err2 := fmt.Errorf("errorResponseFragmentHandler failed to decode recordingSubscription: %w", err)
			if pollContext.control.archive.Listeners.ErrorListener != nil {
				pollContext.control.archive.Listeners.ErrorListener(err2)
			}
		}

		// If this was for us then that's bad
		if rd.ControlSessionId == pollContext.control.archive.SessionID {
			pollContext.control.Results.ErrorResponse = fmt.Errorf("Received and ignoring recordingDescriptor (correlationID:%d). ErrorResponse should not be called on in parallel with sync operations", rd.CorrelationId)
			logger.Warning(pollContext.control.Results.ErrorResponse)

			pollContext.control.Results.IsPollComplete = true
			return term.ControlledPollActionBreak
			// return
		}

	case codecIds.recordingSubscriptionDescriptor:
		var rsd = new(codecs.RecordingSubscriptionDescriptor)

		if err := rsd.Decode(marshaller, buf, hdr.Version, hdr.BlockLength, rangeChecking); err != nil {
			// Not much to be done here as we can't correlate
			err2 := fmt.Errorf("errorResponseFragmentHandler failed to decode recordingSubscription: %w", err)
			if pollContext.control.archive.Listeners.ErrorListener != nil {
				pollContext.control.archive.Listeners.ErrorListener(err2)
			}
		}

		// If this was for us then that's bad
		if rsd.ControlSessionId == pollContext.control.archive.SessionID {
			pollContext.control.Results.ErrorResponse = fmt.Errorf("Received and ignoring recordingsubscriptionDescriptor (correlationID:%d). ErrorResponse should not be called on in parallel with sync operations", rsd.CorrelationId)
			logger.Warning(pollContext.control.Results.ErrorResponse)

			pollContext.control.Results.IsPollComplete = true
			return term.ControlledPollActionBreak
		}

	case codecIds.recordingSignalEvent:
		var rse = new(codecs.RecordingSignalEvent)

		if err := rse.Decode(marshaller, buf, hdr.Version, hdr.BlockLength, rangeChecking); err != nil {
			// Not much to be done here as we can't really tell what went wrong
			err2 := fmt.Errorf("errorResponseFragmentHandler failed to decode recording signal: %w", err)
			if pollContext.control.archive.Listeners.ErrorListener != nil {
				pollContext.control.archive.Listeners.ErrorListener(err2)
			}
		}

		if rse.ControlSessionId == pollContext.control.archive.SessionID {
			// We can call the async callback if it exists
			if pollContext.control.archive.Listeners.RecordingSignalListener != nil {
				pollContext.control.archive.Listeners.RecordingSignalListener(rse)
			}

			pollContext.control.Results.IsPollComplete = true
			return term.ControlledPollActionBreak
		}

	default:
		fmt.Printf("errorResponseFragmentHandler: Insert decoder for type: %d", hdr.TemplateId)
	}

	return
}

// poll provides the control response poller using local state to pass
// back data from the underlying subscription
func (control *Control) poll(handler term.ControlledFragmentHandler, fragmentLimit int) int {

	// Update our globals in case they've changed so we use the current state in our callback
	rangeChecking = control.archive.Options.RangeChecking

	control.Results.ControlResponse = nil  // Clear old results
	control.Results.IsPollComplete = false // Clear completion flag

	return control.Subscription.ControlledPoll(handler, fragmentLimit)
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
func (control *Control) PollForErrorResponse() (int, error) {
	control.Results.ErrorResponse = nil
	control.Results.ErrorMessage = []uint8{}

	if !control.Subscription.IsConnected() {
		return aeron.NullValue, ErrNotConnected
	}

	if control.Poll() != 0 && control.Results.IsPollComplete {
		if control.Results.ControlSessionId == control.archive.SessionID {
			if control.Results.Code == codecs.ControlResponseCode.ERROR {
				archiveErr := NewArchiveError(control.Results.CorrelationId, int(control.Results.RelevantId), fmt.Sprintf("PollForErrorResponse received a ControlResponse (correlationId:%d Code:ERROR error=\"%s\")", control.Results.CorrelationId, control.Results.ErrorMessage))
				control.Results.ErrorResponse = archiveErr
				return aeron.NullValue, archiveErr
			}

			if control.Results.TemplateId == int16(codecIds.recordingSignalEvent) {
				control.dispatchRecordingSignal()
			}
		}
	}

	// If we polled and did nothing then return
	return aeron.NullValue, nil
}

// Poll for control response events. Returns number of fragments read during the operation.
// Zero if no events are available.
func (control *Control) Poll() (workCount int) {
	if control.Results.IsPollComplete {
		// Update our globals in case they've changed so we use the current state in our callback
		rangeChecking = control.archive.Options.RangeChecking
		control.Results = ControlResults{
			ControlSessionId: aeron.NullValue,
			CorrelationId:    aeron.NullValue,
			RelevantId:       aeron.NullValue,
			Version:          0,
			Code:             codecs.ControlResponseCode.NullValue,
			ErrorMessage:     []uint8{},

			RecordingSignal: codecs.RecordingSignal.NullValue,
			RecordingId:     aeron.NullValue,
			SubscriptionId:  aeron.NullValue,
			Position:        aeron.NullValue,

			EncodedChallenge: []uint8{},

			TemplateId:     aeron.NullValue,
			IsPollComplete: false,

			ControlResponse: nil,
		}
	}

	return control.Subscription.ControlledPoll(control.fragmentAssembler.OnFragment, controlFragmentLimit)
}

// PollForResponse polls for a specific correlationID
// Returns (relevantId, nil) on success, (-1 or relevantId, error) on failure
// More complex responses are contained in Control.ControlResponse after the call
func (control *Control) PollForResponse(correlationID int64, sessionID int64) (int64, error) {
	logger.Debugf("PollForResponse(%d:%d)", correlationID, sessionID)

	// Poll for events.
	start := time.Now()

	for {
		err := control.pollNextResponse(start, correlationID, sessionID)
		if err != nil {
			logger.Debug(err) // log it in debug mode as an aid to diagnosis
			return aeron.NullValue, err
		}

		if control.Results.ControlSessionId != control.archive.SessionID {
			continue
		}

		// Check result
		if control.Results.Code == codecs.ControlResponseCode.ERROR {
			archiveErr := NewArchiveError(correlationID, int(control.Results.RelevantId), fmt.Sprintf("response for correlationId=%d, error: %s", correlationID, control.Results.ErrorMessage))
			logger.Debug(archiveErr) // log it in debug mode as an aid to diagnosis

			if control.Results.CorrelationId == correlationID {
				return aeron.NullValue, archiveErr
			}
		}

		if control.Results.CorrelationId == correlationID {
			if control.Results.Code != codecs.ControlResponseCode.OK {
				archiveErr := NewArchiveError(correlationID, int(control.Results.RelevantId), "unexpected response code: "+string(control.Results.Code))
				logger.Debug(archiveErr) // log it in debug mode as an aid to diagnosis
				return aeron.NullValue, archiveErr
			}
			return control.Results.RelevantId, nil
		}
	}
}

func (control *Control) pollNextResponse(startTime time.Time, correlationID, sessionID int64) error {
	for {
		fragments := control.Poll()

		if control.Results.IsPollComplete {
			logger.Debugf("PollForResponse(%d:%d) complete, result is %d", correlationID, sessionID, control.Results.Code)

			if (control.Results.TemplateId == int16(codecIds.recordingSignalEvent)) &&
				(control.Results.ControlSessionId == sessionID) {
				control.dispatchRecordingSignal()
				continue
			}

			break
		}

		if fragments > 0 {
			continue
		}

		if control.Subscription.IsClosed() {
			return fmt.Errorf("response channel from archive is not connected")
		}

		// Check deadline
		if time.Since(startTime) > control.archive.Options.Timeout {
			return fmt.Errorf("timeout waiting for correlationID %d", correlationID)
		}

		control.archive.Options.IdleStrategy.Idle(0)
	}

	return nil
}

func (control *Control) onFragment(
	buffer *atomic.Buffer,
	offset int32,
	length int32,
	header *logbuffer.Header,
) term.ControlledPollAction {

	if control.Results.IsPollComplete {
		return term.ControlledPollActionAbort
	}

	var hdr codecs.SbeGoMessageHeader

	buf := new(bytes.Buffer)
	buffer.WriteBytes(buf, offset, length)

	marshaller := codecs.NewSbeGoMarshaller()
	if err := hdr.Decode(marshaller, buf); err != nil {
		// Not much to be done here as we can't correlate
		err = fmt.Errorf("DescriptorFragmentHandler() failed to decode control message header: %w", err)
		if control.archive.Listeners.ErrorListener != nil {
			control.archive.Listeners.ErrorListener(err)
		}
		return term.ControlledPollActionContinue
	}

	control.Results.TemplateId = int16(hdr.TemplateId)

	switch hdr.TemplateId {
	case codecIds.controlResponse:
		var controlResponse = new(codecs.ControlResponse)
		logger.Debugf("Received controlResponse: length %d", buf.Len())
		if err := controlResponse.Decode(marshaller, buf, hdr.Version, hdr.BlockLength, rangeChecking); err != nil {
			// Not much to be done here as we can't correlate
			err = fmt.Errorf("failed to decode control response: %w", err)
			if control.archive.Listeners.ErrorListener != nil {
				control.archive.Listeners.ErrorListener(err)
			}
			return term.ControlledPollActionContinue
		}
		control.Results.ControlResponse = controlResponse
		control.Results.CorrelationId = controlResponse.CorrelationId
		control.Results.ControlSessionId = controlResponse.ControlSessionId
		control.Results.RelevantId = controlResponse.RelevantId
		control.Results.Code = controlResponse.Code
		control.Results.Version = controlResponse.Version
		control.Results.ErrorMessage = controlResponse.ErrorMessage
		control.Results.IsPollComplete = true
		return term.ControlledPollActionBreak

	case codecIds.challenge:
		var challenge = new(codecs.Challenge)
		if err := challenge.Decode(marshaller, buf, hdr.Version, hdr.BlockLength, rangeChecking); err != nil {
			// Not much to be done here as we can't correlate
			err = fmt.Errorf("failed to decode challenge: %w", err)
			if control.archive.Listeners.ErrorListener != nil {
				control.archive.Listeners.ErrorListener(err)
			}
			return term.ControlledPollActionContinue
		}
		control.Results.ControlSessionId = challenge.ControlSessionId
		control.Results.CorrelationId = challenge.CorrelationId
		control.Results.RelevantId = aeron.NullValue
		control.Results.Code = codecs.ControlResponseCode.NullValue
		control.Results.Version = challenge.Version
		control.Results.ErrorMessage = []uint8{}
		control.Results.EncodedChallenge = challenge.EncodedChallenge
		control.Results.IsPollComplete = true
		return term.ControlledPollActionBreak

	case codecIds.recordingSignalEvent:
		var recordingSignalEvent = new(codecs.RecordingSignalEvent)
		if err := recordingSignalEvent.Decode(marshaller, buf, hdr.Version, hdr.BlockLength, rangeChecking); err != nil {
			// Not much to be done here as we can't correlate
			err = fmt.Errorf("failed to decode recording signal: %w", err)
			if control.archive.Listeners.ErrorListener != nil {
				control.archive.Listeners.ErrorListener(err)
			}
			return term.ControlledPollActionContinue
		}
		control.Results.ControlSessionId = recordingSignalEvent.ControlSessionId
		control.Results.CorrelationId = recordingSignalEvent.CorrelationId
		control.Results.RecordingId = recordingSignalEvent.RecordingId
		control.Results.SubscriptionId = recordingSignalEvent.SubscriptionId
		control.Results.Position = recordingSignalEvent.Position
		control.Results.RecordingSignal = recordingSignalEvent.Signal
		control.Results.IsPollComplete = true
		return term.ControlledPollActionBreak

	default:
		logger.Debug("descriptorFragmentHandler: Insert decoder for type: %d", hdr.TemplateId)
	}
	return term.ControlledPollActionContinue
}

func (control *Control) dispatchRecordingSignal() {
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

// ----- Rewrite from Java [End] ---- //
