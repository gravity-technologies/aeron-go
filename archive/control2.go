package archive

import (
	"bytes"
	"fmt"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/archive/codecs"
)

// Used internally to handle connection state
type controlState struct {
	state int
	err   error
}

type ControlResults struct {
	CorrelationId                    int64
	ControlResponse                  *codecs.ControlResponse
	RecordingDescriptors             []*codecs.RecordingDescriptor
	RecordingSubscriptionDescriptors []*codecs.RecordingSubscriptionDescriptor
	IsPollComplete                   bool
	FragmentsReceived                int
	ErrorResponse                    error // Used by PollForErrorResponse
}

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
			control.archive.Proxy.ChallengeResponse(challenge.CorrelationId, control.archive.Options.AuthResponse, control.archive.SessionID)
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

func (control *Control) poll(handler term.ControlledFragmentHandler, fragmentLimit int) int {

	// Update our globals in case they've changed so we use the current state in our callback
	rangeChecking = control.archive.Options.RangeChecking

	control.Results.ControlResponse = nil  // Clear old results
	control.Results.IsPollComplete = false // Clear completion flag

	return control.Subscription.ControlledPoll(handler, fragmentLimit)
}
