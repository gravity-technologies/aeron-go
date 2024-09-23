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
	"fmt"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/archive/codecs"
)

// Control contains everything required for the archive subscription/response side
type Control struct {
	Subscription *aeron.Subscription

	archive *Archive // link to parent

	controlResponsePoller                 *ControlResponsePoller
	recordingDescriptorPoller             *RecordingDescriptorPoller
	recordingSubscriptionDescriptorPoller *RecordingSubscriptionDescriptorPoller
}

var ErrNotConnected = fmt.Errorf("not connected")

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
				archiveErr := NewArchiveError(aeron.NullValue, aeron.NullValue, fmt.Sprintf("unexpected response code: %d", poller.Code))
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
	poller := control.RecordingSubscriptionDescriptorPoller()
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
	poller := control.RecordingDescriptorPoller()
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

func (control *Control) RecordingDescriptorPoller() *RecordingDescriptorPoller {
	if control.recordingDescriptorPoller == nil {
		control.recordingDescriptorPoller = NewRecordingDescriptorPoller(
			control.Subscription,
			control.archive.Listeners.ErrorListener,
			control.archive.Listeners.RecordingSignalListener,
			control.archive.SessionID,
			ControlFragmentLimit)
	}
	return control.recordingDescriptorPoller
}

func (control *Control) RecordingSubscriptionDescriptorPoller() *RecordingSubscriptionDescriptorPoller {
	if control.recordingSubscriptionDescriptorPoller == nil {
		control.recordingSubscriptionDescriptorPoller = NewRecordingSubscriptionDescriptorPoller(
			control.Subscription,
			control.archive.Listeners.ErrorListener,
			control.archive.Listeners.RecordingSignalListener,
			control.archive.SessionID,
			ControlFragmentLimit)
	}
	return control.recordingSubscriptionDescriptorPoller
}

func (control *Control) ControlResponsePoller() *ControlResponsePoller {
	return control.controlResponsePoller
}

// dispatchRecordingSignal ...
// Java - https://github.com/real-logic/aeron/blob/1.46.2/aeron-archive/src/main/java/io/aeron/archive/client/AeronArchive.java#L2524
func (control *Control) dispatchRecordingSignal() {
	// NOTE: not 100% sure on this cos in Java version this points to
	// aeron context recording signal consumer
	if control.archive.Listeners.RecordingSignalListener != nil {
		control.archive.Listeners.RecordingSignalListener(&codecs.RecordingSignalEvent{
			ControlSessionId: control.controlResponsePoller.ControlSessionId,
			CorrelationId:    control.controlResponsePoller.CorrelationId,
			RecordingId:      control.controlResponsePoller.RecordingId,
			SubscriptionId:   control.controlResponsePoller.SubscriptionId,
			Position:         control.controlResponsePoller.Position,
			Signal:           control.controlResponsePoller.RecordingSignal,
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
