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
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/idlestrategy"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/archive/codecs"
)

// Proxy class for encapsulating encoding and sending of control protocol messages to an archive
type Proxy struct {
	Publication       *aeron.Publication
	marshaller        *codecs.SbeGoMarshaller // currently shared as we're not reentrant (but could be here)
	timeout           time.Duration
	retryIdleStrategy idlestrategy.Idler
	rangeChecking     bool
}

func NewProxy(publication *aeron.Publication, retryIdleStrategy idlestrategy.Idler, timeout time.Duration, rangeChecking bool) *Proxy {
	return &Proxy{
		Publication:       publication,
		marshaller:        codecs.NewSbeGoMarshaller(),
		timeout:           timeout,
		retryIdleStrategy: retryIdleStrategy,
		rangeChecking:     rangeChecking,
	}
}

// Offer to our request publication with a retry to allow time for the image establishment, some back pressure etc
func (proxy *Proxy) Offer(buffer *atomic.Buffer, offset int32, length int32, reservedValueSupplier term.ReservedValueSupplier) int64 {
	start := time.Now()
	var ret int64
	for time.Since(start) < proxy.timeout {
		ret = proxy.Publication.Offer(buffer, offset, length, reservedValueSupplier)
		switch ret {
		// Retry on these
		case aeron.NotConnected, aeron.BackPressured, aeron.AdminAction:
			proxy.retryIdleStrategy.Idle(0)

		// Fail or succeed on other values
		default:
			return ret
		}
	}

	// Give up, returning the last failure
	logger.Debugf("Proxy.Offer timing out [%d]", ret)
	return ret

}

// From here we have all the functions that create a data packet and send it on the
// publication. Responses will be processed on the control

// ConnectRequest packet and offer
func (proxy *Proxy) ConnectRequest(correlationID int64, responseStream int32, responseChannel string) (bool, error) {

	// Create a packet and send it
	bytes, err := codecs.ConnectRequestPacket(proxy.marshaller, proxy.rangeChecking, correlationID, responseStream, responseChannel)
	if err != nil {
		return false, err
	}

	return proxy.OfferWithTimeout(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
}

// CloseSessionRequest packet and offer
func (proxy *Proxy) CloseSessionRequest(controlSessionId int64) error {
	// Create a packet and send it
	bytes, err := codecs.CloseSessionRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId)
	if err != nil {
		return err
	}

	if ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Offer failed: %d", ret)
	}

	return nil
}

func (proxy *Proxy) CloseSession(controlSessionId int64) error {
	// Create a packet and send it
	bytes, err := codecs.CloseSessionRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId)
	if err != nil {
		return err
	}

	if ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Offer failed: %d", ret)
	}

	return nil
}

// StartRecordingRequest packet and offer
// Uses the more recent protocol addition StartdRecordingRequest2 which added autoStop
func (proxy *Proxy) StartRecordingRequest(correlationID int64, stream int32, isLocal bool, autoStop bool, channel string, controlSessionId int64) error {

	bytes, err := codecs.StartRecordingRequest2Packet(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, stream, isLocal, autoStop, channel)
	if err != nil {
		return err
	}

	if ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Offer failed: %d", ret)
	}

	return nil
}

// StopRecordingRequest packet and offer
func (proxy *Proxy) StopRecordingRequest(correlationID int64, stream int32, channel string, controlSessionId int64) error {
	// Create a packet and send it
	bytes, err := codecs.StopRecordingRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, stream, channel)
	if err != nil {
		return err
	}

	if ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Offer failed: %d", ret)
	}

	return nil
}

// ReplayRequest packet and offer
func (proxy *Proxy) ReplayRequest(correlationID int64, recordingID int64, position int64, length int64, replayChannel string, replayStream int32, controlSessionId int64) error {

	// Create a packet and send it
	bytes, err := codecs.ReplayRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, recordingID, position, length, replayStream, replayChannel)
	if err != nil {
		return err
	}

	if ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Offer failed: %d", ret)
	}

	return nil
}

// StopReplayRequest packet and offer
func (proxy *Proxy) StopReplayRequest(correlationID int64, replaySessionID int64, controlSessionId int64) error {
	// Create a packet and send it
	bytes, err := codecs.StopReplayRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, replaySessionID)
	if err != nil {
		return err
	}

	if ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Offer failed: %d", ret)
	}

	return nil
}

// ListRecordingsRequest packet and offer
// Lists up to recordCount recordings starting at fromRecordingID
func (proxy *Proxy) ListRecordingsRequest(correlationID int64, fromRecordingID int64, recordCount int32, controlSessionId int64) error {
	// Create a packet and send it
	bytes, err := codecs.ListRecordingsRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, fromRecordingID, recordCount)
	if err != nil {
		return err
	}

	if ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Offer failed: %d", ret)
	}

	return nil
}

// ListRecordingsForUriRequest packet and offer
// Lists up to recordCount recordings that match the channel and stream
func (proxy *Proxy) ListRecordingsForUriRequest(correlationID int64, fromRecordingID int64, recordCount int32, stream int32, channel string, controlSessionId int64) error {

	bytes, err := codecs.ListRecordingsForUriRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, fromRecordingID, recordCount, stream, channel)

	if err != nil {
		return err
	}

	if ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Offer failed: %d", ret)
	}

	return nil
}

// ListRecordingRequest packet and offer
// Retrieves a recording descriptor for a specific recordingID
func (proxy *Proxy) ListRecordingRequest(correlationID int64, fromRecordingID int64, controlSessionId int64) error {
	// Create a packet and send it
	bytes, err := codecs.ListRecordingRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, fromRecordingID)
	if err != nil {
		return err
	}

	if ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Offer failed: %d", ret)
	}

	return nil
}

// ExtendRecordingRequest packet and offer
// Uses the more recent protocol addition ExtendRecordingRequest2 which added autoStop
func (proxy *Proxy) ExtendRecordingRequest(correlationID int64, recordingID int64, stream int32, sourceLocation codecs.SourceLocationEnum, autoStop bool, channel string, controlSessionId int64) error {
	// Create a packet and send it
	bytes, err := codecs.ExtendRecordingRequest2Packet(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, recordingID, stream, sourceLocation, autoStop, channel)
	if err != nil {
		return err
	}

	if ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Offer failed: %d", ret)
	}

	return nil
}

// RecordingPositionRequest packet and offer
func (proxy *Proxy) RecordingPositionRequest(correlationID int64, recordingID int64, controlSessionId int64) error {
	// Create a packet and send it
	bytes, err := codecs.RecordingPositionRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, recordingID)
	if err != nil {
		return err
	}

	if ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Offer failed: %d", ret)
	}

	return nil
}

// TruncateRecordingRequest packet and offer
func (proxy *Proxy) TruncateRecordingRequest(correlationID int64, recordingID int64, position int64, controlSessionId int64) error {
	// Create a packet and send it
	bytes, err := codecs.TruncateRecordingRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, recordingID, position)
	if err != nil {
		return err
	}

	if ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Offer failed: %d", ret)
	}

	return nil
}

// StopRecordingSubscriptionRequest packet and offer
func (proxy *Proxy) StopRecordingSubscriptionRequest(correlationID int64, subscriptionID int64, controlSessionId int64) error {
	// Create a packet and send it
	bytes, err := codecs.StopRecordingSubscriptionPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, subscriptionID)
	if err != nil {
		return err
	}

	if ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Offer failed: %d", ret)
	}

	return nil
}

// StopRecordingByIdentityRequest packet and offer
func (proxy *Proxy) StopRecordingByIdentityRequest(correlationID int64, recordingID int64, controlSessionId int64) error {
	// Create a packet and send it
	bytes, err := codecs.StopRecordingByIdentityPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, recordingID)
	if err != nil {
		return err
	}

	if ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Offer failed: %d", ret)
	}

	return nil
}

// StopPositionRequest packet and offer
func (proxy *Proxy) StopPositionRequest(correlationID int64, recordingID int64, controlSessionId int64) error {
	// Create a packet and send it
	bytes, err := codecs.StopPositionPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, recordingID)
	if err != nil {
		return err
	}

	if ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Offer failed: %d", ret)
	}

	return nil
}

// FindLastMatchingRecordingRequest packet and offer
func (proxy *Proxy) FindLastMatchingRecordingRequest(correlationID int64, minRecordingID int64, sessionID int32, stream int32, channel string, controlSessionId int64) error {

	// Create a packet and send it
	bytes, err := codecs.FindLastMatchingRecordingPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, minRecordingID, sessionID, stream, channel)
	if err != nil {
		return err
	}

	if ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Offer failed: %d", ret)
	}

	return nil
}

// ListRecordingSubscriptionsRequest packet and offer
func (proxy *Proxy) ListRecordingSubscriptionsRequest(correlationID int64, pseudoIndex int32, subscriptionCount int32, applyStreamID bool, stream int32, channel string, controlSessionId int64) error {

	// Create a packet and send it
	bytes, err := codecs.ListRecordingSubscriptionsPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, pseudoIndex, subscriptionCount, applyStreamID, stream, channel)
	if err != nil {
		return err
	}

	if ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Offer failed: %d", ret)
	}

	return nil
}

// BoundedReplayRequest packet and offer
func (proxy *Proxy) BoundedReplayRequest(correlationID int64, recordingID int64, position int64, length int64, limitCounterID int32, replayStream int32, replayChannel string, controlSessionId int64) error {

	// Create a packet and send it
	bytes, err := codecs.BoundedReplayPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, recordingID, position, length, limitCounterID, replayStream, replayChannel)
	if err != nil {
		return err
	}

	if ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Offer failed: %d", ret)
	}

	return nil
}

// StopAllReplaysRequest packet and offer
func (proxy *Proxy) StopAllReplaysRequest(correlationID int64, recordingID int64, controlSessionId int64) error {

	// Create a packet and send it
	bytes, err := codecs.StopAllReplaysPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, recordingID)
	if err != nil {
		return err
	}

	if ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Offer failed: %d", ret)
	}

	return nil
}

// CatalogHeaderRequest packet and offer
func (proxy *Proxy) CatalogHeaderRequest(version int32, length int32, nextRecordingID int64, alignment int32) error {

	// Create a packet and send it
	bytes, err := codecs.CatalogHeaderPacket(proxy.marshaller, proxy.rangeChecking, version, length, nextRecordingID, alignment)
	if err != nil {
		return err
	}

	if ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Offer failed: %d", ret)
	}

	return nil
}

// ReplicateRequest packet and offer
func (proxy *Proxy) ReplicateRequest(correlationID int64, srcRecordingID int64, dstRecordingID int64, srcControlStreamID int32, srcControlChannel string, liveDestination string, controlSessionId int64) error {
	// Create a packet and send it
	bytes, err := codecs.ReplicateRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, srcRecordingID, dstRecordingID, srcControlStreamID, srcControlChannel, liveDestination)
	if err != nil {
		return err
	}

	if ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Offer failed: %d", ret)
	}

	return nil
}

// ReplicateRequest2 packet and offer
func (proxy *Proxy) ReplicateRequest2(correlationID int64, srcRecordingID int64, dstRecordingID int64, stopPosition int64, channelTagID int64, srcControlStreamID int32, srcControlChannel string, liveDestination string, replicationChannel string, controlSessionId int64) error {
	// Create a packet and send it
	bytes, err := codecs.ReplicateRequest2Packet(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, srcRecordingID, dstRecordingID, stopPosition, channelTagID, srcControlStreamID, srcControlChannel, liveDestination, replicationChannel)
	if err != nil {
		return err
	}

	if ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Offer failed: %d", ret)
	}

	return nil
}

// StopReplicationRequest packet and offer
func (proxy *Proxy) StopReplicationRequest(correlationID int64, replicationID int64, controlSessionId int64) error {
	// Create a packet and send it
	bytes, err := codecs.StopReplicationRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, replicationID)
	if err != nil {
		return err
	}

	if ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Offer failed: %d", ret)
	}

	return nil
}

// StartPositionRequest packet and offer
func (proxy *Proxy) StartPositionRequest(correlationID int64, recordingID int64, controlSessionId int64) error {
	// Create a packet and send it
	bytes, err := codecs.StartPositionRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, recordingID)
	if err != nil {
		return err
	}

	if ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Offer failed: %d", ret)
	}

	return nil
}

// DetachSegmentsRequest packet and offer
func (proxy *Proxy) DetachSegmentsRequest(correlationID int64, recordingID int64, newStartPosition int64, controlSessionId int64) error {
	// Create a packet and send it
	bytes, err := codecs.DetachSegmentsRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, recordingID, newStartPosition)
	if err != nil {
		return err
	}

	if ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Offer failed: %d", ret)
	}

	return nil
}

// DeleteDetachedSegmentsRequest packet and offer
func (proxy *Proxy) DeleteDetachedSegmentsRequest(correlationID int64, recordingID int64, controlSessionId int64) error {
	// Create a packet and send it
	bytes, err := codecs.DeleteDetachedSegmentsRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, recordingID)
	if err != nil {
		return err
	}

	if ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Offer failed: %d", ret)
	}

	return nil
}

// PurgeSegmentsRequest packet and offer
func (proxy *Proxy) PurgeSegmentsRequest(correlationID int64, recordingID int64, newStartPosition int64, controlSessionId int64) error {
	// Create a packet and send it
	bytes, err := codecs.PurgeSegmentsRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, recordingID, newStartPosition)
	if err != nil {
		return err
	}

	if ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Offer failed: %d", ret)
	}

	return nil
}

// AttachSegmentsRequest packet and offer
func (proxy *Proxy) AttachSegmentsRequest(correlationID int64, recordingID int64, controlSessionId int64) error {
	// Create a packet and send it
	bytes, err := codecs.AttachSegmentsRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, recordingID)
	if err != nil {
		return err
	}

	if ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Offer failed: %d", ret)
	}

	return nil
}

// MigrateSegmentsRequest packet and offer
func (proxy *Proxy) MigrateSegmentsRequest(correlationID int64, srcRecordingID int64, destRecordingID int64, controlSessionId int64) error {
	// Create a packet and send it
	bytes, err := codecs.MigrateSegmentsRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, srcRecordingID, destRecordingID)
	if err != nil {
		return err
	}

	if ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Offer failed: %d", ret)
	}

	return nil
}

// AuthConnectRequest packet and offer
func (proxy *Proxy) AuthConnectRequest(correlationID int64, responseStream int32, responseChannel string, encodedCredentials []uint8) (bool, error) {

	// Create a packet and send it
	bytes, err := codecs.AuthConnectRequestPacket(proxy.marshaller, proxy.rangeChecking, correlationID, responseStream, responseChannel, encodedCredentials)
	if err != nil {
		return false, err
	}

	return proxy.OfferWithTimeout(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
}

// ChallengeResponse packet and offer
func (proxy *Proxy) ChallengeResponse(correlationID int64, encodedCredentials []uint8, controlSessionId int64) error {
	// Create a packet and send it
	bytes, err := codecs.ChallengeResponsePacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, encodedCredentials)
	if err != nil {
		return err
	}

	if ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Offer failed: %d", ret)
	}

	return nil
}

// KeepAliveRequest packet and offer
func (proxy *Proxy) KeepAliveRequest(correlationID int64, controlSessionId int64) error {
	// Create a packet and send it
	bytes, err := codecs.KeepAliveRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID)
	if err != nil {
		return err
	}

	if ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Offer failed: %d", ret)
	}

	return nil
}

func (proxy *Proxy) KeepAlive(controlSessionId, correlationId int64) (bool, error) {
	bytes, err := codecs.KeepAliveRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationId)
	if err != nil {
		return false, err
	}

	ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
	if ret < 0 {
		return false, fmt.Errorf("Offer failed: %d", ret)
	}

	return ret > 0, nil
}

// TaggedReplicateRequest packet and offer
func (proxy *Proxy) TaggedReplicateRequest(correlationID int64, srcRecordingID int64, dstRecordingID int64, channelTagID int64, subscriptionTagID int64, srcControlStreamID int32, srcControlChannel string, liveDestination string, controlSessionId int64) error {
	// Create a packet and send it
	bytes, err := codecs.TaggedReplicateRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, srcRecordingID, dstRecordingID, channelTagID, subscriptionTagID, srcControlStreamID, srcControlChannel, liveDestination)
	if err != nil {
		return err
	}

	if ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Offer failed: %d", ret)
	}

	return nil
}

// PurgeRecordingRequest packet and offer
func (proxy *Proxy) PurgeRecordingRequest(correlationID int64, replaySessionID int64, controlSessionId int64) error {
	// Create a packet and send it
	bytes, err := codecs.PurgeRecordingRequestPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationID, replaySessionID)
	if err != nil {
		return err
	}

	if ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Offer failed: %d", ret)
	}

	return nil
}
