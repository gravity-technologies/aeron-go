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
func (proxy *Proxy) OfferV1(buffer *atomic.Buffer, offset int32, length int32, reservedValueSupplier term.ReservedValueSupplier) (bool, error) {

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

	return proxy.OfferV1(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
}

// TryChallengeResponse ...
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

// TryConnect ...
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
