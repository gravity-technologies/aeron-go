package archive

import (
	"fmt"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/archive/codecs"
)

// ArchiveId Get the id of the archive
func (proxy *Proxy) ArchiveId(correlationId, controlSessionId int64) (bool, error) {
	bytes, err := codecs.ArchiveIdPacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationId)
	if err != nil {
		return false, err
	}

	ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
	if ret < 0 {
		return false, fmt.Errorf("Offer failed: %d", ret)
	}

	return ret > 0, nil
}

// TryChallengeResponse ...
func (proxy *Proxy) TryChallengeResponse(encodedCredentials []uint8, correlationId, controlSessionId int64) (bool, error) {
	// Create a packet and send it
	bytes, err := codecs.ChallengeResponsePacket(proxy.marshaller, proxy.rangeChecking, controlSessionId, correlationId, encodedCredentials)
	if err != nil {
		return false, err
	}

	ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil)
	if ret < 0 {
		return false, fmt.Errorf("Offer failed: %d", ret)
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
		return false, fmt.Errorf("Offer failed: %d", ret)
	}

	return ret > 0, nil
}
