package archive

import (
	"fmt"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/archive/codecs"
)

// ArchiveId Get the id of the archive
func (proxy *Proxy) ArchiveId(correlationId, controlSessionId int64) error {
	bytes, err := codecs.ArchiveIdPacket(proxy.marshaller, proxy.archive.Options.RangeChecking, controlSessionId, correlationId)
	if err != nil {
		return err
	}

	if ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Offer failed: %d", ret)
	}

	return nil

}

// TryChallengeResponse ...
func (proxy *Proxy) TryChallengeResponse(encodedCredentials []uint8, correlationId, controlSessionId int64) error {
	// Create a packet and send it
	bytes, err := codecs.ChallengeResponsePacket(proxy.marshaller, proxy.archive.Options.RangeChecking, controlSessionId, correlationId, encodedCredentials)
	if err != nil {
		return err
	}

	if ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Offer failed: %d", ret)
	}

	return nil
}

// TryConnect ...
func (proxy *Proxy) TryConnect(responseChannel string, responseStreamId int32, correlationID int64) error {

	// Create a packet and send it
	bytes, err := codecs.ConnectRequestPacket(proxy.marshaller, proxy.archive.Options.RangeChecking, correlationID, responseStreamId, responseChannel)
	if err != nil {
		return err
	}

	if ret := proxy.Offer(atomic.MakeBuffer(bytes, len(bytes)), 0, int32(len(bytes)), nil); ret < 0 {
		return fmt.Errorf("Offer failed: %d", ret)
	}

	return nil
}
