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

package cluster

import (
	"fmt"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
)

const (
	ClientSessionMockedOffer = 1
)

type ClientSession interface {
	Id() int64
	ResponseStreamId() int32
	ResponseChannel() string
	EncodedPrincipal() []byte
	Connect(aeronClient *aeron.Aeron) error
	Close()
	// TODO: the other close methods are not part of interface.
	// I don't understand the closing bool implementation and why it is needed
	// Leaving out for now unless it is really important
	// IsClosing() bool
	Offer(*atomic.Buffer, int32, int32, term.ReservedValueSupplier) int64
	// TryClaim(...)
	Disconnect()
}

type containerClientSession struct {
	id               int64
	responseStreamId int32
	responseChannel  string
	encodedPrincipal []byte
	agent            *ClusteredServiceAgent
	response         *aeron.Publication
	isClosing        bool
}

func newContainerClientSession(
	id int64,
	responseStreamId int32,
	responseChannel string,
	encodedPrincipal []byte,
	agent *ClusteredServiceAgent,
) (*containerClientSession, error) {
	return &containerClientSession{
		id:               id,
		responseStreamId: responseStreamId,
		responseChannel:  responseChannel,
		encodedPrincipal: encodedPrincipal,
		agent:            agent,
	}, nil
}

func (s *containerClientSession) Id() int64 {
	return s.id
}

func (s *containerClientSession) ResponseStreamId() int32 {
	return s.responseStreamId
}

func (s *containerClientSession) ResponseChannel() string {
	return s.responseChannel
}

func (s *containerClientSession) EncodedPrincipal() []byte {
	return s.encodedPrincipal
}

func (s *containerClientSession) Connect(aeronClient *aeron.Aeron) error {
	pub, err := aeronClient.AddPublication(s.responseChannel, s.responseStreamId)
	if err != nil {
		return fmt.Errorf("failed to connect session response publication: %s", err)
	}
	s.response = pub
	return nil
}

func (s *containerClientSession) Close() {
	if _, ok := s.agent.getClientSession(s.id); ok {
		s.agent.closeClientSession(s.id)
	}
}

func (s *containerClientSession) Disconnect() {
	if err := s.response.Close(); err != nil {
		logger.Warningf("Failed to disconnect publication: %v", err)
	}
	s.response = nil
}

func (s *containerClientSession) Offer(
	buffer *atomic.Buffer,
	offset int32,
	length int32,
	reservedValueSupplier term.ReservedValueSupplier,
) int64 {
	return s.agent.offerToSession(
		s.id,
		s.response,
		buffer,
		offset,
		length,
		reservedValueSupplier,
	)
}

func (s *containerClientSession) IsClosing() bool {
	return s.isClosing
}

func (s *containerClientSession) MarkClosing() {
	s.isClosing = true
}

func (s *containerClientSession) ResetClosing() {
	s.isClosing = false
}
