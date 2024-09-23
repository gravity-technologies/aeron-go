package archive

import (
	"fmt"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/archive/codecs"
)

var PROTOCOL_VERSION_WITH_ARCHIVE_ID = codecs.SemanticVersion()

type AsyncConnect struct {
	Ctx              *ArchiveContext
	State            AsyncConnectStateEnum
	CorrelationId    int64
	ControlSessionId int64

	encodedCredentialsFromChallenge []byte
	publicationRegistrationId       int64
	controlResponsePoller           *ControlResponsePoller
	archiveProxy                    *Proxy

	deadline time.Time
}

// Archive Connection State used internally for connection establishment
type AsyncConnectStateEnum uint8
type AsyncConnectStateValues struct {
	ADD_PUBLICATION              AsyncConnectStateEnum // 0
	AWAIT_PUBLICATION_CONNECTED  AsyncConnectStateEnum // 1
	SEND_CONNECT_REQUEST         AsyncConnectStateEnum // 2
	AWAIT_SUBSCRIPTION_CONNECTED AsyncConnectStateEnum // 3
	AWAIT_CONNECT_RESPONSE       AsyncConnectStateEnum // 4
	SEND_ARCHIVE_ID_REQUEST      AsyncConnectStateEnum // 5
	AWAIT_ARCHIVE_ID_RESPONSE    AsyncConnectStateEnum // 6
	DONE                         AsyncConnectStateEnum // 7
	SEND_CHALLENGE_RESPONSE      AsyncConnectStateEnum // 8
	AWAIT_CHALLENGE_RESPONSE     AsyncConnectStateEnum // 9
}

var State = AsyncConnectStateValues{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

func NewAsyncConnect(ctx *ArchiveContext) (*AsyncConnect, error) {
	ac := &AsyncConnect{
		Ctx:   ctx,
		State: State.ADD_PUBLICATION,
	}

	subscription, err := ac.Ctx.Aeron.AddSubscription(ac.Ctx.ControlResponseChannel, ac.Ctx.ControlResponseStreamId)
	if err != nil {
		return nil, err
	}
	ac.controlResponsePoller = NewControlResponsePoller(subscription, ControlFragmentLimit)

	if err = ac.checkAndSetupResponseChannel(ac.controlResponsePoller.Subscription); err != nil {
		return nil, err
	}

	publicationRegistrationId, err := ac.Ctx.Aeron.AsyncAddExclusivePublication(ac.Ctx.ControlRequestChannel, ac.Ctx.ControlRequestStreamId)
	if err != nil {
		return nil, err
	}
	ac.publicationRegistrationId = publicationRegistrationId
	ac.deadline = time.Now().Add(ac.Ctx.MessageTimeout)
	return ac, nil
}

func (ac *AsyncConnect) checkAndSetupResponseChannel(subscription *aeron.Subscription) error {
	uri, err := aeron.ParseChannelUri(ac.Ctx.ControlResponseChannel)
	logger.Debugf("parsed responseChannel uri: %s", uri.String())
	if err != nil {
		return err
	}
	if uri.Get(aeron.MdcControlModeParamName) == "response" {
		requestChannelURI, err := aeron.ParseChannelUri(ac.Ctx.ControlRequestChannel)
		if err != nil {
			return err
		}
		requestChannelURI.Set(aeron.ResponseCorrelationIdParamName, string(subscription.RegistrationID()))
		ac.Ctx.ControlRequestChannel = requestChannelURI.String()
		// ac.Ctx.ArchiveOptions.RequestChannel = requestChannelURI.String()
		logger.Debugf("new requestChannel uri: %s", requestChannelURI.String())
	}
	return err
}

func (ac *AsyncConnect) Poll() (aeronArchive *Archive, err error) {
	if err := ac.checkDeadline(); err != nil {
		return nil, err
	}

	currState := ac.State

	if State.ADD_PUBLICATION == currState {
		// Create the publication half for the proxy that looks after sending requests on that
		publication, err := ac.Ctx.Aeron.GetExclusivePublication(ac.publicationRegistrationId)
		if err != nil {
			return nil, err
		}
		if publication != nil {
			ac.publicationRegistrationId = aeron.NullValue
			ac.archiveProxy = &Proxy{
				Publication: publication,
				marshaller:  codecs.NewSbeGoMarshaller(),
			}
			ac.State = State.AWAIT_PUBLICATION_CONNECTED
		}
	}

	if State.AWAIT_PUBLICATION_CONNECTED == currState {
		if !ac.archiveProxy.Publication.IsConnected() {
			return nil, nil
		}
		ac.State = State.SEND_CONNECT_REQUEST
	}

	if State.SEND_CONNECT_REQUEST == currState {
		responseChannel := ac.controlResponsePoller.Subscription.TryResolveChannelEndpointPort()
		if responseChannel == "" {
			return nil, nil
		}

		ac.CorrelationId = ac.Ctx.Aeron.NextCorrelationID()
		if err = ac.archiveProxy.TryConnect(responseChannel, ac.Ctx.ControlResponseStreamId, ac.CorrelationId); err != nil {
			return nil, err
		}

		ac.State = State.AWAIT_SUBSCRIPTION_CONNECTED
	}

	if State.AWAIT_SUBSCRIPTION_CONNECTED == currState {
		if !ac.controlResponsePoller.Subscription.IsConnected() {
			return nil, nil
		}

		ac.State = State.AWAIT_CONNECT_RESPONSE
	}

	if State.SEND_ARCHIVE_ID_REQUEST == currState {
		if err = ac.archiveProxy.ArchiveId(ac.CorrelationId, ac.ControlSessionId); err != nil {
			return nil, err
		}

		ac.State = State.AWAIT_ARCHIVE_ID_RESPONSE
	}

	if State.SEND_CHALLENGE_RESPONSE == currState {
		if err = ac.archiveProxy.TryChallengeResponse(ac.encodedCredentialsFromChallenge, ac.CorrelationId, ac.ControlSessionId); err != nil {
			return nil, err
		}

		ac.State = State.AWAIT_CHALLENGE_RESPONSE
	}

	ac.controlResponsePoller.Poll()

	if ac.controlResponsePoller.IsPollComplete &&
		ac.controlResponsePoller.CorrelationId == ac.CorrelationId {
		ac.ControlSessionId = ac.controlResponsePoller.ControlSessionId
		if ac.controlResponsePoller.WasChallenged() {
			// TODO: real security credentials supplier is not part of this for now
			// ac.encodedCredentialsFromChallenge = ac.Ctx.credentialsSupplier().onChallenge(
			// ac.controlResponsePoller.EncodedChallenge)
			// ---
			// We are currrently NOT using AuthChallenge, so skip implementation for now
			// ---
			// From existing aeron-go code
			// Check the challenge is expected if our option for this is not nil
			// if ac.Ctx.AuthChallenge != nil {
			// 	if !bytes.Equal(ac.Ctx.AuthChallenge, ac.controlResponsePoller.EncodedChallenge) {
			// 		return nil, fmt.Errorf("ChallengeResponse Unexpected: expected:%v received:%v", ac.Ctx.AuthChallenge, ac.controlResponsePoller.EncodedChallenge)
			// 		return
			// 	}
			// }
			// ---

			ac.encodedCredentialsFromChallenge = ac.controlResponsePoller.EncodedChallenge

			ac.CorrelationId = ac.Ctx.Aeron.NextCorrelationID()

			ac.State = State.SEND_CHALLENGE_RESPONSE
		} else {
			code := ac.controlResponsePoller.Code
			if codecs.ControlResponseCode.ERROR == code {
				errorMessage := ac.controlResponsePoller.ErrorMessage
				errorCode := ac.controlResponsePoller.RelevantId
				return nil, NewArchiveError(ac.CorrelationId, int(errorCode), errorMessage)
			}
			return nil, NewArchiveError(ac.CorrelationId, aeron.NullValue, fmt.Sprintf("unexpected response: code=%d", code))
		}

		if State.AWAIT_ARCHIVE_ID_RESPONSE == currState {
			archiveId := ac.controlResponsePoller.RelevantId
			aeronArchive, err = ac.transitionToDone(archiveId)
		} else {
			archiveProtocolVersion := ac.controlResponsePoller.Version
			if archiveProtocolVersion < PROTOCOL_VERSION_WITH_ARCHIVE_ID {
				aeronArchive, err = ac.transitionToDone(aeron.NullValue)
			} else {
				ac.CorrelationId = ac.Ctx.Aeron.NextCorrelationID()
				ac.State = State.SEND_ARCHIVE_ID_REQUEST
			}
		}
	}

	return
}

func (ac *AsyncConnect) transitionToDone(archiveId int64) (aeronArchive *Archive, err error) {
	if err = ac.archiveProxy.KeepAlive(ac.ControlSessionId, aeron.NullValue); err != nil {
		ac.archiveProxy.CloseSession(ac.ControlSessionId)
		return nil, NewArchiveError(-1, -1, "failed to send keep alive after archive connect")
	}

	aeronArchive = NewArchiveV2(ac.Ctx, ac.controlResponsePoller, ac.archiveProxy, ac.ControlSessionId, archiveId)

	ac.State = State.DONE
	return
}

func (ac *AsyncConnect) checkDeadline() error {
	if time.Now().After(ac.deadline) {
		uriInfo := " subscription.uri=" + ac.Ctx.ControlResponseChannel
		if ac.State < 3 {
			uriInfo = " publication.uri=" + ac.Ctx.ControlRequestChannel
		}
		return fmt.Errorf("Archive connect timeout: step=%d %s", ac.State, uriInfo)
	}
	return nil
}
