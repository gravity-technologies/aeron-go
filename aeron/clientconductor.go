/*
Copyright 2016-2018 Stanislav Liberman
Copyright (C) 2022 Talos, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package aeron

import (
	"errors"
	"fmt"
	"io"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/broadcast"
	ctr "github.com/lirm/aeron-go/aeron/counters"
	"github.com/lirm/aeron-go/aeron/driver"
	"github.com/lirm/aeron-go/aeron/idlestrategy"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logging"
)

var RegistrationStatus = struct {
	AwaitingMediaDriver   int
	RegisteredMediaDriver int
	ErroredMediaDriver    int
}{
	0,
	1,
	2,
}

const (
	keepaliveTimeoutNS = 500 * int64(time.Millisecond)
	resourceTimeoutNS  = 1000 * int64(time.Millisecond)

	// heartbeatTypeId is the type id of a heartbeat counter.
	heartbeatTypeId = int32(11)

	// registrationIdOffset is the offset in the key metadata for the registration id of the counter.
	heartheatRegistrationIdOffset = int32(0)
)

type publicationStateDefn struct {
	regID                    int64
	origRegID                int64
	timeOfRegistration       int64
	streamID                 int32
	sessionID                int32
	posLimitCounterID        int32
	channelStatusIndicatorID int32
	errorCode                int32
	status                   int
	channel                  string
	errorMessage             string
	buffers                  *logbuffer.LogBuffers
	publication              *Publication
}

func (pub *publicationStateDefn) Init(channel string, regID int64, streamID int32, now int64) *publicationStateDefn {
	pub.channel = channel
	pub.regID = regID
	pub.streamID = streamID
	pub.sessionID = -1
	pub.posLimitCounterID = -1
	pub.timeOfRegistration = now
	pub.status = RegistrationStatus.AwaitingMediaDriver

	return pub
}

type subscriptionStateDefn struct {
	regID                   int64
	timeOfRegistration      int64
	streamID                int32
	errorCode               int32
	status                  int
	channel                 string
	errorMessage            string
	availableImageHandler   AvailableImageHandler
	unavailableImageHandler UnavailableImageHandler
	subscription            *Subscription
}

func (sub *subscriptionStateDefn) Init(
	ch string,
	regID int64,
	sID int32,
	now int64,
	availableImageHandler AvailableImageHandler,
	UnavailableImageHandler UnavailableImageHandler) *subscriptionStateDefn {
	sub.channel = ch
	sub.regID = regID
	sub.streamID = sID
	sub.timeOfRegistration = now
	sub.status = RegistrationStatus.AwaitingMediaDriver
	sub.availableImageHandler = availableImageHandler
	sub.unavailableImageHandler = UnavailableImageHandler

	return sub
}

type counterStateDefn struct {
	timeOfRegistration int64
	counterId          int32
	errorCode          int32
	errorMessage       string
	status             int
	counter            *Counter
}

func (c *counterStateDefn) Init(time int64) {
	c.timeOfRegistration = time
	c.status = RegistrationStatus.AwaitingMediaDriver
}

type lingerResourse struct {
	lastTime int64
	resource io.Closer
}

type IdAndAvailableCounterHandler struct {
	registrationId int64
	handler        AvailableCounterHandler
}

func NewIdAndAvailablePair(registrationId int64, handler AvailableCounterHandler) *IdAndAvailableCounterHandler {
	ret := new(IdAndAvailableCounterHandler)
	ret.registrationId = registrationId
	ret.handler = handler
	return ret
}

type IdAndUnavailableCounterHandler struct {
	registrationId int64
	handler        UnavailableCounterHandler
}

func NewIdAndUnavailablePair(registrationId int64, handler UnavailableCounterHandler) *IdAndUnavailableCounterHandler {
	ret := new(IdAndUnavailableCounterHandler)
	ret.registrationId = registrationId
	ret.handler = handler
	return ret
}

type DriverProxy interface {
	ClientID() int64
	TimeOfLastDriverKeepalive() int64
	NextCorrelationID() int64
	AddSubscription(channel string, streamID int32) (int64, error)
	RemoveSubscription(registrationID int64) error
	AddPublication(channel string, streamID int32) (int64, error)
	AddExclusivePublication(channel string, streamID int32) (int64, error)
	RemovePublication(registrationID int64) error
	ClientClose() error
	AddDestination(registrationID int64, channel string) (int64, error)
	RemoveDestination(registrationID int64, channel string) (int64, error)
	AddRcvDestination(registrationID int64, channel string) (int64, error)
	RemoveRcvDestination(registrationID int64, channel string) (int64, error)
	AddCounter(typeId int32, keyBuffer *atomic.Buffer, keyOffset int32, keyLength int32,
		labelBuffer *atomic.Buffer, labelOffset int32, labelLength int32) (int64, error)
	AddCounterByLabel(typeId int32, label string) (int64, error)
	RemoveCounter(registrationId int64) (int64, error)
}

// ImageFactory allows tests to use fake Images
type ImageFactory func(sessionID int32, corrID int64, logFilename string, subRegId int64, sourceIdentity string,
	counterValuesBuffer *atomic.Buffer, subscriberPositionID int32) Image

type ClientConductor struct {
	pubs     []*publicationStateDefn
	subs     []*subscriptionStateDefn
	counters map[int64]*counterStateDefn

	driverProxy DriverProxy

	counterValuesBuffer *atomic.Buffer
	counterReader       *ctr.Reader

	driverListenerAdapter *driver.ListenerAdapter

	adminLock sync.Mutex

	pendingCloses      map[int64]chan bool
	lingeringResources chan lingerResourse

	onNewPublicationHandler   NewPublicationHandler
	onNewSubscriptionHandler  NewSubscriptionHandler
	onAvailableImageHandler   AvailableImageHandler
	onUnavailableImageHandler UnavailableImageHandler

	// Ordering is only important in that the 0-index element must be called first on [un]available counters.
	availableCounterHandlers   []*IdAndAvailableCounterHandler
	unavailableCounterHandlers []*IdAndUnavailableCounterHandler

	errorHandler func(error)
	imageFactory ImageFactory

	running          atomic.Bool
	conductorRunning atomic.Bool
	driverActive     atomic.Bool

	timeOfLastKeepalive             int64
	timeOfLastCheckManagedResources int64
	timeOfLastDoWork                int64
	driverTimeoutNs                 int64
	interServiceTimeoutNs           int64
	publicationConnectionTimeoutNs  int64
	resourceLingerTimeoutNs         int64

	heartbeatTimestamp *ctr.AtomicCounter
}

// Init is the primary initialization method for ClientConductor
func (cc *ClientConductor) Init(driverProxy DriverProxy, bcast *broadcast.CopyReceiver,
	interServiceTo, driverTo, pubConnectionTo, lingerTo time.Duration, counters *ctr.MetaDataFlyweight) *ClientConductor {

	logger.Debugf("Initializing ClientConductor with: %v %v %d %d %d", driverProxy, bcast, interServiceTo,
		driverTo, pubConnectionTo)

	cc.driverProxy = driverProxy
	cc.running.Set(true)
	cc.driverActive.Set(true)
	cc.driverListenerAdapter = driver.NewAdapter(cc, bcast)
	cc.interServiceTimeoutNs = interServiceTo.Nanoseconds()
	cc.driverTimeoutNs = driverTo.Nanoseconds()
	cc.publicationConnectionTimeoutNs = pubConnectionTo.Nanoseconds()
	cc.resourceLingerTimeoutNs = lingerTo.Nanoseconds()

	cc.counterValuesBuffer = counters.ValuesBuf.Get()
	cc.counterReader = ctr.NewReader(counters.ValuesBuf.Get(), counters.MetaDataBuf.Get())

	cc.pendingCloses = make(map[int64]chan bool)
	cc.lingeringResources = make(chan lingerResourse, 1024)
	cc.imageFactory = DefaultImageFactory

	cc.pubs = make([]*publicationStateDefn, 0)
	cc.subs = make([]*subscriptionStateDefn, 0)
	cc.counters = make(map[int64]*counterStateDefn)
	cc.availableCounterHandlers = make([]*IdAndAvailableCounterHandler, 0)
	cc.unavailableCounterHandlers = make([]*IdAndUnavailableCounterHandler, 0)
	return cc
}

// Close will terminate the Run() goroutine body and close all active publications and subscription. Run() can
// be restarted in a another goroutine.
func (cc *ClientConductor) Close() (err error) {
	logger.Debugf("Closing ClientConductor")

	now := time.Now().UnixNano()

	running := cc.running.Get()

	cc.closeAllResources(now)
	if running {
		cc.driverProxy.ClientClose()
	}

	timeoutDuration := 5 * time.Second
	timeout := time.Now().Add(timeoutDuration)
	for cc.conductorRunning.Get() && time.Now().Before(timeout) {
		time.Sleep(10 * time.Millisecond)
	}
	if cc.conductorRunning.Get() {
		msg := fmt.Sprintf("failed to stop conductor after %v", timeoutDuration)
		logger.Warning(msg)
		err = errors.New(msg)
	}

	logger.Debugf("Closed ClientConductor")
	return err
}

// Start begins the main execution loop of ClientConductor on a goroutine.
func (cc *ClientConductor) Start(idleStrategy idlestrategy.Idler) {
	cc.running.Set(true)
	go cc.run(idleStrategy)
}

// run is the main execution loop of ClientConductor.
func (cc *ClientConductor) run(idleStrategy idlestrategy.Idler) {
	now := time.Now().UnixNano()
	cc.timeOfLastKeepalive = now
	cc.timeOfLastCheckManagedResources = now
	cc.timeOfLastDoWork = now

	// Stay on the same thread for performance
	runtime.LockOSThread()

	// Clean exit from this particular go routine
	defer func() {
		if err := recover(); err != nil {
			errStr := fmt.Sprintf("Panic: %v", err)
			logger.Error(errStr)
			cc.onError(errors.New(errStr))
		}

		cc.running.Set(false)
		cc.forceCloseResources()
		cc.conductorRunning.Set(false)

		logger.Infof("ClientConductor done")
	}()

	cc.conductorRunning.Set(true)
	for cc.running.Get() {
		workCount, err := cc.doWork()
		if err != nil {
			cc.onError(err)
			return
		}
		idleStrategy.Idle(workCount)
	}
}

func (cc *ClientConductor) forceCloseResources() {
	for {
		select {
		case r := <-cc.lingeringResources:
			logger.Debugf("Force closing resource: %v", r)
			res := r.resource
			if res != nil {
				err := res.Close()
				if err != nil {
					logger.Warningf("Failed to force close resource: %v", err)
					cc.onError(err)
				}
			}
		default:
			return
		}
	}
}

func (cc *ClientConductor) doWork() (int, error) {
	workCount := cc.driverListenerAdapter.ReceiveMessages()
	heartbeats, err := cc.onHeartbeatCheckTimeouts()
	return workCount + heartbeats, err
}

func (cc *ClientConductor) getDriverStatus() error {
	if cc.driverActive.Get() {
		return nil
	} else {
		return errors.New("driver is inactive")
	}
}

// AddPublication sends the add publication command through the driver proxy
func (cc *ClientConductor) AddPublication(channel string, streamID int32) (int64, error) {
	logger.Debugf("AddPublication: channel=%s, streamId=%d", channel, streamID)

	if err := cc.getDriverStatus(); err != nil {
		return 0, err
	}

	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()

	now := time.Now().UnixNano()

	regID, err := cc.driverProxy.AddPublication(channel, streamID)
	if err != nil {
		return 0, err
	}

	pubState := new(publicationStateDefn)
	pubState.Init(channel, regID, streamID, now)

	cc.pubs = append(cc.pubs, pubState)

	return regID, nil
}

// AddExclusivePublication sends the add publication command through the driver proxy
func (cc *ClientConductor) AddExclusivePublication(channel string, streamID int32) (int64, error) {
	logger.Debugf("AddExclusivePublication: channel=%s, streamId=%d", channel, streamID)

	if err := cc.getDriverStatus(); err != nil {
		return 0, err
	}

	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()

	now := time.Now().UnixNano()

	regID, err := cc.driverProxy.AddExclusivePublication(channel, streamID)
	if err != nil {
		return 0, err
	}

	pubState := new(publicationStateDefn)
	pubState.Init(channel, regID, streamID, now)

	cc.pubs = append(cc.pubs, pubState)

	return regID, nil
}

func (cc *ClientConductor) FindPublication(registrationID int64) (*Publication, error) {

	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()

	var publication *Publication
	for _, pub := range cc.pubs {
		if pub.regID != registrationID {
			continue
		}
		if pub.publication != nil {
			return pub.publication, nil
		}
		switch pub.status {
		case RegistrationStatus.AwaitingMediaDriver:
			return nil, timeoutExceeded(pub.timeOfRegistration, cc.driverTimeoutNs)
		case RegistrationStatus.RegisteredMediaDriver:
			publication = NewPublication(pub.buffers)
			publication.conductor = cc
			publication.channel = pub.channel
			publication.regID = registrationID
			publication.originalRegID = pub.origRegID
			publication.streamID = pub.streamID
			publication.sessionID = pub.sessionID
			publication.pubLimit = NewPosition(cc.counterValuesBuffer, pub.posLimitCounterID)
			publication.channelStatusIndicatorID = pub.channelStatusIndicatorID
			pub.publication = publication
			return publication, nil
		case RegistrationStatus.ErroredMediaDriver:
			return nil, fmt.Errorf("error on %d: %d: %s", registrationID, pub.errorCode, pub.errorMessage)
		default:
			return nil, errors.New("unknown registration status")
		}
	}
	return nil, fmt.Errorf("registration ID %d cannot be found", registrationID)
}

func (cc *ClientConductor) releasePublication(regID int64) error {
	logger.Debugf("ReleasePublication: regID=%d", regID)

	if err := cc.getDriverStatus(); err != nil {
		return err
	}

	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()

	now := time.Now().UnixNano()

	pubcnt := len(cc.pubs)
	for i, pub := range cc.pubs {
		if pub != nil && pub.regID == regID {
			if err := cc.driverProxy.RemovePublication(regID); err != nil {
				return err
			}

			cc.pubs[i] = cc.pubs[pubcnt-1]
			cc.pubs[pubcnt-1] = nil
			pubcnt--

			if pub.buffers.DecRef() == 0 {
				cc.lingeringResources <- lingerResourse{now, pub.buffers}
			}
		}
	}
	cc.pubs = cc.pubs[:pubcnt]
	return nil
}

// AddSubscription sends the add subscription command through the driver proxy
func (cc *ClientConductor) AddSubscription(channel string, streamID int32) (int64, error) {
	return cc.AddSubscriptionWithHandlers(channel, streamID,
		cc.onAvailableImageHandler, cc.onUnavailableImageHandler)
}

// AddSubscriptionWithHandlers sends the add subscription command through the driver proxy.  It will use the specified Handlers for
// available/unavailable Images instead of the default handlers.
func (cc *ClientConductor) AddSubscriptionWithHandlers(channel string, streamID int32,
	onAvailableImage AvailableImageHandler, onUnavailableImage UnavailableImageHandler) (int64, error) {
	logger.Debugf("AddSubscription: channel=%s, streamId=%d", channel, streamID)

	if err := cc.getDriverStatus(); err != nil {
		return 0, err
	}

	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()

	now := time.Now().UnixNano()

	regID, err := cc.driverProxy.AddSubscription(channel, streamID)
	if err != nil {
		return 0, err
	}

	subState := new(subscriptionStateDefn)
	subState.Init(channel, regID, streamID, now, onAvailableImage, onUnavailableImage)

	cc.subs = append(cc.subs, subState)

	return regID, nil
}

// FindSubscription by Registration ID, which is returned by AddSubscription.  Returns the Subscription or an error.
// A pending Subscription will return nil,nil signifying that there is neither a Subscription nor an error.
func (cc *ClientConductor) FindSubscription(registrationID int64) (*Subscription, error) {
	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()

	for _, sub := range cc.subs {
		if sub.regID != registrationID {
			continue
		}
		switch sub.status {
		case RegistrationStatus.AwaitingMediaDriver:
			return nil, timeoutExceeded(sub.timeOfRegistration, cc.driverTimeoutNs)
		case RegistrationStatus.RegisteredMediaDriver:
			return sub.subscription, nil
		case RegistrationStatus.ErroredMediaDriver:
			return nil, fmt.Errorf("error on %d: %d: %s", registrationID, sub.errorCode, sub.errorMessage)
		default:
			return nil, errors.New("unknown registration status")
		}
	}

	return nil, fmt.Errorf("registration ID %d cannot be found", registrationID)
}

func timeoutExceeded(timeOfRegistration int64, driverTimeoutNs int64) error {
	if now := time.Now().UnixNano(); now > (timeOfRegistration + driverTimeoutNs) {
		return fmt.Errorf("no response from driver. started: %d, now: %d, to: %d",
			timeOfRegistration/time.Millisecond.Nanoseconds(),
			now/time.Millisecond.Nanoseconds(),
			driverTimeoutNs/time.Millisecond.Nanoseconds())
	}
	return nil
}

func (cc *ClientConductor) releaseSubscription(regID int64, images []Image) error {
	logger.Debugf("ReleaseSubscription: regID=%d", regID)

	if err := cc.getDriverStatus(); err != nil {
		return err
	}

	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()

	now := time.Now().UnixNano()

	subcnt := len(cc.subs)
	for i, sub := range cc.subs {
		if sub != nil && sub.regID == regID {
			if logger.IsEnabledFor(logging.DEBUG) {
				logger.Debugf("Removing subscription: %d; %v", regID, images)
			}

			if err := cc.driverProxy.RemoveSubscription(regID); err != nil {
				return err
			}

			cc.subs[i] = cc.subs[subcnt-1]
			cc.subs[subcnt-1] = nil
			subcnt--
			var handler func(Image)
			if sub.subscription != nil {
				handler = sub.subscription.UnavailableImageHandler()
			}

			for i := range images {
				image := images[i]
				if handler != nil {
					handler(image)
				}
				cc.lingeringResources <- lingerResourse{now, image}
			}
		}
	}
	cc.subs = cc.subs[:subcnt]
	return nil
}

func (cc *ClientConductor) releaseCounter(counter Counter) error {
	logger.Debugf("releaseCounter: regId=%d", counter.RegistrationId())

	if err := cc.getDriverStatus(); err != nil {
		return err
	}

	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()

	registrationId := counter.RegistrationId()
	if _, ok := cc.counters[registrationId]; ok {
		delete(cc.counters, registrationId)
		_, err := cc.driverProxy.RemoveCounter(registrationId)
		return err
	}
	return nil
}

// AddDestination sends the add destination command through the driver proxy
func (cc *ClientConductor) AddDestination(registrationID int64, endpointChannel string) error {
	logger.Debugf("AddDestination: regID=%d endpointChannel=%s", registrationID, endpointChannel)

	if err := cc.getDriverStatus(); err != nil {
		return err
	}

	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()

	_, err := cc.driverProxy.AddDestination(registrationID, endpointChannel)
	return err
}

// RemoveDestination sends the remove destination command through the driver proxy
func (cc *ClientConductor) RemoveDestination(registrationID int64, endpointChannel string) error {
	logger.Debugf("RemoveDestination: regID=%d endpointChannel=%s", registrationID, endpointChannel)

	if err := cc.getDriverStatus(); err != nil {
		return err
	}

	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()

	_, err := cc.driverProxy.RemoveDestination(registrationID, endpointChannel)
	return err
}

// AddRcvDestination sends the add rcv destination command through the driver proxy
func (cc *ClientConductor) AddRcvDestination(registrationID int64, endpointChannel string) error {
	logger.Debugf("AddRcvDestination: regID=%d endpointChannel=%s", registrationID, endpointChannel)

	if err := cc.getDriverStatus(); err != nil {
		return err
	}

	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()

	_, err := cc.driverProxy.AddRcvDestination(registrationID, endpointChannel)
	return err
}

// RemoveRcvDestination sends the remove rcv destination command through the driver proxy
func (cc *ClientConductor) RemoveRcvDestination(registrationID int64, endpointChannel string) error {
	logger.Debugf("RemoveRcvDestination: regID=%d endpointChannel=%s", registrationID, endpointChannel)

	if err := cc.getDriverStatus(); err != nil {
		return err
	}

	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()

	_, err := cc.driverProxy.RemoveRcvDestination(registrationID, endpointChannel)
	return err
}

func (cc *ClientConductor) AddCounter(typeId int32, keyBuffer *atomic.Buffer, keyOffset int32, keyLength int32,
	labelBuffer *atomic.Buffer, labelOffset int32, labelLength int32) (int64, error) {
	logger.Debugf("AddCounter: typeId=%d", typeId)

	if err := cc.getDriverStatus(); err != nil {
		return 0, err
	}
	if keyLength < 0 || keyLength > ctr.MaxKeyLength {
		return 0, fmt.Errorf("key length out of bounds: %d", keyLength)
	}
	if labelLength < 0 || labelLength > ctr.MaxLabelLength {
		return 0, fmt.Errorf("label length out of bounds: %d", labelLength)
	}
	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()

	now := time.Now().UnixNano()

	registrationId, err := cc.driverProxy.AddCounter(
		typeId, keyBuffer, keyOffset, keyLength, labelBuffer, labelOffset, labelLength)
	if err != nil {
		return 0, err
	}
	counterState := new(counterStateDefn)
	counterState.Init(now)
	cc.counters[registrationId] = counterState

	return registrationId, nil
}

func (cc *ClientConductor) AddCounterByLabel(typeId int32, label string) (int64, error) {
	logger.Debugf("AddCounterByLabel: typeId=%d, label=%s", typeId, label)

	if err := cc.getDriverStatus(); err != nil {
		return 0, err
	}
	if int32(len(label)) > ctr.MaxLabelLength {
		return 0, fmt.Errorf("label length out of bounds: %d", len(label))
	}
	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()

	now := time.Now().UnixNano()

	registrationId, err := cc.driverProxy.AddCounterByLabel(typeId, label)
	if err != nil {
		return 0, err
	}
	counterState := new(counterStateDefn)
	counterState.Init(now)
	cc.counters[registrationId] = counterState

	return registrationId, nil
}

func (cc *ClientConductor) FindCounter(registrationID int64) (*Counter, error) {
	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()
	counterDef, ok := cc.counters[registrationID]
	if !ok {
		return nil, fmt.Errorf("registration ID %d cannot be found", registrationID)
	}
	if counterDef.counter != nil {
		return counterDef.counter, nil
	}
	switch counterDef.status {
	case RegistrationStatus.AwaitingMediaDriver:
		return nil, timeoutExceeded(counterDef.timeOfRegistration, cc.driverTimeoutNs)
	case RegistrationStatus.RegisteredMediaDriver:
		counter, err := NewCounter(registrationID, cc, counterDef.counterId)
		if err != nil {
			return nil, err
		}
		counterDef.counter = counter
		return counter, nil
	case RegistrationStatus.ErroredMediaDriver:
		return nil, fmt.Errorf("error on %d: %d: %s",
			registrationID, counterDef.errorCode, counterDef.errorMessage)
	default:
		return nil, errors.New("unknown registration status")
	}

}

func (cc *ClientConductor) AddAvailableCounterHandler(handler AvailableCounterHandler) int64 {
	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()
	registrationID := cc.driverProxy.NextCorrelationID()
	cc.availableCounterHandlers = append(cc.availableCounterHandlers, NewIdAndAvailablePair(registrationID, handler))
	return registrationID
}

func (cc *ClientConductor) RemoveAvailableCounterHandlerById(registrationId int64) bool {
	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()
	for i, pair := range cc.availableCounterHandlers {
		if pair.registrationId == registrationId {
			cc.availableCounterHandlers[i] = cc.availableCounterHandlers[len(cc.availableCounterHandlers)-1]
			cc.availableCounterHandlers = cc.availableCounterHandlers[:len(cc.availableCounterHandlers)-1]
			return true
		}
	}
	return false
}

func (cc *ClientConductor) RemoveAvailableCounterHandler(handler AvailableCounterHandler) bool {
	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()
	for i, pair := range cc.availableCounterHandlers {
		if pair.handler == handler {
			cc.availableCounterHandlers[i] = cc.availableCounterHandlers[len(cc.availableCounterHandlers)-1]
			cc.availableCounterHandlers = cc.availableCounterHandlers[:len(cc.availableCounterHandlers)-1]
			return true
		}
	}
	return false
}

func (cc *ClientConductor) AddUnavailableCounterHandler(handler UnavailableCounterHandler) int64 {
	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()
	registrationID := cc.driverProxy.NextCorrelationID()
	cc.unavailableCounterHandlers = append(cc.unavailableCounterHandlers, NewIdAndUnavailablePair(registrationID, handler))
	return registrationID
}

func (cc *ClientConductor) RemoveUnavailableCounterHandlerById(registrationId int64) bool {
	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()
	for i, pair := range cc.unavailableCounterHandlers {
		if pair.registrationId == registrationId {
			cc.unavailableCounterHandlers[i] = cc.unavailableCounterHandlers[len(cc.unavailableCounterHandlers)-1]
			cc.unavailableCounterHandlers = cc.unavailableCounterHandlers[:len(cc.unavailableCounterHandlers)-1]
			return true
		}
	}
	return false
}

func (cc *ClientConductor) RemoveUnavailableCounterHandler(handler UnavailableCounterHandler) bool {
	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()
	for i, pair := range cc.unavailableCounterHandlers {
		if &pair.handler == &handler {
			cc.unavailableCounterHandlers[i] = cc.unavailableCounterHandlers[len(cc.unavailableCounterHandlers)-1]
			cc.unavailableCounterHandlers = cc.unavailableCounterHandlers[:len(cc.unavailableCounterHandlers)-1]
			return true
		}
	}
	return false
}

func (cc *ClientConductor) OnNewPublication(streamID int32, sessionID int32, posLimitCounterID int32,
	channelStatusIndicatorID int32, logFileName string, regID int64, origRegID int64) {

	logger.Debugf("OnNewPublication: streamId=%d, sessionId=%d, posLimitCounterID=%d, channelStatusIndicatorID=%d, logFileName=%s, correlationID=%d, regID=%d",
		streamID, sessionID, posLimitCounterID, channelStatusIndicatorID, logFileName, regID, origRegID)

	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()

	for _, pubDef := range cc.pubs {
		if pubDef.regID == regID {
			pubDef.status = RegistrationStatus.RegisteredMediaDriver
			pubDef.sessionID = sessionID
			pubDef.posLimitCounterID = posLimitCounterID
			pubDef.channelStatusIndicatorID = channelStatusIndicatorID
			pubDef.buffers = logbuffer.Wrap(logFileName)
			pubDef.buffers.IncRef()
			pubDef.origRegID = origRegID

			logger.Debugf("Updated publication: %v", pubDef)

			if cc.onNewPublicationHandler != nil {
				cc.onNewPublicationHandler(pubDef.channel, streamID, sessionID, regID)
			}
		}
	}
}

// TODO Implement logic specific to exclusive publications
func (cc *ClientConductor) OnNewExclusivePublication(streamID int32, sessionID int32, posLimitCounterID int32,
	channelStatusIndicatorID int32, logFileName string, regID int64, origRegID int64) {

	logger.Debugf("OnNewExclusivePublication: streamId=%d, sessionId=%d, posLimitCounterID=%d, channelStatusIndicatorID=%d, logFileName=%s, correlationID=%d, regID=%d",
		streamID, sessionID, posLimitCounterID, channelStatusIndicatorID, logFileName, regID, origRegID)

	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()

	for _, pubDef := range cc.pubs {
		if pubDef.regID == regID {
			pubDef.status = RegistrationStatus.RegisteredMediaDriver
			pubDef.sessionID = sessionID
			pubDef.posLimitCounterID = posLimitCounterID
			pubDef.channelStatusIndicatorID = channelStatusIndicatorID
			pubDef.buffers = logbuffer.Wrap(logFileName)
			pubDef.buffers.IncRef()
			pubDef.origRegID = origRegID

			logger.Debugf("Updated publication: %v", pubDef)

			if cc.onNewPublicationHandler != nil {
				cc.onNewPublicationHandler(pubDef.channel, streamID, sessionID, regID)
			}
		}
	}
}

func (cc *ClientConductor) OnAvailableCounter(registrationId int64, counterId int32) {
	logger.Debugf("OnAvailableCounter: registrationId=%d, counterId=%d",
		registrationId, counterId)

	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()

	counterDef, ok := cc.counters[registrationId]
	if ok && counterDef.status == RegistrationStatus.AwaitingMediaDriver {
		counterDef.counterId = counterId
		counterDef.status = RegistrationStatus.RegisteredMediaDriver
	}
	for _, handler := range cc.availableCounterHandlers {
		handler.handler.Handle(cc.counterReader, registrationId, counterId)
	}
}

func (cc *ClientConductor) OnUnavailableCounter(registrationId int64, counterId int32) {
	logger.Debugf("OnUnavailableCounter: registrationId=%d, counterId=%d",
		registrationId, counterId)

	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()

	for _, handler := range cc.unavailableCounterHandlers {
		handler.handler.Handle(cc.counterReader, registrationId, counterId)
	}
}

func (cc *ClientConductor) OnClientTimeout(clientID int64) {
	logger.Debugf("OnClientTimeout: clientID=%d", clientID)

	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()

	if clientID == cc.driverProxy.ClientID() {
		errStr := fmt.Sprintf("OnClientTimeout for ClientID:%d", clientID)
		cc.onError(errors.New(errStr))
		cc.running.Set(false)
	}
}

func (cc *ClientConductor) OnSubscriptionReady(correlationID int64, channelStatusIndicatorID int32) {
	logger.Debugf("OnSubscriptionReady: correlationID=%d, channelStatusIndicatorID=%d",
		correlationID, channelStatusIndicatorID)

	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()

	for _, sub := range cc.subs {

		if sub.regID == correlationID {
			sub.status = RegistrationStatus.RegisteredMediaDriver
			sub.subscription = NewSubscription(
				cc, sub.channel, correlationID, sub.streamID, channelStatusIndicatorID,
				sub.availableImageHandler, sub.unavailableImageHandler)

			if cc.onNewSubscriptionHandler != nil {
				cc.onNewSubscriptionHandler(sub.channel, sub.streamID, correlationID)
			}
		}
	}

}

func DefaultImageFactory(sessionID int32, corrID int64, logFilename string, subRegId int64, sourceIdentity string,
	counterValuesBuffer *atomic.Buffer, subscriberPositionID int32) Image {
	image := NewImage(sessionID, corrID, logbuffer.Wrap(logFilename))
	image.subscriptionRegistrationID = subRegId
	image.sourceIdentity = sourceIdentity
	image.subscriberPosition = NewPosition(counterValuesBuffer, subscriberPositionID)
	logger.Debugf("OnAvailableImage: new image position: %v -> %d",
		image.subscriberPosition, image.subscriberPosition.get())
	return image
}

//go:norace
func (cc *ClientConductor) OnAvailableImage(streamID int32, sessionID int32, logFilename string, sourceIdentity string,
	subscriberPositionID int32, subsRegID int64, corrID int64) {
	logger.Debugf("OnAvailableImage: streamId=%d, sessionId=%d, logFilename=%s, sourceIdentity=%s, subsRegID=%d, corrID=%d",
		streamID, sessionID, logFilename, sourceIdentity, subsRegID, corrID)

	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()

	for _, sub := range cc.subs {

		// if sub.streamID == streamID && sub.subscription != nil {
		if sub.subscription != nil {
			// logger.Debugf("OnAvailableImage: sub.regID=%d subsRegID=%d corrID=%d %#v", sub.regID, subsRegID, corrID, sub)
			if sub.regID == subsRegID {
				image := cc.imageFactory(sessionID, corrID, logFilename, sub.regID, sourceIdentity,
					cc.counterValuesBuffer, subscriberPositionID)

				sub.subscription.addImage(image)

				if handler := sub.subscription.AvailableImageHandler(); handler != nil {
					handler(image)
				}
			}
		}
	}
}

func (cc *ClientConductor) OnUnavailableImage(corrID int64, subscriptionRegistrationID int64) {
	logger.Debugf("OnUnavailableImage: corrID=%d subscriptionRegistrationID=%d", corrID, subscriptionRegistrationID)

	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()

	for _, sub := range cc.subs {
		if sub.regID == subscriptionRegistrationID {
			if sub.subscription != nil {
				image := sub.subscription.removeImage(corrID)
				if handler := sub.subscription.UnavailableImageHandler(); handler != nil {
					handler(image)
				}
				cc.lingeringResources <- lingerResourse{time.Now().UnixNano(), image}
				runtime.KeepAlive(image)
			}
		}
	}
}

func (cc *ClientConductor) OnOperationSuccess(corrID int64) {
	logger.Debugf("OnOperationSuccess: correlationId=%d", corrID)

	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()

}

func (cc *ClientConductor) OnChannelEndpointError(corrID int64, errorMessage string) {
	logger.Debugf("OnChannelEndpointError: correlationID=%d, errorMessage=%s", corrID, errorMessage)

	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()

	statusIndicatorId := int32(corrID)

	for _, pubDef := range cc.pubs {
		if pubDef.publication != nil && pubDef.publication.ChannelStatusID() == statusIndicatorId {
			cc.onError(fmt.Errorf(errorMessage))
		}
	}

	for _, subDef := range cc.subs {
		if subDef.subscription != nil && subDef.subscription.ChannelStatusId() == statusIndicatorId {
			cc.onError(fmt.Errorf(errorMessage))
		}
	}
}

func (cc *ClientConductor) OnErrorResponse(corrID int64, errorCode int32, errorMessage string) {
	logger.Debugf("OnErrorResponse: correlationID=%d, errorCode=%d, errorMessage=%s", corrID, errorCode, errorMessage)

	cc.adminLock.Lock()
	defer cc.adminLock.Unlock()

	if counterDef, ok := cc.counters[corrID]; ok {
		counterDef.status = RegistrationStatus.ErroredMediaDriver
		counterDef.errorCode = errorCode
		counterDef.errorMessage = errorMessage
		return
	}

	for _, pubDef := range cc.pubs {
		if pubDef.regID == corrID {
			pubDef.status = RegistrationStatus.ErroredMediaDriver
			pubDef.errorCode = errorCode
			pubDef.errorMessage = errorMessage
			return
		}
	}

	for _, subDef := range cc.subs {
		if subDef.regID == corrID {
			subDef.status = RegistrationStatus.ErroredMediaDriver
			subDef.errorCode = errorCode
			subDef.errorMessage = errorMessage
		}
	}
}

func (cc *ClientConductor) onHeartbeatCheckTimeouts() (int, error) {
	var result int

	now := time.Now().UnixNano()

	if now > (cc.timeOfLastDoWork + cc.interServiceTimeoutNs) {
		cc.closeAllResources(now)

		return 0, fmt.Errorf("timeout between service calls over %d ms (%d > %d + %d) (%d)",
			cc.interServiceTimeoutNs/time.Millisecond.Nanoseconds(),
			now/time.Millisecond.Nanoseconds(),
			cc.timeOfLastDoWork,
			cc.interServiceTimeoutNs/time.Millisecond.Nanoseconds(),
			(now-cc.timeOfLastDoWork)/time.Millisecond.Nanoseconds())
	}

	cc.timeOfLastDoWork = now

	if now > (cc.timeOfLastKeepalive + keepaliveTimeoutNS) {
		age := cc.driverProxy.TimeOfLastDriverKeepalive()*time.Millisecond.Nanoseconds() + cc.driverTimeoutNs
		if now > age {
			cc.driverActive.Set(false)
			return 0, fmt.Errorf("MediaDriver keepalive (ms): age=%d > timeout=%d",
				age,
				cc.driverTimeoutNs/time.Millisecond.Nanoseconds(),
			)
		}

		if cc.heartbeatTimestamp != nil {
			registrationID, ctrErr := cc.counterReader.GetKeyPartInt64(cc.heartbeatTimestamp.CounterId, heartheatRegistrationIdOffset)
			if ctrErr == nil && registrationID == cc.driverProxy.ClientID() {
				cc.heartbeatTimestamp.Set(now / time.Millisecond.Nanoseconds())
			} else {
				cc.closeAllResources(now)
				return 0, fmt.Errorf("client heartbeat timestamp not active")
			}
		} else {
			counterId := cc.counterReader.FindCounter(heartbeatTypeId, func(keyBuffer *atomic.Buffer) bool {
				return keyBuffer.GetInt64(heartheatRegistrationIdOffset) == cc.driverProxy.ClientID()
			})
			if counterId != ctr.NullCounterId {
				var ctrErr error
				if cc.heartbeatTimestamp, ctrErr = ctr.NewAtomicCounter(cc.counterReader, counterId); ctrErr != nil {
					logger.Warning("unable to allocate heartbeat counter %d", counterId)
				} else {
					cc.heartbeatTimestamp.Set(now / time.Millisecond.Nanoseconds())
				}
			}
		}

		cc.timeOfLastKeepalive = now
		result = 1
	}

	if now > (cc.timeOfLastCheckManagedResources + resourceTimeoutNS) {
		cc.onCheckManagedResources(now)
		cc.timeOfLastCheckManagedResources = now
		result = 1
	}

	return result, nil
}

func (cc *ClientConductor) onCheckManagedResources(now int64) {
	moreToCheck := true
	for moreToCheck {
		select {
		case r := <-cc.lingeringResources:
			logger.Debugf("Resource to linger: %v", r)
			if cc.resourceLingerTimeoutNs < now-r.lastTime {
				res := r.resource
				logger.Debugf("lingering resource expired(%dms old): %v",
					(now-r.lastTime)/time.Millisecond.Nanoseconds(), res)
				if res != nil {
					err := res.Close()
					if err != nil {
						logger.Warningf("Failed to close lingering resource: %v", err)
						cc.onError(err)
					}
				}
			} else {
				// The assumption is that resources are queued in order
				moreToCheck = false
				// FIXME ..and we're breaking it here, but since there is no peek...
				cc.lingeringResources <- r
			}
		default:
			moreToCheck = false
		}
	}
}

func (cc *ClientConductor) isPublicationConnected(timeOfLastStatusMessage int64) bool {
	return time.Now().UnixNano() <= (timeOfLastStatusMessage*int64(time.Millisecond) + cc.publicationConnectionTimeoutNs)
}

func (cc *ClientConductor) CounterReader() *ctr.Reader {
	return cc.counterReader
}

func (cc *ClientConductor) closeAllResources(now int64) {
	var err error
	if cc.running.CompareAndSet(true, false) {
		for _, pub := range cc.pubs {
			if pub != nil && pub.publication != nil {
				err = pub.publication.Close()
				if err != nil {
					cc.onError(err)
				}
			}
		}
		cc.pubs = nil

		for _, sub := range cc.subs {
			if sub != nil && sub.subscription != nil {
				err = sub.subscription.Close()
				if err != nil {
					cc.onError(err)
				}
			}
		}
		cc.subs = nil

		for _, counterDef := range cc.counters {
			if counterDef != nil && counterDef.counter != nil {
				err = counterDef.counter.Close()
				if err != nil {
					cc.onError(err)
				}
			}
		}
		cc.counters = nil
	}
}

func (cc *ClientConductor) onError(err error) {
	if cc.errorHandler != nil {
		cc.errorHandler(err)
	} else {
		log.Fatal(err)
	}
}
