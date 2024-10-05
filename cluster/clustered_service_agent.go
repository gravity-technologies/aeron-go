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
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/counters"
	"github.com/lirm/aeron-go/aeron/idlestrategy"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/aeron/logging"
	"github.com/lirm/aeron-go/aeron/util"
	"github.com/lirm/aeron-go/archive"
	"github.com/lirm/aeron-go/cluster/codecs"
)

const NullValue = -1
const NullPosition = -1
const markFileUpdateIntervalMs = 1000

const (
	recordingPosCounterTypeId  = 100
	commitPosCounterTypeId     = 203
	recoveryStateCounterTypeId = 204
)

type LifeCycleCallback int8

const (
	LIFECYCLE_CALLBACK_NONE = iota
	LIFECYCLE_CALLBACK_ON_START
	LIFECYCLE_CALLBACK_ON_TERMINATE
	LIFECYCLE_CALLBACK_ON_ROLE_CHANGE
	LIFECYCLE_CALLBACK_DO_BACKGROUND_WORK // noop in aeron-go ver
)

var logger = logging.MustGetLogger("cluster")

type ClusteredServiceAgent struct {
	aeronClient              *aeron.Aeron
	aeronCtx                 *aeron.Context
	opts                     *Options
	consensusModuleProxy     *consensusModuleProxy
	counters                 *counters.Reader
	serviceAdapter           *serviceAdapter
	logAdapter               *boundedLogAdapter
	markFile                 *ClusterMarkFile
	activeLogEvent           *activeLogEvent
	cachedTimeMs             int64
	markFileUpdateDeadlineMs int64
	logPosition              int64
	clusterTime              int64
	timeUnit                 codecs.ClusterTimeUnitEnum
	memberId                 int32
	nextAckId                int64
	terminationPosition      int64
	isServiceActive          bool
	role                     Role
	service                  ClusteredService
	sessions                 map[int64]ClientSession
	commitPosition           *counters.ReadableCounter
	sessionMsgHdrBuffer      *atomic.Buffer
	requestedAckPosition     int64
	activeLifecycleCallback  LifeCycleCallback
	signalChan               chan os.Signal
}

func NewClusteredServiceAgent(
	aeronCtx *aeron.Context,
	options *Options,
	service ClusteredService,
) (*ClusteredServiceAgent, error) {
	if !strings.HasPrefix(options.ArchiveOptions.RequestChannel, "aeron:ipc") {
		return nil, fmt.Errorf("archive request channel must be IPC: %s", options.ArchiveOptions.RequestChannel)
	}
	if !strings.HasPrefix(options.ArchiveOptions.ResponseChannel, "aeron:ipc") {
		return nil, fmt.Errorf("archive response channel must be IPC: %s", options.ArchiveOptions.ResponseChannel)
	}
	if options.ServiceId < 0 || options.ServiceId > 127 {
		return nil, fmt.Errorf("serviceId is outside allowed range (0-127): %d", options.ServiceId)
	}

	logging.SetLevel(options.Loglevel, "cluster")

	aeronClient, err := aeron.Connect(aeronCtx)
	if err != nil {
		return nil, err
	}

	pub, err := aeronClient.AddPublication(options.ControlChannel, options.ConsensusModuleStreamId)
	if err != nil {
		return nil, err
	}
	proxy := newConsensusModuleProxy(options, pub)

	sub, err := aeronClient.AddSubscription(options.ControlChannel, options.ServiceStreamId)
	if err != nil {
		return nil, err
	}
	serviceAdapter := &serviceAdapter{
		marshaller:   codecs.NewSbeGoMarshaller(),
		subscription: sub,
	}
	logAdapter := &boundedLogAdapter{
		marshaller: codecs.NewSbeGoMarshaller(),
		options:    options,
	}

	counterFile, _, _ := counters.MapFile(aeronCtx.CncFileName())
	countersReader := counters.NewReader(
		counterFile.ValuesBuf.Get(),
		counterFile.MetaDataBuf.Get(),
	)

	cmf, err := NewClusterMarkFile(options.ClusterDir + "/cluster-mark-service-" + strconv.Itoa(int(options.ServiceId)) + ".dat")
	if err != nil {
		return nil, err
	}

	agent := &ClusteredServiceAgent{
		aeronClient:          aeronClient,
		opts:                 options,
		serviceAdapter:       serviceAdapter,
		logAdapter:           logAdapter,
		aeronCtx:             aeronCtx,
		consensusModuleProxy: proxy,
		counters:             countersReader,
		markFile:             cmf,
		role:                 Follower,
		service:              service,
		logPosition:          NullPosition,
		terminationPosition:  NullPosition,
		sessions:             map[int64]ClientSession{},
		sessionMsgHdrBuffer:  codecs.MakeClusterMessageBuffer(SessionMessageHeaderTemplateId, SessionMessageHdrBlockLength),
		requestedAckPosition: NullPosition,

		activeLifecycleCallback: LIFECYCLE_CALLBACK_NONE,
	}
	serviceAdapter.agent = agent
	logAdapter.agent = agent

	cmf.flyweight.ArchiveStreamId.Set(options.ArchiveOptions.RequestStream)
	cmf.flyweight.ServiceStreamId.Set(options.ServiceStreamId)
	cmf.flyweight.ConsensusModuleStreamId.Set(options.ConsensusModuleStreamId)
	cmf.flyweight.IngressStreamId.Set(-1)
	cmf.flyweight.MemberId.Set(-1)
	cmf.flyweight.ServiceId.Set(options.ServiceId)
	cmf.flyweight.ClusterId.Set(options.ClusterId)

	cmf.UpdateActivityTimestamp(time.Now().UnixMilli())
	cmf.SignalReady()

	return agent, nil
}

func (agent *ClusteredServiceAgent) StartAndRunWithGracefulShutdown() error {
	agent.signalChan = make(chan os.Signal, 1)
	signal.Notify(agent.signalChan, syscall.SIGINT, syscall.SIGTERM)
	defer agent.OnClose()

	go func() {
		<-agent.signalChan
		agent.OnClose()
	}()

	return agent.StartAndRun()
}

func (agent *ClusteredServiceAgent) StartAndRun() error {
	if err := agent.OnStart(); err != nil {
		return err
	}
	for agent.isServiceActive {
		agent.opts.IdleStrategy.Idle(agent.DoWork())
	}
	return nil
}

func (agent *ClusteredServiceAgent) OnStart() error {
	if err := agent.awaitCommitPositionCounter(); err != nil {
		return err
	}
	if err := agent.recoverState(); err != nil {
		return err
	}
	agent.isServiceActive = true
	return nil
}

func (agent *ClusteredServiceAgent) awaitCommitPositionCounter() error {
	for {
		id := agent.counters.FindCounter(commitPosCounterTypeId, func(keyBuffer *atomic.Buffer) bool {
			return keyBuffer.GetInt32(0) == agent.opts.ClusterId
		})
		if id != counters.NullCounterId {
			commitPos, err := counters.NewReadableCounter(agent.counters, id)
			logger.Debugf("found commit position counter - id=%d value=%d", id, commitPos.Get())
			agent.commitPosition = commitPos
			return err
		}
		agent.Idle(0)
	}
}

func (agent *ClusteredServiceAgent) recoverState() error {
	recoveryCounterId, leadershipTermId := agent.awaitRecoveryCounter()
	logger.Debugf("found recovery counter - id=%d leadershipTermId=%d logPos=%d clusterTime=%d",
		recoveryCounterId, leadershipTermId, agent.logPosition, agent.clusterTime)
	agent.sessionMsgHdrBuffer.PutInt64(SBEHeaderLength, leadershipTermId)

	agent.activeLifecycleCallback = LIFECYCLE_CALLBACK_ON_START
	if leadershipTermId != NullValue {
		snapshotRecId, err := getSnapshotRecordingID(agent.counters, recoveryCounterId, agent.opts.ServiceId)
		if err != nil {
			agent.activeLifecycleCallback = LIFECYCLE_CALLBACK_NONE
			return err
		}
		if err := agent.loadSnapshot(snapshotRecId); err != nil {
			agent.activeLifecycleCallback = LIFECYCLE_CALLBACK_NONE
			return err
		}
	} else {
		agent.service.OnStart(agent, nil)
	}
	agent.activeLifecycleCallback = LIFECYCLE_CALLBACK_NONE

	ackId := agent.getAndIncrementNextAckId()
	logger.Debugf("ack :: recoveryState :: start :: ackId=%d, clusterTime=%d, clientId=%d, serviceId=%d", ackId, agent.clusterTime, agent.aeronClient.ClientID(), agent.opts.ServiceId)
	for !agent.consensusModuleProxy.ack(
		agent.logPosition,
		agent.clusterTime,
		ackId,
		agent.aeronClient.ClientID(),
		agent.opts.ServiceId,
	) {
		agent.Idle(0)
	}
	logger.Debugf("ack :: recoveryState :: end :: ackId=%d, clusterTime=%d, clientId=%d, serviceId=%d", ackId, agent.clusterTime, agent.aeronClient.ClientID(), agent.opts.ServiceId)
	return nil
}

func getSnapshotRecordingID(counters *counters.Reader, recoveryCounterId, serviceId int32) (int64, error) {
	serviceCount, err := counters.GetKeyPartInt32(recoveryCounterId, 28)
	if err != nil {
		return 0, err
	}
	if serviceId < 0 || serviceId >= serviceCount {
		return 0, fmt.Errorf("invalid service id %d for count of: %d", serviceId, serviceCount)
	}
	return counters.GetKeyPartInt64(recoveryCounterId, 32+(serviceId*util.SizeOfInt64))
}

func (agent *ClusteredServiceAgent) awaitRecoveryCounter() (int32, int64) {
	for {
		var leadershipTermId int64
		id := agent.counters.FindCounter(recoveryStateCounterTypeId, func(keyBuffer *atomic.Buffer) bool {
			if keyBuffer.GetInt32(24) == agent.opts.ClusterId {
				leadershipTermId = keyBuffer.GetInt64(0)
				agent.logPosition = keyBuffer.GetInt64(8)
				agent.clusterTime = keyBuffer.GetInt64(16)
				return true
			}
			return false
		})
		if id != counters.NullCounterId {
			return id, leadershipTermId
		}
		agent.Idle(0)
	}
}

func (agent *ClusteredServiceAgent) loadSnapshot(recordingId int64) error {
	arch, err := archive.NewArchive(agent.opts.ArchiveOptions, agent.aeronCtx)
	if err != nil {
		return err
	}
	defer closeArchive(arch)

	channel := agent.opts.ReplayChannel
	streamId := agent.opts.ReplayStreamId
	replaySessionId, err := arch.StartReplay(recordingId, 0, NullValue, channel, streamId)
	if err != nil {
		return err
	}
	subChannel, err := archive.AddSessionIdToChannel(channel, archive.ReplaySessionIdToSessionId(replaySessionId))
	if err != nil {
		return err
	}

	logger.Debugf("replaying snapshot - recId=%d sessionId=%d streamId=%d", recordingId, replaySessionId, streamId)
	subscription, err := arch.AddSubscription(subChannel, streamId)
	if err != nil {
		return err
	}
	defer closeSubscription(subscription)

	img := agent.awaitImage(int32(replaySessionId), subscription)
	if err = agent.loadState(img, arch); err != nil {
		return err
	}
	agent.service.OnStart(agent, img)
	return nil
}

func (agent *ClusteredServiceAgent) loadState(image aeron.Image, archive *archive.Archive) (err error) {
	snapshotLoader := newSnapshotLoader(agent, image)
	for !snapshotLoader.isDone {
		fragments := snapshotLoader.poll()
		if fragments == 0 {
			if _, err = archive.PollForErrorResponse(); err != nil {
				return err
			}
			if image.IsClosed() {
				return fmt.Errorf("cluster exception - snapshot ended unexpectedly: %v", image)
			}
		}
		agent.opts.IdleStrategy.Idle(fragments)
	}

	if snapshotLoader.err != nil {
		return snapshotLoader.err
	}

	if util.SemanticVersionMajor(uint32(agent.opts.AppVersion)) != util.SemanticVersionMajor(uint32(snapshotLoader.appVersion)) {
		panic(fmt.Errorf("incompatible app version: %v snapshot=%v",
			util.SemanticVersionToString(uint32(agent.opts.AppVersion)),
			util.SemanticVersionToString(uint32(snapshotLoader.appVersion))))
	}
	agent.timeUnit = snapshotLoader.timeUnit
	return
}

func (agent *ClusteredServiceAgent) addSessionFromSnapshot(session *containerClientSession) {
	agent.sessions[session.id] = session
}

func (agent *ClusteredServiceAgent) checkForClockTick() bool {
	if agent.aeronClient.IsClosed() {
		logger.Error("agent termination exception - unexpected Aeron close")
		return false
	}
	nowMs := time.Now().UnixMilli()
	if agent.cachedTimeMs != nowMs {
		agent.cachedTimeMs = nowMs

		if agent.aeronClient.IsClosed() {
			logger.Error("agent termination exception - unexpected Aeron close")
			return false
		}

		if agent.commitPosition != nil && agent.commitPosition.IsClosed() {
			logger.Error("cluster termination exception - commit-pos counter unexpectedly closed, terminating")
			return false
		}

		if nowMs > agent.markFileUpdateDeadlineMs {
			agent.markFileUpdateDeadlineMs = nowMs + markFileUpdateIntervalMs
			agent.markFile.UpdateActivityTimestamp(nowMs)
		}
		return true
	}
	return false
}

func (agent *ClusteredServiceAgent) pollServiceAdapter() {
	agent.serviceAdapter.poll()

	if agent.activeLogEvent != nil && agent.logAdapter.image == nil {
		event := agent.activeLogEvent
		agent.activeLogEvent = nil
		agent.joinActiveLog(event)
	}

	if agent.terminationPosition != NullPosition && agent.logPosition >= agent.terminationPosition {
		if agent.logPosition > agent.terminationPosition {
			logger.Errorf("service terminate: logPos=%d > terminationPos=%d", agent.logPosition, agent.terminationPosition)
		}
		agent.terminate()
	}

	if agent.requestedAckPosition != NullPosition && agent.logPosition >= agent.requestedAckPosition {
		if agent.logPosition > agent.requestedAckPosition {
			logger.Errorf("invalid ack request: logPos=%d > requestedAckPos=%d", agent.logPosition, agent.requestedAckPosition)
		}
		ackId := agent.getAndIncrementNextAckId()
		logger.Debugf("ack :: pollServiceAdapter :: start :: ackId=%d, clusterTime=%d, clientId=%d, serviceId=%d", ackId, agent.clusterTime, agent.aeronClient.ClientID(), agent.opts.ServiceId)
		for !agent.consensusModuleProxy.ack(agent.logPosition, agent.clusterTime, ackId, NullValue, agent.opts.ServiceId) {
			agent.Idle(0)
		}
		logger.Debugf("ack :: pollServiceAdapter :: end :: ackId=%d, clusterTime=%d, clientId=%d, serviceId=%d", ackId, agent.clusterTime, agent.aeronClient.ClientID(), agent.opts.ServiceId)
		agent.requestedAckPosition = NullPosition
	}
}

// We need to call this on end of each StartAndRun() like how java does it with finally
// Either we defer agent.OnClose() or trap signal and execute it
// https://github.com/real-logic/aeron/blob/master/aeron-samples/src/main/java/io/aeron/samples/stress/StressMdcClient.java#L292
func (agent *ClusteredServiceAgent) OnClose() {
	if agent.isServiceActive {
		agent.isServiceActive = false
		agent.service.OnTerminate(agent)
	}
	if err := agent.logAdapter.Close(); err != nil {
		logger.Errorf("error closing log image: %v", err)
	}
	if !agent.aeronClient.IsClosed() {
		if err := agent.serviceAdapter.subscription.Close(); err != nil {
			logger.Errorf("failed to close service adapter subscription: %v", err)
		}
		if err := agent.consensusModuleProxy.publication.Close(); err != nil {
			logger.Errorf("failed to close consensusModuleProxy.publication: %v", err)
		}
		agent.disconnectEgress()
	}
	agent.markFile.UpdateActivityTimestamp(NullValue)
	if err := agent.markFile.file.Close(); err != nil {
		logger.Errorf("failed to close markFile: %v", err)
	}
	if !agent.aeronClient.IsClosed() {
		if err := agent.aeronClient.Close(); err != nil {
			logger.Errorf("failed to close aeronClient: %v", err)
		}
	}
}

func (agent *ClusteredServiceAgent) terminate() {
	agent.isServiceActive = false
	agent.activeLifecycleCallback = LIFECYCLE_CALLBACK_ON_TERMINATE
	agent.service.OnTerminate(agent)
	agent.activeLifecycleCallback = LIFECYCLE_CALLBACK_NONE
	attempts := 5
	ackId := agent.getAndIncrementNextAckId()
	logger.Debugf("ack :: terminate :: start :: ackId=%d, clusterTime=%d, clientId=%d, serviceId=%d", ackId, agent.clusterTime, agent.aeronClient.ClientID(), agent.opts.ServiceId)
	for attempts > 0 && !agent.consensusModuleProxy.ack(
		agent.logPosition,
		agent.clusterTime,
		ackId,
		NullValue,
		agent.opts.ServiceId,
	) {
		attempts--
		agent.Idle(0)
	}
	logger.Debugf("ack :: terminate :: end :: ackId=%d, clusterTime=%d, clientId=%d, serviceId=%d", ackId, agent.clusterTime, agent.aeronClient.ClientID(), agent.opts.ServiceId)
	agent.terminationPosition = NullPosition
}

func (agent *ClusteredServiceAgent) DoWork() int {
	work := 0

	if agent.checkForClockTick() {
		agent.pollServiceAdapter()
	}

	if agent.logAdapter.image != nil {
		polled := agent.logAdapter.poll(agent.commitPosition.Get())
		work += polled
		if polled == 0 && agent.logAdapter.isDone() {
			agent.closeLog()
		}
	}

	return work
}

func (agent *ClusteredServiceAgent) onJoinLog(
	logPosition int64,
	maxLogPosition int64,
	memberId int32,
	logSessionId int32,
	logStreamId int32,
	isStartup bool,
	role Role,
	logChannel string,
) {
	logger.Debugf("onJoinLog - logPos=%d isStartup=%v role=%v logChannel=%s", logPosition, isStartup, role, logChannel)
	agent.logAdapter.maxLogPosition = logPosition
	event := &activeLogEvent{
		logPosition:    logPosition,
		maxLogPosition: maxLogPosition,
		memberId:       memberId,
		logSessionId:   logSessionId,
		logStreamId:    logStreamId,
		isStartup:      isStartup,
		role:           role,
		logChannel:     logChannel,
	}
	agent.activeLogEvent = event
}

type activeLogEvent struct {
	logPosition    int64
	maxLogPosition int64
	memberId       int32
	logSessionId   int32
	logStreamId    int32
	isStartup      bool
	role           Role
	logChannel     string
}

func (agent *ClusteredServiceAgent) joinActiveLog(event *activeLogEvent) error {
	if event.role != Leader {
		agent.disconnectEgress()
	}

	logSub, err := agent.aeronClient.AddSubscription(event.logChannel, event.logStreamId)
	if err != nil {
		return err
	}
	img := agent.awaitImage(event.logSessionId, logSub)
	if img.Position() != agent.logPosition {
		return fmt.Errorf("joinActiveLog - image.position=%v expected=%v", img.Position(), agent.logPosition)
	}
	if event.logPosition != agent.logPosition {
		return fmt.Errorf("joinActiveLog - event.logPos=%v expected=%v", event.logPosition, agent.logPosition)
	}
	agent.logAdapter.image = img
	agent.logAdapter.maxLogPosition = event.maxLogPosition

	ackId := agent.getAndIncrementNextAckId()
	logger.Debugf("ack :: joinActiveLog :: start :: ackId=%d, clusterTime=%d, clientId=%d, serviceId=%d", ackId, agent.clusterTime, agent.aeronClient.ClientID(), agent.opts.ServiceId)
	for !agent.consensusModuleProxy.ack(
		event.logPosition,
		agent.clusterTime,
		ackId,
		NullValue,
		agent.opts.ServiceId,
	) {
		agent.Idle(0)
	}
	logger.Debugf("ack :: joinActiveLog :: end :: ackId=%d, clusterTime=%d, clientId=%d, serviceId=%d", ackId, agent.clusterTime, agent.aeronClient.ClientID(), agent.opts.ServiceId)
	agent.memberId = event.memberId
	agent.markFile.flyweight.MemberId.Set(agent.memberId)

	if Leader == event.role {
		for _, session := range agent.sessions {
			// TODO: issue-6 for responder conf check - always true for now
			if !event.isStartup {
				if err = session.Connect(agent.aeronClient); err != nil {
					return err
				}
			}
			session.(*containerClientSession).ResetClosing()
		}
	}

	agent.setRole(event.role)
	return nil
}

func (agent *ClusteredServiceAgent) closeLog() {
	imageLogPos := agent.logAdapter.image.Position()
	if imageLogPos > agent.logPosition {
		agent.logPosition = imageLogPos
	}
	if err := agent.logAdapter.Close(); err != nil {
		logger.Errorf("error closing log image: %v", err)
	}
	agent.disconnectEgress()
	agent.setRole(Follower)
}

func (agent *ClusteredServiceAgent) disconnectEgress() {
	for _, session := range agent.sessions {
		session.Disconnect()
	}
}

func (agent *ClusteredServiceAgent) setRole(newRole Role) {
	if newRole != agent.role {
		agent.activeLifecycleCallback = LIFECYCLE_CALLBACK_ON_ROLE_CHANGE
		agent.role = newRole
		agent.service.OnRoleChange(newRole)
		agent.activeLifecycleCallback = LIFECYCLE_CALLBACK_NONE
	}
}

func (agent *ClusteredServiceAgent) awaitImage(
	sessionId int32,
	subscription *aeron.Subscription,
) aeron.Image {
	for {
		if img := subscription.ImageBySessionID(sessionId); img != nil {
			return img
		}
		agent.opts.IdleStrategy.Idle(0)
	}
}

func (agent *ClusteredServiceAgent) onSessionOpen(
	leadershipTermId int64,
	logPosition int64,
	clusterSessionId int64,
	timestamp int64,
	responseStreamId int32,
	responseChannel string,
	encodedPrincipal []byte,
) error {
	agent.logPosition = logPosition
	agent.clusterTime = timestamp
	if _, ok := agent.sessions[clusterSessionId]; ok {
		return fmt.Errorf("clashing open session - id=%d leaderTermId=%d logPos=%d",
			clusterSessionId, leadershipTermId, logPosition)
	} else {
		session, err := newContainerClientSession(
			clusterSessionId,
			responseStreamId,
			responseChannel,
			encodedPrincipal,
			agent,
		)
		if err != nil {
			return err
		}
		// TODO: issue-6 for responder conf check - always true for now
		if Leader == agent.role {
			if err = session.Connect(agent.aeronClient); err != nil {
				return err
			}
		}

		agent.sessions[session.id] = session
		agent.service.OnSessionOpen(session, timestamp)
	}
	return nil
}

func (agent *ClusteredServiceAgent) onSessionClose(
	leadershipTermId int64,
	logPosition int64,
	clusterSessionId int64,
	timestamp int64,
	closeReason codecs.CloseReasonEnum,
) {
	agent.logPosition = logPosition
	agent.clusterTime = timestamp

	if session, ok := agent.sessions[clusterSessionId]; ok {
		delete(agent.sessions, clusterSessionId)
		session.Disconnect()
		agent.service.OnSessionClose(session, timestamp, closeReason)
	} else {
		logger.Errorf("onSessionClose: unknown session - id=%d leaderTermId=%d logPos=%d reason=%v",
			clusterSessionId, leadershipTermId, logPosition, closeReason)
	}
}

func (agent *ClusteredServiceAgent) onSessionMessage(
	logPosition int64,
	clusterSessionId int64,
	timestamp int64,
	buffer *atomic.Buffer,
	offset int32,
	length int32,
	header *logbuffer.Header,
) {
	agent.logPosition = logPosition
	agent.clusterTime = timestamp
	clientSession := agent.sessions[clusterSessionId]
	agent.service.OnSessionMessage(clientSession, timestamp, buffer, offset, length, header)
}

func (agent *ClusteredServiceAgent) onNewLeadershipTermEvent(
	leadershipTermId int64,
	logPosition int64,
	timestamp int64,
	termBaseLogPosition int64,
	leaderMemberId int32,
	logSessionId int32,
	timeUnit codecs.ClusterTimeUnitEnum,
	appVersion int32,
) {
	if util.SemanticVersionMajor(uint32(agent.opts.AppVersion)) != util.SemanticVersionMajor(uint32(appVersion)) {
		panic(fmt.Errorf("incompatible app version: %v log=%v",
			util.SemanticVersionToString(uint32(agent.opts.AppVersion)),
			util.SemanticVersionToString(uint32(appVersion))))
	}
	agent.sessionMsgHdrBuffer.PutInt64(SBEHeaderLength, leadershipTermId)
	agent.logPosition = logPosition
	agent.clusterTime = timestamp
	agent.timeUnit = timeUnit

	agent.service.OnNewLeadershipTermEvent(
		leadershipTermId,
		logPosition,
		timestamp,
		termBaseLogPosition,
		leaderMemberId,
		logSessionId,
		timeUnit,
		appVersion)
}

func (agent *ClusteredServiceAgent) onServiceAction(
	leadershipTermId int64,
	logPos int64,
	timestamp int64,
	action codecs.ClusterActionEnum,
) {
	agent.logPosition = logPos
	agent.clusterTime = timestamp
	agent.executeAction(action, logPos, leadershipTermId)
}

func (agent *ClusteredServiceAgent) executeAction(
	action codecs.ClusterActionEnum,
	logPosition,
	leadershipTermId int64,
) {
	if action == codecs.ClusterAction.SNAPSHOT {
		recordingId, err := agent.takeSnapshot(logPosition, leadershipTermId)
		if err != nil {
			logger.Errorf("take snapshot failed: ", err)
		}

		ackId := agent.getAndIncrementNextAckId()
		logger.Debugf("ack :: executeAction :: start :: ackId=%d, clusterTime=%d, recordingId=%d, serviceId=%d", ackId, agent.clusterTime, recordingId, agent.opts.ServiceId)
		for !agent.consensusModuleProxy.ack(logPosition, agent.clusterTime, ackId, recordingId, agent.opts.ServiceId) {
			agent.Idle(0)
		}
		logger.Debugf("ack :: executeAction :: end :: ackId=%d, clusterTime=%d, recordingId=%d, serviceId=%d", ackId, agent.clusterTime, recordingId, agent.opts.ServiceId)
	}
}

func (agent *ClusteredServiceAgent) onTimerEvent(
	logPosition int64,
	correlationId int64,
	timestamp int64,
) {
	agent.logPosition = logPosition
	agent.clusterTime = timestamp
	agent.service.OnTimerEvent(correlationId, timestamp)
}

func (agent *ClusteredServiceAgent) onMembershipChange(
	logPos int64,
	timestamp int64,
	changeType codecs.ChangeTypeEnum,
	memberId int32,
) {
	agent.logPosition = logPos
	agent.clusterTime = timestamp
	if memberId == agent.memberId && changeType == codecs.ChangeType.QUIT {
		agent.terminate()
	}
}

func (agent *ClusteredServiceAgent) takeSnapshot(logPos int64, leadershipTermId int64) (int64, error) {
	arch, err := archive.NewArchive(agent.opts.ArchiveOptions, agent.aeronCtx)
	if err != nil {
		return NullValue, err
	}
	defer closeArchive(arch)

	pub, err := arch.AddRecordedPublication(agent.opts.SnapshotChannel, agent.opts.SnapshotStreamId)
	if err != nil {
		return NullValue, err
	}
	defer closePublication(pub)

	recordingId, counterId, err := agent.awaitRecordingCounterAndId(pub.SessionID())
	if err != nil {
		return NullValue, err
	}

	logger.Debugf("takeSnapshot - got recordingId: %d", recordingId)
	snapshotTaker := newSnapshotTaker(agent.opts, pub)
	if err := snapshotTaker.markBegin(logPos, leadershipTermId, agent.timeUnit, agent.opts.AppVersion); err != nil {
		return NullValue, err
	}
	for _, session := range agent.sessions {
		if err := snapshotTaker.snapshotSession(session); err != nil {
			return NullValue, err
		}
	}
	if err := snapshotTaker.markEnd(logPos, leadershipTermId, agent.timeUnit, agent.opts.AppVersion); err != nil {
		return NullValue, err
	}
	agent.checkForClockTick()
	if _, err := arch.PollForErrorResponse(); err != nil {
		var archiveErr *archive.ArchiveError
		if errors.As(err, &archiveErr) {
			logger.Error("archive error: space storage is at minimum threshold or exhausted")
			if archiveErr.ErrorCode == archive.ExceptionSpaceStorage {
				agent.terminate()
			}
		}
		return NullValue, err
	}
	agent.service.OnTakeSnapshot(pub)
	if err = agent.awaitRecordingComplete(recordingId, pub.Position(), counterId, arch); err != nil {
		return NullValue, err
	}

	return recordingId, nil
}

func (agent *ClusteredServiceAgent) awaitRecordingCounterAndId(sessionId int32) (int64, int32, error) {
	for {
		recId := int64(NullValue)
		counterId := agent.counters.FindCounter(recordingPosCounterTypeId, func(keyBuffer *atomic.Buffer) bool {
			if keyBuffer.GetInt32(8) == sessionId {
				recId = keyBuffer.GetInt64(0)
				return true
			}
			return false
		})
		if counterId != counters.NullCounterId {
			return recId, counterId, nil
		}
		agent.Idle(0)
	}
}

func (agent *ClusteredServiceAgent) awaitRecordingComplete(recordingId int64, position int64, counterId int32, arch *archive.Archive) error {
	for agent.counters.GetCounterValue(counterId) < position {
		agent.Idle(0)
		if _, err := arch.PollForErrorResponse(); err != nil {
			return err
		}

		if !archive.IsRecordingActive(agent.counters, counterId, recordingId) {
			return fmt.Errorf("cluster exception - recording stopped unexpectedly: %d", recordingId)
		}
	}
	return nil
}

func (agent *ClusteredServiceAgent) onServiceTerminationPosition(position int64) {
	agent.terminationPosition = position
}

func (agent *ClusteredServiceAgent) getAndIncrementNextAckId() int64 {
	ackId := agent.nextAckId
	agent.nextAckId++
	return ackId
}

func (agent *ClusteredServiceAgent) offerToSession(
	clusterSessionId int64,
	publication *aeron.Publication,
	buffer *atomic.Buffer,
	offset int32,
	length int32,
	reservedValueSupplier term.ReservedValueSupplier,
) int64 {
	if err := agent.checkForValidInvocation(); err != nil {
		logger.Errorf("offerToSession: error in checkForValidInvocation clusterSessionId=%d publication.sessionID=%d offset=%d length=%d", clusterSessionId, publication.SessionID(), offset, length)
		return NullValue
	}

	if agent.role != Leader {
		return ClientSessionMockedOffer
	}

	if publication == nil || !publication.IsConnected() {
		return aeron.NotConnected
	}

	hdrBuf := agent.sessionMsgHdrBuffer
	hdrBuf.PutInt64(SBEHeaderLength+8, clusterSessionId)
	hdrBuf.PutInt64(SBEHeaderLength+16, agent.clusterTime)
	return publication.Offer2(hdrBuf, 0, hdrBuf.Capacity(), buffer, offset, length, reservedValueSupplier)
}

func (agent *ClusteredServiceAgent) getClientSession(id int64) (ClientSession, bool) {
	session, ok := agent.sessions[id]
	return session, ok
}

func (agent *ClusteredServiceAgent) closeClientSession(id int64) bool {
	if err := agent.checkForValidInvocation(); err != nil {
		logger.Errorf("closeClientSession: error in checkForValidInvocation id=%d", id)
		return false
	}
	session, ok := agent.sessions[id]
	if !ok {
		logger.Errorf("closeClientSession: unknown session id=%d", id)
		return false
	}

	clientSession, ok := session.(*containerClientSession)
	if !ok {
		logger.Errorf("closeClientSession: not a valid containerClientSession")
		return false
	}
	if clientSession.isClosing {
		return true
	}

	attempts := 3
	for attempts > 0 {
		if agent.consensusModuleProxy.closeSessionRequest(id) {
			clientSession.MarkClosing()
			return true
		}
		agent.Idle(0)
		attempts--
	}

	return false
}

func closeArchive(arch *archive.Archive) {
	err := arch.Close()
	if err != nil {
		logger.Errorf("error closing archive connection: %v", err)
	}
}

func closeSubscription(sub *aeron.Subscription) {
	err := sub.Close()
	if err != nil {
		logger.Errorf("error closing subscription, streamId=%d channel=%s: %v", sub.StreamID(), sub.Channel(), err)
	}
}

func closePublication(pub *aeron.Publication) {
	err := pub.Close()
	if err != nil {
		logger.Errorf("error closing publication, streamId=%d channel=%s: %v", pub.StreamID(), pub.Channel(), err)
	}
}

func (agent *ClusteredServiceAgent) Idle(workCount int) {
	agent.opts.IdleStrategy.Idle(workCount)
	if workCount <= 0 {
		agent.checkForClockTick()
	}
}

// BEGIN CLUSTER IMPLEMENTATION

func (agent *ClusteredServiceAgent) LogPosition() int64 {
	return agent.logPosition
}

func (agent *ClusteredServiceAgent) MemberId() int32 {
	return agent.memberId
}

func (agent *ClusteredServiceAgent) Role() Role {
	return agent.role
}

func (agent *ClusteredServiceAgent) Time() int64 {
	return agent.clusterTime
}

func (agent *ClusteredServiceAgent) TimeUnit() codecs.ClusterTimeUnitEnum {
	return agent.timeUnit
}

func (agent *ClusteredServiceAgent) IdleStrategy() idlestrategy.Idler {
	return agent
}

func (agent *ClusteredServiceAgent) ScheduleTimer(correlationId int64, deadline int64) bool {
	if err := agent.checkForValidInvocation(); err != nil {
		logger.Errorf("ScheduleTimer: error in checkForValidInvocation correlationId=%d", correlationId)
		return false
	}
	return agent.consensusModuleProxy.scheduleTimer(correlationId, deadline)
}

func (agent *ClusteredServiceAgent) CancelTimer(correlationId int64) bool {
	if err := agent.checkForValidInvocation(); err != nil {
		logger.Errorf("CancelTimer: error in checkForValidInvocation correlationId=%d", correlationId)
		return false
	}
	return agent.consensusModuleProxy.cancelTimer(correlationId)
}

func (agent *ClusteredServiceAgent) Offer(buffer *atomic.Buffer, offset, length int32) int64 {
	if err := agent.checkForValidInvocation(); err != nil {
		logger.Errorf("Offer: error in checkForValidInvocation offset=%d length=%d", offset, length)
		return NullValue
	}
	hdrBuf := agent.sessionMsgHdrBuffer
	hdrBuf.PutInt64(SBEHeaderLength+8, int64(agent.opts.ServiceId))
	hdrBuf.PutInt64(SBEHeaderLength+16, agent.clusterTime)
	return agent.consensusModuleProxy.Offer2(hdrBuf, 0, hdrBuf.Capacity(), buffer, offset, length)
}

func (agent *ClusteredServiceAgent) OnRequestServiceAck(logPosition int64) {
	agent.requestedAckPosition = logPosition
}

func (agent *ClusteredServiceAgent) checkForValidInvocation() error {
	if agent.activeLifecycleCallback != LIFECYCLE_CALLBACK_NONE {
		return fmt.Errorf("cluster exception - sending messages or scheduling timers is not allowed from %s", lifecycleName(agent.activeLifecycleCallback))
	}
	return nil
}

func lifecycleName(activeLifecycleCallback LifeCycleCallback) string {
	switch activeLifecycleCallback {
	case LIFECYCLE_CALLBACK_NONE:
		return "none"
	case LIFECYCLE_CALLBACK_ON_START:
		return "onStart"
	case LIFECYCLE_CALLBACK_ON_TERMINATE:
		return "onTerminate"
	case LIFECYCLE_CALLBACK_ON_ROLE_CHANGE:
		return "onRoleChange"
	case LIFECYCLE_CALLBACK_DO_BACKGROUND_WORK:
		return "doBackgroundWork"
	default:
		return "unknown"
	}
}

// END CLUSTER IMPLEMENTATION
