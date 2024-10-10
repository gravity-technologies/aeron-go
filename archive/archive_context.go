package archive

import (
	"sync"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/idlestrategy"
	"github.com/lirm/aeron-go/aeron/logging"
	"github.com/lirm/aeron-go/archive/codecs"
)

// ArchiveContext specialised configuration options for communicating with an Aeron Archive.
// Java:
// * https://github.com/real-logic/aeron/blob/1.46.2/aeron-archive/src/main/java/io/aeron/archive/client/AeronArchive.java#L2918
// * https://github.com/real-logic/aeron/blob/1.46.2/aeron-archive/src/main/java/io/aeron/archive/client/AeronArchive.java#L2568
type ArchiveContext struct {
	MessageTimeout          time.Duration
	RecordingEventsChannel  string
	RecordingEventsStreamId int32
	ControlRequestChannel   string
	ControlRequestStreamId  int32
	ControlResponseChannel  string
	ControlResponseStreamId int32
	ControlTermBufferSparse bool
	IdleStrategy            idlestrategy.Idler
	Aeron                   *aeron.Aeron
	Lock                    sync.Mutex
	ErrorHandler            func(error)
	recordingSignalConsumer func(*codecs.RecordingSignalEvent)
	// TODO: refactor
	// Unused for now as there are no context switching yet, all are still residing
	// in the Archive class instead of here.
	// Lock                    *sync.Mutex
	// AeronDirectoryName      string
	// CredentialsSupplier credentialsSupplier;
	// ControlTermBufferLength int32
	// ControlMtuLength        int32
	// OwnsAeronClient         bool
	// IsConcluded             atomic.Bool
	// ---
	// Variable needed for now for backward compat
	ArchiveOptions *Options
	AeronCtx       *aeron.Context
}

// NewArchiveContext bridges the gap from aeron-go archive init to Java
func NewArchiveContext(options *Options, aeronCtx *aeron.Context) (*ArchiveContext, error) {
	// Use the provided options or use our defaults
	if options == nil {
		options = DefaultOptions()
	}

	// Set the logging levels
	logging.SetLevel(options.ArchiveLoglevel, "archive")
	logging.SetLevel(options.AeronLoglevel, "aeron")
	logging.SetLevel(options.AeronLoglevel, "memmap")
	logging.SetLevel(options.AeronLoglevel, "driver")
	logging.SetLevel(options.AeronLoglevel, "counters")
	logging.SetLevel(options.AeronLoglevel, "logbuffers")
	logging.SetLevel(options.AeronLoglevel, "buffer")
	logging.SetLevel(options.AeronLoglevel, "rb")

	// In Debug mode initialize our listeners with simple loggers
	// Note that these actually log at INFO so you can do this manually for INFO if you like
	if logging.GetLevel("archive") >= logging.DEBUG {
		logger.Debugf("Setting logging listeners")

		aeronCtx.NewSubscriptionHandler(LoggingNewSubscriptionListener)
		aeronCtx.NewPublicationHandler(LoggingNewPublicationListener)
	}

	aeronClient, err := aeron.Connect(aeronCtx)
	if err != nil {
		logger.Errorf("archive_context :: unable to connect to aeron : %s", err)
		return nil, err
	}

	ctx := &ArchiveContext{
		MessageTimeout:          options.Timeout,
		RecordingEventsChannel:  options.RecordingEventsChannel,
		RecordingEventsStreamId: options.RecordingEventsStream,
		ControlRequestChannel:   options.RequestChannel,
		ControlRequestStreamId:  options.RequestStream,
		ControlResponseChannel:  options.ResponseChannel,
		ControlResponseStreamId: options.ResponseStream,
		IdleStrategy:            options.IdleStrategy,
		Aeron:                   aeronClient,
		Lock:                    sync.Mutex{},
		ErrorHandler:            LoggingErrorListener,
		recordingSignalConsumer: LoggingRecordingSignalListener,
		// ControlTermBufferSparse: true,
		// ControlTermBufferLength: 64 * 1024,
		// ControlMtuLength: 8192,
		// AeronDirectoryName: ""
		// CredentialsSupplier: nil
		ArchiveOptions: options,
		AeronCtx:       aeronCtx,
	}

	return ctx, nil
}

func (ac *ArchiveContext) Close() {
	if ac.Aeron != nil {
		if err := ac.Aeron.Close(); err != nil {
			logger.Errorf("archive_context :: failed to close aeron : %s", err)
		}
	}
}
