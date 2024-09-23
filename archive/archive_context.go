package archive

import (
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/idlestrategy"
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

func NewArchiveContext(options *Options, aeronCtx *aeron.Context) (*ArchiveContext, error) {
	// Connect the underlying aeron
	aeron, err := aeron.Connect(aeronCtx)
	if err != nil {
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
		Aeron:                   aeron,
		ErrorHandler:            LoggingErrorListener,
		recordingSignalConsumer: LoggingRecordingSignalListener,
		// ControlTermBufferSparse: true,
		// ControlTermBufferLength: 64 * 1024,
		// ControlMtuLength: 8192,
		// Lock:         &sync.Mutex{},
		// AeronDirectoryName: ""
		// CredentialsSupplier: nil
		ArchiveOptions: options,
		AeronCtx:       aeronCtx,
	}

	return ctx, nil
}
