package counters

import (
	"unsafe"

	"github.com/lirm/aeron-go/aeron/atomic"
)

// TODO: refactor this into pkg/namespace
const RECORD_RECLAIMED int32 = -1
const RECORD_UNUSED int32 = 0
const RECORD_ALLOCATED int32 = 1

const RECORDING_POSITION_TYPE_ID = 100

const KEY_OFFSET = KeyOffset

const SIZE_OF_LONG = 8
const SIZE_OF_INT = 4
const TYPE_ID_OFFSET = SIZE_OF_INT
const RECORDING_ID_OFFSET = 0
const SESSION_ID_OFFSET = RECORDING_ID_OFFSET + SIZE_OF_LONG
const SOURCE_IDENTITY_LENGTH_OFFSET = SESSION_ID_OFFSET + SIZE_OF_INT
const SOURCE_IDENTITY_OFFSET = SOURCE_IDENTITY_LENGTH_OFFSET + SIZE_OF_INT

// TODO: fix this - right now need to be here due to import cycle
const NULL_VALUE = -1

// FindCounterIdBySession Find the active counter id for a stream based on the session id and archive id.
// https://github.com/real-logic/aeron/blob/release/1.46.x/aeron-archive/src/main/java/io/aeron/archive/status/RecordingPos.java#L209
func (reader *Reader) FindCounterIdBySession(sessionId int32, archiveId int64) int32 {
	var keyBuf atomic.Buffer
	for id := 0; id < reader.maxCounterID; id++ {
		metaDataOffset := metaDataOffset(int32(id))
		counterState := reader.GetCounterState(int32(id))
		if RECORD_ALLOCATED == counterState {
			if reader.GetCounterTypeId(int32(id)) == RECORDING_POSITION_TYPE_ID {
				keyOffset := metaDataOffset + KeyOffset
				keyPtr := unsafe.Add(reader.metaData.Ptr(), keyOffset)
				keyBuf.Wrap(keyPtr, MaxKeyLength)
				// TODO: Perplexed as why we need to reset the keyOffset here after wrapping the buffer, else an Out of Bounds error is thrown. keyOffset reset is not expected in Java
				keyOffset = 0
				if keyBuf.GetInt32(keyOffset+SESSION_ID_OFFSET) == sessionId {
					sourceIdentityLength := keyBuf.GetInt32(keyOffset + SOURCE_IDENTITY_LENGTH_OFFSET)
					archiveIdOffset := keyOffset + SOURCE_IDENTITY_OFFSET + sourceIdentityLength
					if NULL_VALUE == archiveId || keyBuf.GetInt64(archiveIdOffset) == archiveId {
						return int32(id)
					}
				}
			}
		} else if RECORD_UNUSED == counterState {
			break
		}
	}
	return NullCounterId
}

// TODO: refactor to namespace
// https://github.com/real-logic/agrona/blob/release/1.23.x/agrona/src/main/java/org/agrona/concurrent/status/CountersReader.java#L336
func metaDataOffset(counterId int32) int32 {
	return counterId * MetadataLength
}

// GetCounterState Get the state for a given counter id as a volatile read.
// https://github.com/real-logic/agrona/blob/release/1.23.x/agrona/src/main/java/org/agrona/concurrent/status/CountersReader.java#L557
func (reader *Reader) GetCounterState(counterId int32) int32 {
	return reader.metaData.GetInt32Volatile(metaDataOffset(counterId))
}

// GetRecordingId Get the recording id for a given counter id.
func (reader *Reader) GetRecordingId(counterId int32) int64 {
	if reader.GetCounterState(counterId) == RECORD_ALLOCATED &&
		reader.GetCounterTypeId(counterId) == RECORDING_POSITION_TYPE_ID {
		return reader.metaData.GetInt64(metaDataOffset(counterId) + KEY_OFFSET + RECORDING_ID_OFFSET)
	}

	return int64(NullCounterId)
}

// GetCounterTypeId returns the type id for a counter.
func (reader *Reader) GetCounterTypeId(counterId int32) int32 {
	// TODO: move to validate
	if counterId < 0 || counterId >= int32(reader.maxCounterID) {
		return -1
	}
	return reader.metaData.GetInt32(metaDataOffset(counterId) + TYPE_ID_OFFSET)
}

// TODO:
// func (reader *Reader) validateCounterId(counterId int32) {
// }
