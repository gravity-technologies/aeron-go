package archive

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"
	"unsafe"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/idlestrategy"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/archive/codecs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestControl_PollForResponse(t *testing.T) {
	t.Run("times out when nothing to poll", func(t *testing.T) {
		control, image := newTestControl(t)
		mockPollResponses(t, control, image, false)
		correlations.Store(int64(1), control)
		id, err := control.PollForResponse(1, 0)
		assert.Equal(t, int(id), aeron.NullValue)
		assert.EqualError(t, err, `timeout waiting for correlationID 1`)
	})

	t.Run("returns the error response", func(t *testing.T) {
		control, image := newTestControl(t)
		mockPollResponses(t, control, image, false,
			&codecs.ControlResponse{
				Code:          codecs.ControlResponseCode.ERROR,
				CorrelationId: 1,
				ErrorMessage:  []byte(`b0rk`),
				RelevantId:    3,
			},
		)
		correlations.Store(int64(1), control)
		id, err := control.PollForResponse(1, 0)
		assert.EqualValues(t, -1, id)
		assert.EqualError(t, err, `archive error: response for correlationId=1, error: b0rk`)
	})

	t.Run("discards all responses preceding the first error", func(t *testing.T) {
		control, image := newTestControl(t)
		mockPollResponses(t, control, image, false,
			&codecs.ControlResponse{Code: codecs.ControlResponseCode.OK},
			&codecs.ControlResponse{Code: codecs.ControlResponseCode.OK},
			&codecs.ControlResponse{
				Code:          codecs.ControlResponseCode.ERROR,
				CorrelationId: 1,
				ErrorMessage:  []byte(`b0rk`),
				RelevantId:    3,
			},
		)
		correlations.Store(int64(1), control)
		id, err := control.PollForResponse(1, 0)
		assert.EqualValues(t, -1, id)
		assert.EqualError(t, err, `archive error: response for correlationId=1, error: b0rk`)
	})

	t.Run("does not process messages after result", func(t *testing.T) {
		control, image := newTestControl(t)
		mockPollResponses(t, control, image, false,
			&codecs.ControlResponse{
				Code:          codecs.ControlResponseCode.ERROR,
				CorrelationId: 1,
				ErrorMessage:  []byte(`b0rk`),
				RelevantId:    3,
			},
			&codecs.ControlResponse{Code: codecs.ControlResponseCode.OK},
		)
		correlations.Store(int64(1), control)
		id, err := control.PollForResponse(1, 0)
		assert.EqualValues(t, -1, id)
		assert.EqualError(t, err, `archive error: response for correlationId=1, error: b0rk`)
		fragments := image.Poll(func(buffer *atomic.Buffer, offset, length int32, header *logbuffer.Header) {}, 1)
		assert.EqualValues(t, 1, fragments)
	})
}

func TestControl_PollForErrorResponse(t *testing.T) {
	t.Run("return ArchiveError STORAGE_SPACE when RelevantId=11", func(t *testing.T) {
		control, image := newTestControl(t)
		mockPollResponses(t, control, image, true,
			&codecs.ControlResponse{
				Code:         codecs.ControlResponseCode.ERROR,
				RelevantId:   int64(ExceptionSpaceStorage),
				ErrorMessage: []byte(`11`),
			},
		)
		cnt, err := control.PollForErrorResponse()
		var archiveErr *ArchiveError
		assert.True(t, errors.As(err, &archiveErr))
		assert.EqualValues(t, ExceptionSpaceStorage, int(archiveErr.ErrorCode))
		assert.EqualValues(t, -1, cnt)
		assert.EqualError(t, err, `archive error: PollForErrorResponse received a ControlResponse (correlationId:0 Code:ERROR error="11")`)
	})

	t.Run("returns zero when nothing to poll", func(t *testing.T) {
		control, image := newTestControl(t)
		mockPollResponses(t, control, image, true)
		cnt, err := control.PollForErrorResponse()
		// assert.Zero(t, cnt)
		assert.EqualValues(t, -1, cnt)
		assert.NoError(t, err)
	})

	t.Run("returns the error response", func(t *testing.T) {
		control, image := newTestControl(t)
		mockPollResponses(t, control, image, true,
			&codecs.ControlResponse{
				Code:         codecs.ControlResponseCode.ERROR,
				ErrorMessage: []byte(`b0rk`),
			},
		)
		cnt, err := control.PollForErrorResponse()
		assert.EqualValues(t, -1, cnt)
		assert.EqualError(t, err, `archive error: PollForErrorResponse received a ControlResponse (correlationId:0 Code:ERROR error="b0rk")`)
	})

	t.Run("discards all queued responses unless error", func(t *testing.T) {
		control, image := newTestControl(t)
		mockPollResponses(t, control, image, true,
			&codecs.ControlResponse{Code: codecs.ControlResponseCode.OK},
			&codecs.ControlResponse{Code: codecs.ControlResponseCode.OK},
		)
		cnt, err := control.PollForErrorResponse()
		assert.EqualValues(t, -1, cnt)
		assert.NoError(t, err)
	})

	t.Run("does not process messages after error", func(t *testing.T) {
		control, image := newTestControl(t)
		mockPollResponses(t, control, image, true,
			&codecs.ControlResponse{
				Code:         codecs.ControlResponseCode.ERROR,
				ErrorMessage: []byte(`b0rk`),
			},
			&codecs.ControlResponse{Code: codecs.ControlResponseCode.OK},
		)
		cnt, err := control.PollForErrorResponse()
		assert.EqualValues(t, -1, cnt)
		assert.Error(t, err)
		cnt, err = control.PollForErrorResponse()
		assert.EqualValues(t, -1, cnt)
		assert.NoError(t, err)
	})
}

func mockPollResponses(t *testing.T, control *Control, image *aeron.MockImage, isErrorResponse bool, responses ...encodable) {
	poll := image.On("Poll", mock.Anything, mock.Anything)
	poll.Maybe()
	poll.Run(func(args mock.Arguments) {
		handler := args.Get(0).(term.FragmentHandler)
		fragmentCount := args.Get(1).(int)
		count := image.ControlledPoll(func(buffer *atomic.Buffer, offset, length int32, header *logbuffer.Header) term.ControlledPollAction {
			handler(buffer, offset, length, header)
			return term.ControlledPollActionContinue
		}, fragmentCount)
		poll.Return(count)
	})
	isClosed := image.On("IsClosed", mock.Anything)
	isClosed.Maybe()
	isClosed.Run(func(args mock.Arguments) {
		isClosed.Return(false)
	})
	controlledPoll := image.On("ControlledPoll", mock.Anything, mock.Anything)
	controlledPoll.Maybe()
	controlledPoll.Run(func(args mock.Arguments) {
		fragmentCount := args.Get(1).(int)
		var handler term.ControlledFragmentHandler
		if isErrorResponse {
			handler = control.fragmentAssembler.OnFragment
		} else {
			handler = args.Get(0).(term.ControlledFragmentHandler)
		}
		count := 0
		for count < fragmentCount {
			if len(responses) == 0 {
				break
			}
			buffer := encode(t, responses[0])
			action := handler(buffer, 0, buffer.Capacity(), newTestHeader())
			if action == term.ControlledPollActionAbort {
				break
			}
			responses = responses[1:]
			count++
			if action == term.ControlledPollActionBreak {
				break
			}
		}
		controlledPoll.Return(count)
	})

}

type encodable interface {
	SbeBlockLength() uint16
	SbeTemplateId() uint16
	SbeSchemaId() uint16
	SbeSchemaVersion() uint16
	Encode(*codecs.SbeGoMarshaller, io.Writer, bool) error
}

func encode(t *testing.T, data encodable) *atomic.Buffer {
	m := codecs.NewSbeGoMarshaller()
	buf := new(bytes.Buffer)
	header := codecs.MessageHeader{
		BlockLength: data.SbeBlockLength(),
		TemplateId:  data.SbeTemplateId(),
		SchemaId:    data.SbeSchemaId(),
		Version:     data.SbeSchemaVersion(),
	}
	if !assert.NoError(t, header.Encode(m, buf)) {
		return nil
	}
	if !assert.NoError(t, data.Encode(m, buf, false)) {
		return nil
	}
	return atomic.MakeBuffer(buf.Bytes())
}

func newTestControl(t *testing.T) (*Control, *aeron.MockImage) {
	image := aeron.NewMockImage(t)
	c := &Control{
		Subscription: newTestSub(image),
	}
	c.archive = &Archive{
		Listeners: &ArchiveListeners{},
		Options: &Options{
			Timeout:      100 * time.Millisecond,
			IdleStrategy: idlestrategy.Yielding{},
		},
	}
	c.fragmentAssembler = aeron.NewControlledFragmentAssembler(
		c.onFragment, aeron.DefaultFragmentAssemblyBufferLength,
	)
	c.errorFragmentHandler = c.errorResponseFragmentHandler
	return c, image
}

func newTestSub(image aeron.Image) *aeron.Subscription {
	images := aeron.NewImageList()
	images.Set([]aeron.Image{image})
	sub := aeron.NewSubscription(nil, "", 1, 2, 3, nil, nil)
	rsub := reflect.ValueOf(sub)
	rfimages := rsub.Elem().FieldByName("images")
	rfimages = reflect.NewAt(rfimages.Type(), unsafe.Pointer(rfimages.UnsafeAddr())).Elem()
	rfimages.Set(reflect.ValueOf(images))
	return sub
}

func newTestHeader() *logbuffer.Header {
	buffer := atomic.MakeBuffer(make([]byte, logbuffer.DataFrameHeader.Length))
	buffer.PutUInt8(logbuffer.DataFrameHeader.FlagsFieldOffset, 0xc0) // unfragmented
	return new(logbuffer.Header).Wrap(buffer.Ptr(), buffer.Capacity())
}
