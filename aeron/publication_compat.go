package aeron

import "fmt"

// https://github.com/real-logic/aeron/blob/release/1.46.x/aeron-client/src/main/java/io/aeron/Publication.java#L693
func (pub *Publication) checkPayloadLength(length int32) {
	if length < 0 {
		panic(fmt.Sprintf("invalid length: %d", length))
	}

	if length > pub.maxPayloadLength {
		panic(fmt.Sprintf("claim exceeds maxPayloadLength of %d, length=%d", pub.maxPayloadLength, length))
	}
}
