package client

import "testing"

func TestRequestWriter(t *testing.T) {
	request := new(TopicMetadataRequest)
	writer := NewRequestHeader(1, "", request)
	size := writer.Size()
	assert(t, size, int32(4+2+2+4+2+4))
	bytes := make([]byte, size)
	encoder := NewBinaryEncoder(bytes)
	writer.Write(encoder)
	assert(t, bytes, []byte{0x00, 0x00, 0x00, 0x0E, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
}
