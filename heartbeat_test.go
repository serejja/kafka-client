package client

import "testing"

var emptyHeartbeatRequestBytes = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
var goodHeartbeatRequestBytes = []byte{0x00, 0x05, 'g', 'r', 'o', 'u', 'p', 0x00, 0x00, 0x00, 0x03, 0x00, 0x06, 'm', 'e', 'm', 'b', 'e', 'r'}

var errorHeartbeatResponseBytes = []byte{0x00, 25}
var goodHeartbeatResponseBytes = []byte{0x00, 0x00}

func TestHeartbeatRequest(t *testing.T) {
	emptyHeartbeatRequest := new(HeartbeatRequest)
	testRequest(t, emptyHeartbeatRequest, emptyHeartbeatRequestBytes)

	goodHeartbeatRequest := new(HeartbeatRequest)
	goodHeartbeatRequest.GroupID = "group"
	goodHeartbeatRequest.GenerationID = 3
	goodHeartbeatRequest.MemberID = "member"
	testRequest(t, goodHeartbeatRequest, goodHeartbeatRequestBytes)
}

func TestHeartbeatResponse(t *testing.T) {
	errorHeartbeatResponse := new(HeartbeatResponse)
	decode(t, errorHeartbeatResponse, errorHeartbeatResponseBytes)
	assert(t, errorHeartbeatResponse.Error, ErrUnknownMemberID)

	goodHeartbeatResponse := new(HeartbeatResponse)
	decode(t, goodHeartbeatResponse, goodHeartbeatResponseBytes)
	assert(t, goodHeartbeatResponse.Error, ErrNoError)
}
