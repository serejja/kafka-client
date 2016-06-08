package client

import "testing"

var emptySyncGroupRequestBytes = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
var goodSyncGroupRequestBytes = []byte{0x00, 0x03, 'f', 'o', 'o', 0x00, 0x00, 0x00, 0x03, 0x00, 0x03, 'b', 'a', 'r', 0x00, 0x00, 0x00, 0x01, 0x00, 0x03, 'f', 'o', 'o', 0x00, 0x00, 0x00, 0x01, 0x01}

var errorSyncGroupResponseBytes = []byte{0x00, 27, 0x00, 0x00, 0x00, 0x00}
var goodSyncGroupResponseBytes = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01}

func TestSyncGroupRequest(t *testing.T) {
	emptySyncGroupRequest := new(SyncGroupRequest)
	testRequest(t, emptySyncGroupRequest, emptySyncGroupRequestBytes)

	goodSyncGroupRequest := new(SyncGroupRequest)
	goodSyncGroupRequest.GroupID = "foo"
	goodSyncGroupRequest.GenerationID = 3
	goodSyncGroupRequest.MemberID = "bar"
	goodSyncGroupRequest.GroupAssignment = map[string][]byte{
		"foo": {1},
	}
	testRequest(t, goodSyncGroupRequest, goodSyncGroupRequestBytes)
}

func TestSyncGroupResponse(t *testing.T) {
	errorSyncGroupResponse := new(SyncGroupResponse)
	decode(t, errorSyncGroupResponse, errorSyncGroupResponseBytes)
	assert(t, errorSyncGroupResponse.Error, ErrRebalanceInProgress)

	goodSyncGroupResponse := new(SyncGroupResponse)
	decode(t, goodSyncGroupResponse, goodSyncGroupResponseBytes)
	assert(t, goodSyncGroupResponse.Error, ErrNoError)
	assert(t, goodSyncGroupResponse.MemberAssignment, []byte{1})
}
