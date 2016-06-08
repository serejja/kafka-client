package client

import "testing"

var emptyLeaveGroupRequestBytes = []byte{0x00, 0x00, 0x00, 0x00}
var goodLeaveGroupRequestBytes = []byte{0x00, 0x05, 'g', 'r', 'o', 'u', 'p', 0x00, 0x06, 'm', 'e', 'm', 'b', 'e', 'r'}

var errorLeaveGroupResponseBytes = []byte{0x00, 15}
var goodLeaveGroupResponseBytes = []byte{0x00, 0x00}

func TestLeaveGroupRequest(t *testing.T) {
	emptyRequest := new(LeaveGroupRequest)
	testRequest(t, emptyRequest, emptyLeaveGroupRequestBytes)

	goodRequest := new(LeaveGroupRequest)
	goodRequest.GroupID = "group"
	goodRequest.MemberID = "member"
	testRequest(t, goodRequest, goodLeaveGroupRequestBytes)
}

func TestLeaveGroupResponse(t *testing.T) {
	errorLeaveGroupResponse := new(LeaveGroupResponse)
	decode(t, errorLeaveGroupResponse, errorLeaveGroupResponseBytes)
	assert(t, errorLeaveGroupResponse.Error, ErrConsumerCoordinatorNotAvailableCode)

	goodLeaveGroupResponse := new(LeaveGroupResponse)
	decode(t, goodLeaveGroupResponse, goodLeaveGroupResponseBytes)
	assert(t, goodLeaveGroupResponse.Error, ErrNoError)
}
