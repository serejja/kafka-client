package client

import "testing"

var emptyListGroupsResponseBytes = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
var errorListGroupsResponseBytes = []byte{0x00, 15, 0x00, 0x00, 0x00, 0x00}
var goodListGroupsResponseBytes = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x03, 'f', 'o', 'o', 0x00, 0x08, 'c', 'o', 'n', 's', 'u', 'm', 'e', 'r'}

func TestListGroupsResponse(t *testing.T) {
	emptyResponse := new(ListGroupsResponse)
	decode(t, emptyResponse, emptyListGroupsResponseBytes)
	assert(t, emptyResponse.Error, ErrNoError)
	assert(t, len(emptyResponse.Groups), 0)

	errorResponse := new(ListGroupsResponse)
	decode(t, errorResponse, errorListGroupsResponseBytes)
	assert(t, errorResponse.Error, ErrConsumerCoordinatorNotAvailableCode)

	goodResponse := new(ListGroupsResponse)
	decode(t, goodResponse, goodListGroupsResponseBytes)
	assert(t, goodResponse.Error, ErrNoError)
	assert(t, len(goodResponse.Groups), 1)
	assert(t, goodResponse.Groups["foo"], "consumer")
}
