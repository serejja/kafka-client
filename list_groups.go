package client

// ListGroupsRequest is used to list the current groups managed by a broker
type ListGroupsRequest struct{}

// Key returns the Kafka API key for ListGroupsRequest.
func (*ListGroupsRequest) Key() int16 {
	return 16
}

// Version returns the Kafka request version for backwards compatibility.
func (*ListGroupsRequest) Version() int16 {
	return 0
}

func (*ListGroupsRequest) Write(encoder Encoder) {}

// ListGroupsResponse lists the current groups managed by a broker and contains an error if it happened.
type ListGroupsResponse struct {
	Error  error
	Groups map[string]string
}

func (lgr *ListGroupsResponse) Read(decoder Decoder) *DecodingError {
	errCode, err := decoder.GetInt16()
	if err != nil {
		return NewDecodingError(err, reasonInvalidListGroupsResponseErrorCode)
	}
	lgr.Error = BrokerErrors[errCode]

	groupsLen, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reasonInvalidListGroupsResponseGroupsLength)
	}

	lgr.Groups = make(map[string]string, groupsLen)
	for i := int32(0); i < groupsLen; i++ {
		groupID, err := decoder.GetString()
		if err != nil {
			return NewDecodingError(err, reasonInvalidListGroupsResponseGroupID)
		}

		protocolType, err := decoder.GetString()
		if err != nil {
			return NewDecodingError(err, reasonInvalidListGroupsResponseProtocolType)
		}
		lgr.Groups[groupID] = protocolType
	}

	return nil
}

const (
	reasonInvalidListGroupsResponseErrorCode    = "Invalid error code in ListGroupsResponse"
	reasonInvalidListGroupsResponseGroupsLength = "Invalid groups length in ListGroupsResponse"
	reasonInvalidListGroupsResponseGroupID      = "Invalid group id in ListGroupsResponse"
	reasonInvalidListGroupsResponseProtocolType = "Invalid protocol type in ListGroupsResponse"
)
