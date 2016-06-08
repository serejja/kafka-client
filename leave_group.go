package client

type LeaveGroupRequest struct {
	GroupID  string
	MemberID string
}

// Key returns the Kafka API key for LeaveGroupRequest.
func (*LeaveGroupRequest) Key() int16 {
	return 13
}

// Version returns the Kafka request version for backwards compatibility.
func (*LeaveGroupRequest) Version() int16 {
	return 0
}

func (lgr *LeaveGroupRequest) Write(encoder Encoder) {
	encoder.WriteString(lgr.GroupID)
	encoder.WriteString(lgr.MemberID)
}

type LeaveGroupResponse struct {
	Error error
}

func (lgr *LeaveGroupResponse) Read(decoder Decoder) *DecodingError {
	errCode, err := decoder.GetInt16()
	if err != nil {
		return NewDecodingError(err, reasonInvalidLeaveGroupResponseErrorCode)
	}
	lgr.Error = BrokerErrors[errCode]
	return nil
}

var reasonInvalidLeaveGroupResponseErrorCode = "Invalid error code in LeaveGroupResponse"
