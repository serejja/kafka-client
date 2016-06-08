package client

// HeartbeatRequest is used to keep a member alive in a group.
type HeartbeatRequest struct {
	GroupID      string
	GenerationID int32
	MemberID     string
}

// Key returns the Kafka API key for HeartbeatRequest.
func (*HeartbeatRequest) Key() int16 {
	return 12
}

// Version returns the Kafka request version for backwards compatibility.
func (*HeartbeatRequest) Version() int16 {
	return 0
}

func (hr *HeartbeatRequest) Write(encoder Encoder) {
	encoder.WriteString(hr.GroupID)
	encoder.WriteInt32(hr.GenerationID)
	encoder.WriteString(hr.MemberID)
}

// HeartbeatResponse signals whether the sent HeartbeatRequest succeeded or not, and tells why if not.
type HeartbeatResponse struct {
	Error error
}

func (hr *HeartbeatResponse) Read(decoder Decoder) *DecodingError {
	errCode, err := decoder.GetInt16()
	if err != nil {
		return NewDecodingError(err, reasonInvalidHeartbeatResponseErrorCode)
	}

	hr.Error = BrokerErrors[errCode]
	return nil
}

var reasonInvalidHeartbeatResponseErrorCode = "Invalid error code in HeartbeatResponse"
