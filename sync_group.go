package client

type SyncGroupRequest struct {
	GroupID         string
	GenerationID    int32
	MemberID        string
	GroupAssignment map[string][]byte
}

// Key returns the Kafka API key for SyncGroupRequest.
func (*SyncGroupRequest) Key() int16 {
	return 14
}

// Version returns the Kafka request version for backwards compatibility.
func (*SyncGroupRequest) Version() int16 {
	return 0
}

func (sgr *SyncGroupRequest) Write(encoder Encoder) {
	encoder.WriteString(sgr.GroupID)
	encoder.WriteInt32(sgr.GenerationID)
	encoder.WriteString(sgr.MemberID)

	encoder.WriteInt32(int32(len(sgr.GroupAssignment)))

	for memberID, memberAssignment := range sgr.GroupAssignment {
		encoder.WriteString(memberID)
		encoder.WriteBytes(memberAssignment)
	}
}

type SyncGroupResponse struct {
	Error            error
	MemberAssignment []byte
}

func (sgr *SyncGroupResponse) Read(decoder Decoder) *DecodingError {
	errCode, err := decoder.GetInt16()
	if err != nil {
		return NewDecodingError(err, reasonInvalidSyncGroupResponseErrorCode)
	}
	sgr.Error = BrokerErrors[errCode]

	sgr.MemberAssignment, err = decoder.GetBytes()
	if err != nil {
		return NewDecodingError(err, reasonInvalidSyncGroupResponseMemberAssignment)
	}

	return nil
}

var (
	reasonInvalidSyncGroupResponseErrorCode        = "Invalid error code in SyncGroupResponse"
	reasonInvalidSyncGroupResponseMemberAssignment = "Invalid SyncGroupResponse MemberAssignment"
)
