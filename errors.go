package client

import "errors"

// Signals that an end of file or stream has been reached unexpectedly.
var ErrEOF = errors.New("End of file reached")

// Happens when a compressed message is empty.
var ErrNoDataToUncompress = errors.New("No data to uncompress")

// A mapping for Kafka error code 0.
var ErrNoError = errors.New("No error - it worked!")

// A mapping for Kafka error code -1.
var ErrUnknown = errors.New("An unexpected server error")

// A mapping for Kafka error code 1.
var ErrOffsetOutOfRange = errors.New("The requested offset is outside the range of offsets maintained by the server for the given topic/partition.")

// A mapping for Kafka error code 2.
var ErrInvalidMessage = errors.New("Message contents does not match its CRC")

// A mapping for Kafka error code 3.
var ErrUnknownTopicOrPartition = errors.New("This request is for a topic or partition that does not exist on this broker.")

// A mapping for Kafka error code 4.
var ErrInvalidMessageSize = errors.New("The message has a negative size")

// A mapping for Kafka error code 5.
var ErrLeaderNotAvailable = errors.New("In the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes.")

// A mapping for Kafka error code 6.
var ErrNotLeaderForPartition = errors.New("You've just attempted to send messages to a replica that is not the leader for some partition. It indicates that the clients metadata is out of date.")

// A mapping for Kafka error code 7.
var ErrRequestTimedOut = errors.New("Request exceeds the user-specified time limit in the request.")

// A mapping for Kafka error code 8.
var ErrBrokerNotAvailable = errors.New("Broker is likely not alive.")

// A mapping for Kafka error code 9.
var ErrReplicaNotAvailable = errors.New("Replica is expected on a broker, but is not (this can be safely ignored).")

// A mapping for Kafka error code 10.
var ErrMessageSizeTooLarge = errors.New("You've just attempted to produce a message of size larger than broker is allowed to accept.")

// A mapping for Kafka error code 11.
var ErrStaleControllerEpochCode = errors.New("Broker-to-broker communication fault.")

// A mapping for Kafka error code 12.
var ErrOffsetMetadataTooLargeCode = errors.New("You've jsut specified a string larger than configured maximum for offset metadata.")

// A mapping for Kafka error code 14.
var ErrOffsetsLoadInProgressCode = errors.New("Offset loading is in progress. (Usually happens after a leader change for that offsets topic partition).")

// A mapping for Kafka error code 15.
var ErrConsumerCoordinatorNotAvailableCode = errors.New("Offsets topic has not yet been created.")

// A mapping for Kafka error code 16.
var ErrNotCoordinatorForConsumerCode = errors.New("There is no coordinator for this consumer.")

// A mapping for Kafka error code 17
var ErrInvalidTopicCode = errors.New("Attempt to access an invalid topic.")

// A mapping for Kafka error code 18
var ErrRecordListTooLarge = errors.New("Message batch exceeds the maximum configured segment size.")

// A mapping for Kafka error code 19
var ErrNotEnoughReplicas = errors.New("The number of in-sync replicas is lower than the configured minimum.")

// A mapping for Kafka error code 20
var ErrNotEnoughReplicasAfterAppend = errors.New("The message was written to the log, but with fewer in-sync replicas than required.")

// A mapping for Kafka error code 21
var ErrInvalidRequiredAcks = errors.New("The requested requiredAcks is invalid.")

// A mapping for Kafka error code 22
var ErrIllegalGeneration = errors.New("The generation id provided in the request is not the current generation.")

// A mapping for Kafka error code 23
var ErrInconsistentGroupProtocol = errors.New("Provided protocol type or set of protocols is not compatible with the current group.")

// A mapping for Kafka error code 24
var ErrInvalidGroupID = errors.New("The groupId is empty or null.")

// A mapping for Kafka error code 25
var ErrUnknownMemberID = errors.New("The memberId is not in the current generation.")

// A mapping for Kafka error code 26
var ErrInvalidSessionTimeout = errors.New("The requested session timeout is outside of the allowed range on the broker.")

// A mapping for Kafka error code 27
var ErrRebalanceInProgress = errors.New("The coordinator has begun rebalancing the group. This indicates to the client that it should rejoin the group.")

// A mapping for Kafka error code 28
var ErrInvalidCommitOffsetSize = errors.New("Offset commit was rejected because of oversize metadata.")

// A mapping for Kafka error code 29
var ErrTopicAuthorizationFailed = errors.New("The client is not authorized to access the requested topic.")

// A mapping for Kafka error code 30
var ErrGroupAuthorizationFailed = errors.New("The client is not authorized to access a particular groupId.")

// A mapping for Kafka error code 31
var ErrClusterAuthorizationFailed = errors.New("The client is not authorized to use an inter-broker or administrative API.")

// ErrFailedToGetMetadata happens when TopicMetadataResponse does not contain metadata for requested topic.
var ErrFailedToGetMetadata = errors.New("Failed to get topic metadata.")

// ErrFailedToGetLeader happens when TopicMetadataResponse does not contain leader metadata for requested topic and partition.
var ErrFailedToGetLeader = errors.New("Failed to get leader.")

// Mapping between Kafka error codes and actual error messages.
var BrokerErrors = map[int16]error{
	-1: ErrUnknown,
	0:  ErrNoError,
	1:  ErrOffsetOutOfRange,
	2:  ErrInvalidMessage,
	3:  ErrUnknownTopicOrPartition,
	4:  ErrInvalidMessageSize,
	5:  ErrLeaderNotAvailable,
	6:  ErrNotLeaderForPartition,
	7:  ErrRequestTimedOut,
	8:  ErrBrokerNotAvailable,
	9:  ErrReplicaNotAvailable,
	10: ErrMessageSizeTooLarge,
	11: ErrStaleControllerEpochCode,
	12: ErrOffsetMetadataTooLargeCode,
	14: ErrOffsetsLoadInProgressCode,
	15: ErrConsumerCoordinatorNotAvailableCode,
	16: ErrNotCoordinatorForConsumerCode,
	17: ErrInvalidTopicCode,
	18: ErrRecordListTooLarge,
	19: ErrNotEnoughReplicas,
	20: ErrNotEnoughReplicasAfterAppend,
	21: ErrInvalidRequiredAcks,
	22: ErrIllegalGeneration,
	23: ErrInconsistentGroupProtocol,
	24: ErrInvalidGroupID,
	25: ErrUnknownMemberID,
	26: ErrInvalidSessionTimeout,
	27: ErrRebalanceInProgress,
	28: ErrInvalidCommitOffsetSize,
	29: ErrTopicAuthorizationFailed,
	30: ErrGroupAuthorizationFailed,
	31: ErrClusterAuthorizationFailed,
}
