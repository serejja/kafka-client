// kafka-client is a low-level Apache Kafka client in Go.

package client

import (
	"errors"
	"fmt"
	"github.com/yanzay/log"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// InvalidOffset is a constant that is used to denote an invalid or uninitialized offset.
const InvalidOffset int64 = -1

// Client is an interface that should provide ways to clearly interact with Kafka cluster and hide all broker management stuff from user.
type Client interface {
	// GetTopicMetadata is primarily used to discover leaders for given topics and how many partitions these topics have.
	// Passing it an empty topic list will retrieve metadata for all topics in a cluster.
	GetTopicMetadata(topics []string) (*MetadataResponse, error)

	// GetAvailableOffset issues an offset request to a specified topic and partition with a given offset time.
	// More on offset time here - https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetRequest
	GetAvailableOffset(topic string, partition int32, offsetTime int64) (int64, error)

	// Fetch issues a single fetch request to a broker responsible for a given topic and partition and returns a FetchResponse that contains messages starting from a given offset.
	Fetch(topic string, partition int32, offset int64) (*FetchResponse, error)

	// GetOffset gets the offset for a given group, topic and partition from Kafka. A part of new offset management API.
	GetOffset(group string, topic string, partition int32) (int64, error)

	// CommitOffset commits the offset for a given group, topic and partition to Kafka. A part of new offset management API.
	CommitOffset(group string, topic string, partition int32, offset int64) error

	GetLeader(topic string, partition int32) (*BrokerConnection, error)

	GetConsumerMetadata(group string) (*ConsumerMetadataResponse, error)

	// Metadata returns a structure that holds all topic and broker metadata.
	Metadata() *Metadata

	// Tells the Client to close all existing connections and stop.
	// This method is NOT blocking but returns a channel which will be closed once the closing is finished.
	Close() <-chan struct{}
}

// Config is used to pass multiple configuration values for a Client
type Config struct {
	// BrokerList is a bootstrap list to discover other brokers in a cluster. At least one broker is required.
	BrokerList []string

	// ReadTimeout is a timeout to read the response from a TCP socket.
	ReadTimeout time.Duration

	// WriteTimeout is a timeout to write the request to a TCP socket.
	WriteTimeout time.Duration

	// ConnectTimeout is a timeout to connect to a TCP socket.
	ConnectTimeout time.Duration

	// Sets whether the connection should be kept alive.
	KeepAlive bool

	// A keep alive period for a TCP connection.
	KeepAliveTimeout time.Duration

	// Maximum fetch size in bytes which will be used in all Consume() calls.
	FetchSize int32

	// The minimum amount of data the server should return for a fetch request. If insufficient data is available the request will block
	FetchMinBytes int32

	// The maximum amount of time the server will block before answering the fetch request if there isn't sufficient data to immediately satisfy FetchMinBytes
	FetchMaxWaitTime int32

	// Number of retries to get topic metadata.
	MetadataRetries int

	// Backoff value between topic metadata requests.
	MetadataBackoff time.Duration

	// MetadataTTL is how long topic metadata is considered valid. Used to refresh metadata from time to time even if no leader changes occurred.
	MetadataTTL time.Duration

	// Number of retries to commit an offset.
	CommitOffsetRetries int

	// Backoff value between commit offset requests.
	CommitOffsetBackoff time.Duration

	// Number of retries to get consumer metadata.
	ConsumerMetadataRetries int

	// Backoff value between consumer metadata requests.
	ConsumerMetadataBackoff time.Duration

	// ClientID that will be used by a Client to identify client requests by broker.
	ClientID string
}

// NewConfig returns a new ClientConfig with sane defaults.
func NewConfig() *Config {
	return &Config{
		ReadTimeout:             5 * time.Second,
		WriteTimeout:            5 * time.Second,
		ConnectTimeout:          5 * time.Second,
		KeepAlive:               true,
		KeepAliveTimeout:        1 * time.Minute,
		FetchMinBytes:           1,
		FetchSize:               1024000,
		FetchMaxWaitTime:        1000,
		MetadataRetries:         5,
		MetadataBackoff:         200 * time.Millisecond,
		MetadataTTL:             5 * time.Minute,
		CommitOffsetRetries:     5,
		CommitOffsetBackoff:     200 * time.Millisecond,
		ConsumerMetadataRetries: 15,
		ConsumerMetadataBackoff: 500 * time.Millisecond,
		ClientID:                "kafka-client",
	}
}

// Validate validates this ClientConfig. Returns a corresponding error if the ClientConfig is invalid and nil otherwise.
func (cc *Config) Validate() error {
	if len(cc.BrokerList) == 0 {
		return ErrConfigNoBrokers
	}

	if cc.ReadTimeout < time.Millisecond {
		return ErrConfigInvalidReadTimeout
	}

	if cc.WriteTimeout < time.Millisecond {
		return ErrConfigInvalidWriteTimeout
	}

	if cc.ConnectTimeout < time.Millisecond {
		return ErrConfigInvalidConnectTimeout
	}

	if cc.KeepAliveTimeout < time.Millisecond {
		return ErrConfigInvalidKeepAliveTimeout
	}

	if cc.FetchSize < 1 {
		return ErrConfigInvalidFetchSize
	}

	if cc.MetadataRetries < 0 {
		return ErrConfigInvalidMetadataRetries
	}

	if cc.MetadataBackoff < time.Millisecond {
		return ErrConfigInvalidMetadataBackoff
	}

	if cc.MetadataTTL < time.Millisecond {
		return ErrConfigInvalidMetadataTTL
	}

	if cc.CommitOffsetRetries < 0 {
		return ErrConfigInvalidCommitOffsetRetries
	}

	if cc.CommitOffsetBackoff < time.Millisecond {
		return ErrConfigInvalidCommitOffsetBackoff
	}

	if cc.ConsumerMetadataRetries < 0 {
		return ErrConfigInvalidConsumerMetadataRetries
	}

	if cc.ConsumerMetadataBackoff < time.Millisecond {
		return ErrConfigInvalidConsumerMetadataBackoff
	}

	if cc.ClientID == "" {
		return ErrConfigEmptyClientID
	}

	return nil
}

// KafkaClient is a default (and only one for now) Client implementation for kafka-client library.
type KafkaClient struct {
	config           Config
	metadata         *Metadata
	bootstrapBrokers []*BrokerConnection
	lock             sync.RWMutex
}

// New creates a new KafkaClient with a given ClientConfig. May return an error if the passed config is invalid.
func New(config *Config) (*KafkaClient, error) {
	if config == nil {
		return nil, ErrNoClientConfig
	}

	if err := config.Validate(); err != nil {
		return nil, err
	}

	bootstrapConnections := make([]*BrokerConnection, len(config.BrokerList))
	for i := 0; i < len(config.BrokerList); i++ {
		broker := config.BrokerList[i]
		hostPort := strings.Split(broker, ":")
		if len(hostPort) != 2 {
			return nil, fmt.Errorf("incorrect broker connection string: %s", broker)
		}

		port, err := strconv.Atoi(hostPort[1])
		if err != nil {
			return nil, fmt.Errorf("incorrect port in broker connection string: %s", broker)
		}

		bootstrapConnections[i] = NewBrokerConnection(&Broker{
			ID:   -1,
			Host: hostPort[0],
			Port: int32(port),
		}, config.KeepAliveTimeout)
	}

	kafkaClient := &KafkaClient{
		config:           *config,
		bootstrapBrokers: bootstrapConnections,
	}
	kafkaClient.metadata = NewMetadata(kafkaClient, NewBrokers(config.KeepAliveTimeout), config.MetadataTTL)

	return kafkaClient, nil
}

// GetTopicMetadata is primarily used to discover leaders for given topics and how many partitions these topics have.
// Passing it an empty topic list will retrieve metadata for all topics in a cluster.
func (c *KafkaClient) GetTopicMetadata(topics []string) (*MetadataResponse, error) {
	for i := 0; i <= c.config.MetadataRetries; i++ {
		if metadata, err := c.getMetadata(topics); err == nil {
			return metadata, nil
		}

		log.Debugf("GetTopicMetadata for %s failed after %d try", topics, i)
		time.Sleep(c.config.MetadataBackoff)
	}

	return nil, fmt.Errorf("Could not get topic metadata for %s after %d retries", topics, c.config.MetadataRetries)
}

// GetAvailableOffset issues an offset request to a specified topic and partition with a given offset time.
func (c *KafkaClient) GetAvailableOffset(topic string, partition int32, offsetTime int64) (int64, error) {
	request := new(OffsetRequest)
	request.AddPartitionOffsetRequestInfo(topic, partition, offsetTime, 1)
	response, err := c.sendToAllAndReturnFirstSuccessful(request, c.offsetValidator)
	if response != nil {
		return response.(*OffsetResponse).PartitionErrorAndOffsets[topic][partition].Offsets[0], err
	}

	return -1, err
}

// Fetch issues a single fetch request to a broker responsible for a given topic and partition and returns a FetchResponse that contains messages starting from a given offset.
func (c *KafkaClient) Fetch(topic string, partition int32, offset int64) (*FetchResponse, error) {
	response, err := c.tryFetch(topic, partition, offset)
	if err != nil {
		return response, err
	}

	if response.Error(topic, partition) == ErrNotLeaderForPartition {
		log.Infof("Sent a fetch reqest to a non-leader broker. Refreshing metadata for topic %s and retrying the request", topic)
		err = c.metadata.Refresh([]string{topic})
		if err != nil {
			return nil, err
		}
		response, err = c.tryFetch(topic, partition, offset)
	}

	return response, err
}

func (c *KafkaClient) tryFetch(topic string, partition int32, offset int64) (*FetchResponse, error) {
	brokerConnection, err := c.GetLeader(topic, partition)
	if err != nil {
		return nil, err
	}

	request := new(FetchRequest)
	request.MinBytes = c.config.FetchMinBytes
	request.MaxWait = c.config.FetchMaxWaitTime
	request.AddFetch(topic, partition, offset, c.config.FetchSize)
	bytes, err := c.syncSendAndReceive(brokerConnection, request)
	if err != nil {
		c.metadata.Invalidate(topic)
		return nil, err
	}

	decoder := NewBinaryDecoder(bytes)
	response := new(FetchResponse)
	decodingErr := response.Read(decoder)
	if decodingErr != nil {
		c.metadata.Invalidate(topic)
		log.Errorf("Could not decode a FetchResponse. Reason: %s", decodingErr.Reason())
		return nil, decodingErr.Error()
	}

	return response, nil
}

// GetOffset gets the offset for a given group, topic and partition from Kafka. A part of new offset management API.
func (c *KafkaClient) GetOffset(group string, topic string, partition int32) (int64, error) {
	log.Infof("Getting offset for group %s, topic %s, partition %d", group, topic, partition)
	coordinator, err := c.metadata.OffsetCoordinator(group)
	if err != nil {
		return InvalidOffset, err
	}

	request := NewOffsetFetchRequest(group)
	request.AddOffset(topic, partition)
	bytes, err := c.syncSendAndReceive(coordinator, request)
	if err != nil {
		return InvalidOffset, err
	}
	response := new(OffsetFetchResponse)
	decodingErr := c.decode(bytes, response)
	if decodingErr != nil {
		log.Errorf("Could not decode an OffsetFetchResponse. Reason: %s", decodingErr.Reason())
		return InvalidOffset, decodingErr.Error()
	}

	topicOffsets, exist := response.Offsets[topic]
	if !exist {
		return InvalidOffset, fmt.Errorf("OffsetFetchResponse does not contain information about requested topic")
	}

	if offset, exists := topicOffsets[partition]; !exists {
		return InvalidOffset, fmt.Errorf("OffsetFetchResponse does not contain information about requested partition")
	} else if offset.Error != ErrNoError {
		return InvalidOffset, offset.Error
	} else {
		return offset.Offset, nil
	}
}

// CommitOffset commits the offset for a given group, topic and partition to Kafka. A part of new offset management API.
func (c *KafkaClient) CommitOffset(group string, topic string, partition int32, offset int64) error {
	for i := 0; i <= c.config.CommitOffsetRetries; i++ {
		err := c.tryCommitOffset(group, topic, partition, offset)
		if err == nil {
			return nil
		}

		log.Debugf("Failed to commit offset %d for group %s, topic %s, partition %d after %d try: %s", offset, group, topic, partition, i, err)
		time.Sleep(c.config.CommitOffsetBackoff)
	}

	return fmt.Errorf("Could not get commit offset %d for group %s, topic %s, partition %d after %d retries", offset, group, topic, partition, c.config.CommitOffsetRetries)
}

// GetLeader returns a leader broker for a given topic and partition. Returns an error if fails to get leader for
// whatever reason for MetadataRetries retries.
func (c *KafkaClient) GetLeader(topic string, partition int32) (*BrokerConnection, error) {
	leader, err := c.metadata.Leader(topic, partition)
	if err != nil {
		leader, err = c.getLeaderRetryBackoff(topic, partition, c.config.MetadataRetries)
		if err != nil {
			return nil, err
		}
	}

	return c.metadata.Brokers.Get(leader), nil
}

// Close tells the Client to close all existing connections and stop.
// This method is NOT blocking but returns a channel which will be closed once the closing is finished.
func (c *KafkaClient) Close() <-chan struct{} {
	closed := make(chan struct{})
	go func() {
		c.bootstrapBrokers = nil
		close(closed)
	}()

	return closed
}

// Metadata returns Metadata structure used by this Client.
func (c *KafkaClient) Metadata() *Metadata {
	return c.metadata
}

func (c *KafkaClient) getMetadata(topics []string) (*MetadataResponse, error) {
	request := NewMetadataRequest(topics)
	brokerConnections := c.metadata.Brokers.GetAll()
	response, err := c.sendToAllBrokers(brokerConnections, request, c.topicMetadataValidator(topics))
	if err != nil {
		response, err = c.sendToAllBrokers(c.bootstrapBrokers, request, c.topicMetadataValidator(topics))
	}

	if response != nil {
		return response.(*MetadataResponse), err
	}

	return nil, err
}

func (c *KafkaClient) getLeaderRetryBackoff(topic string, partition int32, retries int) (int32, error) {
	var err error
	for i := 0; i <= retries; i++ {
		err = c.metadata.Refresh([]string{topic})
		if err != nil {
			continue
		}

		leader := int32(-1)
		leader, err = c.metadata.Leader(topic, partition)
		if err == nil {
			return leader, nil
		}

		time.Sleep(c.config.MetadataBackoff)
	}

	return -1, err
}

// GetConsumerMetadata returns a ConsumerMetadataResponse for a given consumer group.
// May return an error if fails to get consumer metadata for whatever reason within ConsumerMetadataRetries retries.
func (c *KafkaClient) GetConsumerMetadata(group string) (*ConsumerMetadataResponse, error) {
	for i := 0; i <= c.config.ConsumerMetadataRetries; i++ {
		metadata, err := c.tryGetConsumerMetadata(group)
		if err == nil {
			return metadata, nil
		}

		log.Debugf("Failed to get consumer coordinator for group %s after %d try: %s", group, i, err)
		time.Sleep(c.config.ConsumerMetadataBackoff)
	}

	return nil, fmt.Errorf("Could not get consumer coordinator for group %s after %d retries", group, c.config.ConsumerMetadataRetries)
}

func (c *KafkaClient) tryGetConsumerMetadata(group string) (*ConsumerMetadataResponse, error) {
	request := NewConsumerMetadataRequest(group)

	response, err := c.sendToAllAndReturnFirstSuccessful(request, c.consumerMetadataValidator)
	if err != nil {
		log.Infof("Could not get consumer metadata from all known brokers: %s", err)
		return nil, err
	}

	return response.(*ConsumerMetadataResponse), nil
}

func (c *KafkaClient) tryCommitOffset(group string, topic string, partition int32, offset int64) error {
	coordinator, err := c.metadata.OffsetCoordinator(group)
	if err != nil {
		return err
	}

	request := NewOffsetCommitRequest(group)
	request.AddOffset(topic, partition, offset, time.Now().Unix(), "")

	bytes, err := c.syncSendAndReceive(coordinator, request)
	if err != nil {
		return err
	}

	response := new(OffsetCommitResponse)
	decodingErr := c.decode(bytes, response)
	if decodingErr != nil {
		log.Errorf("Could not decode an OffsetCommitResponse. Reason: %s", decodingErr.Reason())
		return decodingErr.Error()
	}

	topicErrors, exist := response.CommitStatus[topic]
	if !exist {
		return fmt.Errorf("OffsetCommitResponse does not contain information about requested topic")
	}

	if partitionError, exist := topicErrors[partition]; !exist {
		return fmt.Errorf("OffsetCommitResponse does not contain information about requested partition")
	} else if partitionError != ErrNoError {
		return partitionError
	}

	return nil
}

func (c *KafkaClient) decode(bytes []byte, response Response) *DecodingError {
	decoder := NewBinaryDecoder(bytes)
	decodingErr := response.Read(decoder)
	if decodingErr != nil {
		log.Errorf("Could not decode a response. Reason: %s", decodingErr.Reason())
		return decodingErr
	}

	return nil
}

func (c *KafkaClient) sendToAllAndReturnFirstSuccessful(request Request, check func([]byte) (Response, error)) (Response, error) {
	c.lock.RLock()
	brokerConnection := c.metadata.Brokers.GetAll()
	if len(brokerConnection) == 0 {
		c.lock.RUnlock()
		err := c.metadata.Refresh(nil)
		if err != nil {
			return nil, err
		}
	} else {
		c.lock.RUnlock()
	}

	response, err := c.sendToAllBrokers(brokerConnection, request, check)
	if err != nil {
		response, err = c.sendToAllBrokers(c.bootstrapBrokers, request, check)
	}

	return response, err
}

func (c *KafkaClient) sendToAllBrokers(brokerConnections []*BrokerConnection, request Request, check func([]byte) (Response, error)) (Response, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if len(brokerConnections) == 0 {
		return nil, errors.New("Empty broker list")
	}

	responses := make(chan *rawResponseAndError, len(brokerConnections))
	for i := 0; i < len(brokerConnections); i++ {
		brokerConnection := brokerConnections[i]
		go func() {
			bytes, err := c.syncSendAndReceive(brokerConnection, request)
			responses <- &rawResponseAndError{bytes, brokerConnection, err}
		}()
	}

	var response *rawResponseAndError
	for i := 0; i < len(brokerConnections); i++ {
		response = <-responses
		if response.err == nil {
			checkResult, err := check(response.bytes)
			if err == nil {
				return checkResult, nil
			}

			response.err = err
		}
	}

	return nil, response.err
}

func (c *KafkaClient) syncSendAndReceive(broker *BrokerConnection, request Request) ([]byte, error) {
	conn, err := broker.GetConnection()
	if err != nil {
		return nil, err
	}

	id := c.metadata.Brokers.NextCorrelationID()
	if err = c.send(id, conn, request); err != nil {
		return nil, err
	}

	bytes, err := c.receive(conn)
	if err != nil {
		return nil, err
	}

	broker.ReleaseConnection(conn)
	return bytes, err
}

func (c *KafkaClient) send(correlationID int32, conn *net.TCPConn, request Request) error {
	writer := NewRequestHeader(correlationID, c.config.ClientID, request)
	bytes := make([]byte, writer.Size())
	encoder := NewBinaryEncoder(bytes)
	writer.Write(encoder)

	err := conn.SetWriteDeadline(time.Now().Add(c.config.WriteTimeout))
	if err != nil {
		return err
	}
	_, err = conn.Write(bytes)
	return err
}

func (c *KafkaClient) receive(conn *net.TCPConn) ([]byte, error) {
	err := conn.SetReadDeadline(time.Now().Add(c.config.ReadTimeout))
	if err != nil {
		return nil, err
	}

	header := make([]byte, 8)
	_, err = io.ReadFull(conn, header)
	if err != nil {
		return nil, err
	}

	decoder := NewBinaryDecoder(header)
	length, err := decoder.GetInt32()
	if err != nil {
		return nil, err
	}
	response := make([]byte, length-4)
	_, err = io.ReadFull(conn, response)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (c *KafkaClient) topicMetadataValidator(topics []string) func(bytes []byte) (Response, error) {
	return func(bytes []byte) (Response, error) {
		response := new(MetadataResponse)
		err := c.decode(bytes, response)
		if err != nil {
			return nil, err.Error()
		}

		if len(topics) > 0 {
			for _, topic := range topics {
				var topicMetadata *TopicMetadata
				for _, topicMetadata = range response.TopicsMetadata {
					if topicMetadata.Topic == topic {
						break
					}
				}

				if topicMetadata.Error != ErrNoError {
					return nil, topicMetadata.Error
				}

				for _, partitionMetadata := range topicMetadata.PartitionsMetadata {
					if partitionMetadata.Error != ErrNoError && partitionMetadata.Error != ErrReplicaNotAvailable {
						return nil, partitionMetadata.Error
					}
				}
			}
		}

		return response, nil
	}
}

func (c *KafkaClient) consumerMetadataValidator(bytes []byte) (Response, error) {
	response := new(ConsumerMetadataResponse)
	err := c.decode(bytes, response)
	if err != nil {
		return nil, err.Error()
	}

	if response.Error != ErrNoError {
		return nil, response.Error
	}

	return response, nil
}

func (c *KafkaClient) offsetValidator(bytes []byte) (Response, error) {
	response := new(OffsetResponse)
	err := c.decode(bytes, response)
	if err != nil {
		return nil, err.Error()
	}
	for _, offsets := range response.PartitionErrorAndOffsets {
		for _, offset := range offsets {
			if offset.Error != ErrNoError {
				return nil, offset.Error
			}
		}
	}

	return response, nil
}

type rawResponseAndError struct {
	bytes            []byte
	brokerConnection *BrokerConnection
	err              error
}
