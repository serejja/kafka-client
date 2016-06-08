package client

import (
	"fmt"
	"math/rand"
	"net"
	"os"
	"testing"
	"time"
)

var ci = os.Getenv("TRAVIS_CI") != ""
var brokerUp = true
var brokerAddr = "localhost:9092"

func init() {
	conn, err := net.Dial("tcp", brokerAddr)
	if err == nil {
		brokerUp = true
		err = conn.Close()
		if err != nil {
			panic(err)
		}
	}
}

func TestKafkaClientFunctional(t *testing.T) {
	if !brokerUp && !ci {
		t.Skip("Broker is not running. Please spin up the broker at localhost:9092 for this test to work.")
	}

	numMessages := 1000
	topicName := fmt.Sprintf("kafka-client-%d", time.Now().Unix())

	kafkaClient := testClient(t)
	testTopicMetadata(t, topicName, kafkaClient)
	testOffsetStorage(t, topicName, kafkaClient)
	testProduce(t, topicName, numMessages, kafkaClient)
	testConsume(t, topicName, numMessages, kafkaClient)
	closeWithin(t, time.Second, kafkaClient)

	anotherClient := testClient(t)
	//should also work fine - must get topic metadata before consuming
	testConsume(t, topicName, numMessages, anotherClient)
	closeWithin(t, time.Second, anotherClient)
}

func testTopicMetadata(t *testing.T, topicName string, kafkaClient *KafkaClient) {
	metadata, err := kafkaClient.GetTopicMetadata([]string{topicName})
	assertFatal(t, err, nil)

	assertNot(t, len(metadata.Brokers), 0)
	assertNot(t, len(metadata.TopicsMetadata), 0)
	if len(metadata.Brokers) > 1 {
		t.Skip("Cluster should consist only of one broker for this test to run.")
	}

	broker := metadata.Brokers[0]
	assert(t, broker.ID, int32(0))
	if ci {
		// this can be asserted on Travis only as we are guaranteed to advertise the broker as localhost
		assert(t, broker.Host, "localhost")
	}
	assert(t, broker.Port, int32(9092))

	topicMetadata := findTopicMetadata(t, metadata.TopicsMetadata, topicName)
	assert(t, topicMetadata.Error, ErrNoError)
	assert(t, topicMetadata.Topic, topicName)
	assertFatal(t, len(topicMetadata.PartitionsMetadata), 1)

	partitionMetadata := topicMetadata.PartitionsMetadata[0]
	assert(t, partitionMetadata.Error, ErrNoError)
	assert(t, partitionMetadata.ISR, []int32{0})
	assert(t, partitionMetadata.Leader, int32(0))
	assert(t, partitionMetadata.PartitionID, int32(0))
	assert(t, partitionMetadata.Replicas, []int32{0})
}

func testOffsetStorage(t *testing.T, topicName string, kafkaClient *KafkaClient) {
	group := fmt.Sprintf("test-%d", time.Now().Unix())
	targetOffset := rand.Int63()

	offset, err := kafkaClient.GetOffset(group, topicName, 0)
	assertFatal(t, err, ErrUnknownTopicOrPartition)
	assert(t, offset, int64(-1))

	err = kafkaClient.CommitOffset(group, topicName, 0, targetOffset)
	assertFatal(t, err, nil)

	offset, err = kafkaClient.GetOffset(group, topicName, 0)
	assertFatal(t, err, nil)
	assert(t, offset, targetOffset)
}

func testProduce(t *testing.T, topicName string, numMessages int, kafkaClient *KafkaClient) {
	produceRequest := new(ProduceRequest)
	produceRequest.AckTimeoutMs = 1000
	produceRequest.RequiredAcks = 1
	for i := 0; i < numMessages; i++ {
		produceRequest.AddMessage(topicName, 0, &Message{
			Key:   []byte(fmt.Sprintf("%d", numMessages-i)),
			Value: []byte(fmt.Sprintf("%d", i)),
		})
	}

	leader, err := kafkaClient.GetLeader(topicName, 0)
	assert(t, err, nil)
	assertNot(t, leader, (*BrokerConnection)(nil))
	bytes, err := kafkaClient.syncSendAndReceive(leader, produceRequest)
	assertFatal(t, err, nil)

	produceResponse := new(ProduceResponse)
	decodingErr := kafkaClient.decode(bytes, produceResponse)
	assertFatal(t, decodingErr, (*DecodingError)(nil))

	topicBlock, exists := produceResponse.Status[topicName]
	assertFatal(t, exists, true)
	partitionBlock, exists := topicBlock[int32(0)]
	assertFatal(t, exists, true)

	assert(t, partitionBlock.Error, ErrNoError)
	assert(t, partitionBlock.Offset, int64(0))
}

func testConsume(t *testing.T, topicName string, numMessages int, kafkaClient *KafkaClient) {
	response, err := kafkaClient.Fetch(topicName, 0, 0)
	assertFatal(t, response.Error(topicName, 0), ErrNoError)
	assertFatal(t, err, nil)
	messages, err := response.GetMessages()
	assertFatal(t, err, nil)
	assertFatal(t, len(messages), numMessages)
	for i := 0; i < numMessages; i++ {
		message := messages[i]
		assert(t, message.Topic, topicName)
		assert(t, message.Partition, int32(0))
		assert(t, message.Offset, int64(i))
		assert(t, message.Key, []byte(fmt.Sprintf("%d", numMessages-i)))
		assert(t, message.Value, []byte(fmt.Sprintf("%d", i)))
	}
}

func findTopicMetadata(t *testing.T, metadata []*TopicMetadata, topic string) *TopicMetadata {
	for _, topicMetadata := range metadata {
		if topicMetadata.Topic == topic {
			return topicMetadata
		}
	}

	t.Fatalf("TopicMetadata for topic %s not found", topic)
	return nil
}
