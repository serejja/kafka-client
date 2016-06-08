package client

import (
	"fmt"
	"github.com/yanzay/log"
	"sort"
	"sync"
	"time"
)

// Metadata is a helper structure that provides access to topic/partition leaders and offset coordinators, caches them,
// refreshes and invalidates when necessary.
type Metadata struct {
	Brokers         *Brokers
	kafka           Client
	metadataTTL     time.Duration
	metadata        map[string]map[int32]int32
	metadataExpires map[string]time.Time
	metadataLock    sync.RWMutex

	//offset coordination part
	offsetCoordinators    map[string]int32
	offsetCoordinatorLock sync.RWMutex
}

// NewMetadata creates a new Metadata for a given Client and Brokers with metadata cache TTL set to metadataTTL.
func NewMetadata(kafkaClient Client, brokers *Brokers, metadataTTL time.Duration) *Metadata {
	return &Metadata{
		Brokers:            brokers,
		kafka:              kafkaClient,
		metadataTTL:        metadataTTL,
		metadata:           make(map[string]map[int32]int32),
		metadataExpires:    make(map[string]time.Time),
		offsetCoordinators: make(map[string]int32),
	}
}

// Leader tries to get a leader for a topic and partition from cache. Automatically refreshes if the leader information
// is missing or expired. May return an error if fails to get a leader for whatever reason.
func (m *Metadata) Leader(topic string, partition int32) (int32, error) {
	topicMetadata, err := m.TopicMetadata(topic)
	if err != nil {
		return -1, err
	}

	leader, exists := topicMetadata[partition]
	if !exists {
		err := m.Refresh([]string{topic})
		if err != nil {
			m.Invalidate(topic)
			return -1, err
		}

		topicMetadata, err = m.TopicMetadata(topic)
		if err != nil {
			m.Invalidate(topic)
			return -1, err
		}

		leader, exists = topicMetadata[partition]
		if !exists {
			m.Invalidate(topic)
			return -1, ErrFailedToGetLeader
		}
	}

	return leader, nil
}

// TopicMetadata returns a map where keys are partitions of a topic and values are leader broker IDs.
// Automatically refreshes metadata if it is missing or expired.
// May return an error if fails to to get metadata for whatever reason.
func (m *Metadata) TopicMetadata(topic string) (map[int32]int32, error) {
	topicMetadata, ttl := m.topicMetadata(topic)

	if topicMetadata == nil || ttl.Add(m.metadataTTL).Before(time.Now()) {
		err := m.refreshIfExpired(topic, ttl)
		if err != nil {
			return nil, err
		}

		topicMetadata, _ = m.topicMetadata(topic)
		if topicMetadata != nil {
			return topicMetadata, nil
		}

		return nil, ErrFailedToGetMetadata
	}

	return topicMetadata, nil
}

// PartitionsFor returns a sorted slice of partitions for a given topic.
// Automatically refreshes metadata if it is missing or expired.
// May return an error if fails to to get metadata for whatever reason.
func (m *Metadata) PartitionsFor(topic string) ([]int32, error) {
	topicMetadata, err := m.TopicMetadata(topic)
	if err != nil {
		return nil, err
	}

	partitions := make([]int32, 0, len(topicMetadata))
	for partition := range topicMetadata {
		partitions = append(partitions, partition)
	}

	sort.Sort(int32Slice(partitions))
	return partitions, nil
}

// Refresh forces metadata refresh for given topics.
// If the argument is empty or nil, metadata for all known topics by a Kafka cluster will be requested.
// May return an error if fails to to get metadata for whatever reason.
func (m *Metadata) Refresh(topics []string) error {
	log.Infof("Refreshing metadata for topics %v", topics)
	m.metadataLock.Lock()
	defer m.metadataLock.Unlock()

	return m.refresh(topics)
}

// Invalidate forcibly invalidates metadata cache for a given topic so that next time it is requested
// it is guaranteed to be refreshed.
func (m *Metadata) Invalidate(topic string) {
	m.metadataLock.Lock()
	defer m.metadataLock.Unlock()

	m.metadataExpires[topic] = time.Unix(0, 0)
}

// OffsetCoordinator returns a BrokerConnection for an offset coordinator for a given group ID.
// May return an error if fails to to get metadata for whatever reason.
func (m *Metadata) OffsetCoordinator(group string) (*BrokerConnection, error) {
	m.offsetCoordinatorLock.RLock()
	coordinatorID, exists := m.offsetCoordinators[group]
	m.offsetCoordinatorLock.RUnlock()

	if !exists {
		err := m.refreshOffsetCoordinator(group)
		if err != nil {
			return nil, err
		}

		m.offsetCoordinatorLock.RLock()
		coordinatorID = m.offsetCoordinators[group]
		m.offsetCoordinatorLock.RUnlock()
	}

	brokerConnection := m.Brokers.Get(coordinatorID)
	if brokerConnection == nil {
		return nil, fmt.Errorf("Could not find broker with node id %d", coordinatorID)
	}

	return brokerConnection, nil
}

func (m *Metadata) refreshOffsetCoordinator(group string) error {
	m.offsetCoordinatorLock.Lock()
	defer m.offsetCoordinatorLock.Unlock()

	metadata, err := m.kafka.GetConsumerMetadata(group)
	if err != nil {
		return err
	}
	m.offsetCoordinators[group] = metadata.Coordinator.ID

	return nil
}

func (m *Metadata) topicMetadata(topic string) (map[int32]int32, time.Time) {
	m.metadataLock.RLock()
	defer m.metadataLock.RUnlock()

	metadataCopy := make(map[int32]int32)
	for k, v := range m.metadata[topic] {
		metadataCopy[k] = v
	}
	return metadataCopy, m.metadataExpires[topic]
}

func (m *Metadata) refreshIfExpired(topic string, ttl time.Time) error {
	m.metadataLock.Lock()
	defer m.metadataLock.Unlock()

	if ttl.Add(m.metadataTTL).Before(time.Now()) {
		return m.refresh([]string{topic})
	}

	return nil
}

func (m *Metadata) refresh(topics []string) error {
	topicMetadataResponse, err := m.kafka.GetTopicMetadata(topics)
	if err != nil {
		return err
	}

	for _, broker := range topicMetadataResponse.Brokers {
		m.Brokers.Update(broker)
	}

	for _, topicMetadata := range topicMetadataResponse.TopicsMetadata {
		if topicMetadata.Error != ErrNoError {
			return topicMetadata.Error
		}

		partitionLeaders := make(map[int32]int32)
		for _, partitionMetadata := range topicMetadata.PartitionsMetadata {
			if partitionMetadata.Error != ErrNoError {
				return partitionMetadata.Error
			}

			partitionLeaders[partitionMetadata.PartitionID] = partitionMetadata.Leader
		}

		m.metadata[topicMetadata.Topic] = partitionLeaders
		m.metadataExpires[topicMetadata.Topic] = time.Now()
		log.Debugf("Received metadata: partitions %v for topic %s", partitionLeaders, topicMetadata.Topic)
	}

	return nil
}

type int32Slice []int32

func (p int32Slice) Len() int           { return len(p) }
func (p int32Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p int32Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
