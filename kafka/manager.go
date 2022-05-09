package kafka

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/topics"
	"golang.org/x/exp/maps"
)

const hostnameRegex = `^([a-zA-Z0-9]{1}[a-zA-Z0-9_-]{0,62}){1}(\.[a-zA-Z0-9_]{1}[a-zA-Z0-9_-]{0,62})*?$`

type Config struct {
	Brokers          string
	DialTimeout      time.Duration
	IdleTimeout      time.Duration
	MetadataTTL      time.Duration
	ResponseTimeout  time.Duration
	ResponseMinBytes int64
	ResponseMaxBytes int64
	KafkaClientID    string
}

type Manager struct {
	client *kafka.Client
	config Config
}

func GetDefaultConfig() *Config {
	programName := filepath.Base(os.Args[0])
	hostname, _ := os.Hostname()

	return &Config{
		DialTimeout:      500 * time.Millisecond,
		IdleTimeout:      500 * time.Millisecond,
		MetadataTTL:      5 * time.Second,
		ResponseTimeout:  10 * time.Second,
		ResponseMinBytes: 1,
		ResponseMaxBytes: 1e6,
		KafkaClientID:    fmt.Sprintf("%s@%s (github.com/rohilsurana/kafka-offset)", programName, hostname),
	}
}

func NewManager(config Config) (*Manager, error) {
	brokerList := strings.Split(config.Brokers, ",")
	for _, broker := range brokerList {
		if !isHostnamePort(broker) {
			return nil, errors.New("invalid brokers string")
		}
	}

	transport := &kafka.Transport{
		Dial:        (&net.Dialer{Timeout: config.DialTimeout}).DialContext,
		DialTimeout: config.DialTimeout,
		IdleTimeout: config.IdleTimeout,
		MetadataTTL: config.MetadataTTL,
		ClientID:    config.KafkaClientID,
		Resolver:    kafka.NewBrokerResolver(nil),
	}

	client := &kafka.Client{
		Addr:      kafka.TCP(brokerList...),
		Transport: transport,
		Timeout:   config.ResponseTimeout,
	}

	return &Manager{
		client: client,
		config: config,
	}, nil
}

func (m Manager) GetTopicPartitionList(ctx context.Context, topicRegex regexp.Regexp) (map[string][]int, error) {
	topicsMap := map[string][]int{}

	ts, err := topics.ListRe(ctx, m.client, &topicRegex)
	if err != nil {
		return nil, fmt.Errorf("unable to fetch topics list: %w", err)
	}

	for _, t := range ts {
		for _, partition := range t.Partitions {
			topicsMap[t.Name] = append(topicsMap[t.Name], partition.ID)
		}
	}

	return topicsMap, nil
}

func (m Manager) IsConsumerDeadOrEmpty(ctx context.Context, consumerID string) (bool, error) {
	if consumerID == "" {
		return false, errors.New("invalid consumer group id")
	}

	res, err := m.client.DescribeGroups(ctx, &kafka.DescribeGroupsRequest{
		GroupIDs: []string{consumerID},
	})
	if err != nil {
		return false, fmt.Errorf("errors fetching consumer group status: %w", err)
	}

	if len(res.Groups) < 1 {
		return true, nil
	}

	if res.Groups[0].GroupState == "Dead" || res.Groups[0].GroupState == "Empty" {
		return true, nil
	}

	return false, nil
}

func (m Manager) GetTopicPartitionOffsetsForTimestampMapping(ctx context.Context, topicPartitionTimestamps map[string]map[int]time.Time) (map[string]map[int]int64, error) {
	topicPartitionOffsets := map[string]map[int]int64{}
	offsetRequest := map[string][]kafka.OffsetRequest{}

	for t, partitionTimestamps := range topicPartitionTimestamps {
		for partition, timestamp := range partitionTimestamps {
			offsetRequest[t] = append(offsetRequest[t], kafka.OffsetRequest{
				Partition: partition,
				Timestamp: timestamp.UnixMilli(),
			})
		}
	}

	offsets, err := m.client.ListOffsets(ctx, &kafka.ListOffsetsRequest{
		Topics: offsetRequest,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to fetch offsets for given topic partitions: %w", err)
	}

	for topic, offset := range offsets.Topics {
		topicPartitionOffsets[topic] = map[int]int64{}

		for _, po := range offset {
			offsetList := maps.Keys(po.Offsets)
			topicPartitionOffsets[topic][po.Partition] = offsetList[0]

			for _, o := range offsetList {
				if o < topicPartitionOffsets[topic][po.Partition] {
					topicPartitionOffsets[topic][po.Partition] = o
				}
			}
		}
	}

	return topicPartitionOffsets, nil
}

func (m Manager) GetTopicPartitionOffsetsForTimestamp(ctx context.Context, topicPartitions map[string][]int, timestampMs int64) (map[string]map[int]int64, error) {
	topicPartitionTimestamps := map[string]map[int]time.Time{}

	for t, partitions := range topicPartitions {
		topicPartitionTimestamps[t] = map[int]time.Time{}
		for _, partition := range partitions {
			topicPartitionTimestamps[t][partition] = time.UnixMilli(timestampMs)
		}
	}

	return m.GetTopicPartitionOffsetsForTimestampMapping(ctx, topicPartitionTimestamps)
}

func (m Manager) GetConsumerOffsets(ctx context.Context, consumerID string, topics map[string][]int) (map[string]map[int]int64, error) {
	consumerOffsets := map[string]map[int]int64{}
	offsetFetchResponse, err := m.client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
		GroupID: consumerID,
		Topics:  topics,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to fetch consumer offsets: %w", err)
	}

	for topic, partitions := range offsetFetchResponse.Topics {
		consumerOffsets[topic] = map[int]int64{}
		for _, partition := range partitions {
			consumerOffsets[topic][partition.Partition] = partition.CommittedOffset
		}
	}

	return consumerOffsets, nil
}

func (m Manager) GetOffsetTimestamps(ctx context.Context, topicPartitionOffsets map[string]map[int]int64) (map[string]map[int]time.Time, error) {
	topicOffsetTimestamps := map[string]map[int]time.Time{}
	topicOffsetTimestampsC := map[string]*sync.Map{}
	wg := sync.WaitGroup{}

	for topic, partitionOffsets := range topicPartitionOffsets {
		topicOffsetTimestampsC[topic] = &sync.Map{}
		for partition, offset := range partitionOffsets {
			wg.Add(1)
			go func(topic string, partition int, offset int64) {
				defer wg.Done()

				for {
					fetchResponse, err := m.client.Fetch(ctx, &kafka.FetchRequest{
						Topic:     topic,
						Partition: partition,
						Offset:    offset,
						MinBytes:  m.config.ResponseMinBytes,
						MaxBytes:  m.config.ResponseMaxBytes,
						MaxWait:   m.config.ResponseTimeout,
					})
					if err != nil {
						return
					}

					minTime := time.Unix(math.MaxInt64, 0)
					recCount := 0
					for {
						rec, err := fetchResponse.Records.ReadRecord()
						if err != nil {
							break
						}
						recCount++
						if rec.Time.Unix() < minTime.Unix() {
							minTime = rec.Time
						}
					}
					if recCount == 0 {
						minTime = time.Unix(0, 0)
					}

					topicOffsetTimestampsC[topic].Store(partition, minTime)
					if offset < 0 || recCount > 0 {
						break
					}
				}
			}(topic, partition, offset)
		}
	}
	wg.Wait()

	for topic, partitionOffsets := range topicOffsetTimestampsC {
		topicOffsetTimestamps[topic] = map[int]time.Time{}
		partitionOffsets.Range(func(partition, timestamp any) bool {
			topicOffsetTimestamps[topic][partition.(int)] = timestamp.(time.Time)
			return true
		})
	}

	return topicOffsetTimestamps, nil
}

func (m Manager) MoveConsumerOffsets(ctx context.Context, consumerID string, topicPartitionOffsets map[string]map[int]int64) error {
	topicList := maps.Keys(topicPartitionOffsets)

	if consumerID == "" {
		return errors.New("invalid consumer group id")
	}

	group, err := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
		ID:      consumerID,
		Topics:  topicList,
		Brokers: []string{m.client.Addr.String()},
		Dialer: &kafka.Dialer{
			ClientID:  m.config.KafkaClientID,
			Timeout:   m.config.ResponseTimeout,
			DualStack: true,
		},
		Timeout: m.config.ResponseTimeout,
	})
	if err != nil {
		return fmt.Errorf("unable to create consumer instance: %w", err)
	}

	defer group.Close()

	gen, err := group.Next(ctx)
	if err != nil {
		return fmt.Errorf("unable to create consumer instance generation: %w", err)
	}

	err = gen.CommitOffsets(topicPartitionOffsets)
	if err != nil {
		return fmt.Errorf("unable to commit offsets for consumer: %w", err)
	}
	return nil
}

func isHostnamePort(val string) bool {
	host, port, err := net.SplitHostPort(val)
	if err != nil {
		return false
	}

	if portNum, err := strconv.ParseInt(port, 10, 32); err != nil || portNum > 65535 || portNum < 1 {
		return false
	}

	if host != "" {
		return regexp.MustCompile(hostnameRegex).MatchString(host)
	}
	return true
}
