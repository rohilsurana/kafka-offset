package kafka

import (
	"context"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/topics"
	"golang.org/x/exp/maps"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const HOSTNAME_REGEX = `^([a-zA-Z0-9]{1}[a-zA-Z0-9_-]{0,62}){1}(\.[a-zA-Z0-9_]{1}[a-zA-Z0-9_-]{0,62})*?$`

type manager struct {
	client *kafka.Client
}

type partitionOffset struct {
	PartitionID int
	Offset      int64
}

func NewManager(brokers string, timeout int64) (*manager, error) {
	brokerList := strings.Split(brokers, ",")
	for _, broker := range brokerList {
		if !isHostnamePort(broker) {
			return nil, errors.New("invalid brokers string")
		}
	}

	timeoutDuration := time.Duration(timeout * int64(time.Second))
	transport := &kafka.Transport{
		Dial:        (&net.Dialer{Timeout: timeoutDuration}).DialContext,
		DialTimeout: timeoutDuration,
		IdleTimeout: timeoutDuration,
		MetadataTTL: timeoutDuration,
		ClientID:    kafka.DefaultClientID,
		Resolver:    kafka.NewBrokerResolver(nil),
	}

	client := &kafka.Client{
		Addr:      kafka.TCP(brokers),
		Transport: transport,
		Timeout:   timeoutDuration,
	}

	return &manager{
		client: client,
	}, nil
}

func (m manager) GetTopicPartitionList(ctx context.Context, topicPattern string) (map[string][]int, error) {
	topicsMap := map[string][]int{}

	topicRegex, err := regexp.Compile(topicPattern)
	if err != nil {
		return nil, fmt.Errorf("invalid topic pattern: %w", err)
	}

	ts, err := topics.ListRe(ctx, m.client, topicRegex)
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

func (m manager) IsConsumerDeadOrEmpty(ctx context.Context, consumerID string) (bool, error) {
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
	} else {
		return false, nil
	}
}

func (m manager) GetTopicPartitionOffsetsForTimestamp(ctx context.Context, topicPartitions map[string][]int, timestampMs int64) (map[string]map[int]int64, error) {
	topicPartitionOffsets := map[string]map[int]int64{}
	offsetRequest := map[string][]kafka.OffsetRequest{}

	for t, partitions := range topicPartitions {
		for _, partition := range partitions {
			offsetRequest[t] = append(offsetRequest[t], kafka.OffsetRequest{
				Partition: partition,
				Timestamp: timestampMs,
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

func (m manager) MoveConsumerOffsets(ctx context.Context, consumerID string, topicPartitionOffsets map[string]map[int]int64) error {
	topicList := maps.Keys(topicPartitionOffsets)

	if consumerID == "" {
		return errors.New("invalid consumer group id")
	}

	group, err := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
		ID:      consumerID,
		Topics:  topicList,
		Brokers: []string{m.client.Addr.String()},
		Dialer:  kafka.DefaultDialer,
		Timeout: m.client.Timeout,
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
		return regexp.MustCompile(HOSTNAME_REGEX).MatchString(host)
	}
	return true
}
