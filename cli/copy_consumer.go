package cli

import (
	"context"
	"errors"
	"fmt"
	"log"
	"regexp"
	"time"

	manager "github.com/rohilsurana/kafka-offset/kafka"
	"github.com/rohilsurana/sflags/gen/gpflag"
	"github.com/spf13/cobra"
)

type copyConsumerConfig struct {
	SourceKafka               *manager.Config
	TargetKafka               *manager.Config
	SourceConsumerGroupID     string         `desc:"kafka consumer group on source kafka"`
	TargetConsumerGroupID     string         `desc:"kafka consumer group on target kafka"`
	SkipConsumerLivenessCheck bool           `desc:"check to verify if all consumer instances are dead or not on the target kafka"`
	ConsumerLivenessPollTime  time.Duration  `desc:"time between each consumer liveness check"`
	Buffer                    time.Duration  `desc:"duration of buffer to add while copying consumer group offsets to target to account for any delay like due to mirroring"`
	TopicPattern              *regexp.Regexp `desc:"topic pattern to match list of topics for which the consumer groups offsets will be copied"`
	Execute                   bool           `desc:"perform execution to actually copy consumer offsets. If unset, only does a dry run"`
}

func cmdCopyConsumer(ctx context.Context) *cobra.Command {
	cfg := copyConsumerConfig{
		SourceKafka:               manager.GetDefaultConfig(),
		TargetKafka:               manager.GetDefaultConfig(),
		Execute:                   false,
		ConsumerLivenessPollTime:  10 * time.Second,
		SkipConsumerLivenessCheck: false,
		Buffer:                    0,
	}

	cmd := &cobra.Command{
		Use:     "copy-consumer",
		Short:   "Copy consumer from source kafka to target kafka",
		Aliases: []string{"copy", "cc"},
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		if cfg.SourceConsumerGroupID == "" {
			return errors.New("source consumer group id can't be empty")
		}

		if cfg.TargetConsumerGroupID == "" {
			return errors.New("target consumer group id can't be empty")
		}

		return copyConsumer(ctx, cfg)
	}

	err := gpflag.ParseTo(&cfg, cmd.Flags())
	if err != nil {
		log.Fatalf("err: %v", err)
	}

	return cmd
}

func copyConsumer(ctx context.Context, cfg copyConsumerConfig) error {
	skm, err := manager.NewManager(*cfg.SourceKafka)
	if err != nil {
		return err
	}

	tkm, err := manager.NewManager(*cfg.SourceKafka)
	if err != nil {
		return err
	}

	topicPartitionList, err := skm.GetTopicPartitionList(ctx, *cfg.TopicPattern)
	if err != nil {
		return err
	}

	if !cfg.Execute {
		fmt.Println("list of topics and partitions on source kafka:")
		printJSON(topicPartitionList)
	}

	consumerOffsets, err := skm.GetConsumerOffsets(ctx, cfg.SourceConsumerGroupID, topicPartitionList)
	if err != nil {
		return err
	}

	if !cfg.Execute {
		fmt.Println("current consumer offsets on source kafka:")
		printJSON(consumerOffsets)
	}

	offsetTimestamps, err := skm.GetOffsetTimestamps(ctx, consumerOffsets)
	if err != nil {
		panic(err)
	}

	if !cfg.Execute {
		fmt.Println("current offset timestamps from source kafka:")
		printJSON(offsetTimestamps)
	}

	for topic, partitionTimestamps := range offsetTimestamps {
		for partition, timestamp := range partitionTimestamps {
			offsetTimestamps[topic][partition] = timestamp.Add(-1 * cfg.Buffer)
		}
	}

	if !cfg.Execute {
		fmt.Println("planned topic offsets after using buffer:")
		printJSON(offsetTimestamps)
	}

	for !cfg.SkipConsumerLivenessCheck {
		doe, err := tkm.IsConsumerDeadOrEmpty(ctx, cfg.TargetConsumerGroupID)
		if err != nil {
			return err
		} else if doe {
			break
		}
		fmt.Println("consumer not dead or empty on target kafka")
		if !cfg.Execute {
			break
		}
		time.Sleep(10 * time.Second)
	}

	topicPartitionOffsets, offsetNotFoundTopics, err := tkm.GetTopicPartitionOffsetsForTimestampMapping(ctx, offsetTimestamps)
	if err != nil {
		return err
	}

	if len(offsetNotFoundTopics) > 0 {
		fmt.Println("offsets could not be found for these topics on target kafka:")
		printJSON(offsetNotFoundTopics)
	}

	if !cfg.Execute {
		fmt.Println("list of topic partition offsets on target kafka:")
		printJSON(topicPartitionOffsets)
	}

	if cfg.Execute {
		if err = tkm.SetConsumerOffsets(ctx, cfg.TargetConsumerGroupID, topicPartitionOffsets); err != nil {
			return err
		}
		fmt.Println("consumer copied to target kafka!")
	} else {
		fmt.Println("dry run complete")
	}
	return nil
}
