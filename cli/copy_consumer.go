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
	ConsumerGroupID           string         `desc:"kafka consumer group"`
	SkipConsumerLivenessCheck bool           `desc:"check to verify if all consumer instances are dead or not on the target kafka"`
	ConsumerLivenessPollTime  time.Duration  `desc:"time between each consumer liveness check"`
	Buffer                    time.Duration  `desc:"duration of buffer to add while copying consumer group offsets to target to account for any delay like due to mirroring"`
	TopicPattern              *regexp.Regexp `desc:"topic pattern to match list of topics for which the consumer groups offsets will be moved"`
	Execute                   bool           `desc:"perform execution to actually reset consumer offsets. If unset, only does a dry run"`
}

func cmdCopyConsumer(ctx context.Context) *cobra.Command {
	cfg := copyConsumerConfig{
		SourceKafka:               manager.GetDefaultConfig(),
		TargetKafka:               manager.GetDefaultConfig(),
		Execute:                   false,
		ConsumerLivenessPollTime:  10 * time.Second,
		SkipConsumerLivenessCheck: false,
		Buffer:                    1 * time.Minute,
	}

	cmd := &cobra.Command{
		Use:     "copy-consumer",
		Short:   "copy consumer from source kafka to target kafka",
		Aliases: []string{"copy", "cc"},
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		if cfg.ConsumerGroupID == "" {
			return errors.New("consumer group id can't be empty")
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

	consumerOffsets, err := skm.GetConsumerOffsets(ctx, cfg.ConsumerGroupID, topicPartitionList)
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
		doe, err := tkm.IsConsumerDeadOrEmpty(ctx, cfg.ConsumerGroupID)
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

	topicPartitionOffsets, err := tkm.GetTopicPartitionOffsetsForTimestampMapping(ctx, offsetTimestamps)
	if err != nil {
		return err
	}

	if !cfg.Execute {
		fmt.Println("list of topic partition offsets on target kafka:")
		printJSON(topicPartitionOffsets)
	}

	if cfg.Execute {
		if err = tkm.MoveConsumerOffsets(ctx, cfg.ConsumerGroupID, topicPartitionOffsets); err != nil {
			return err
		}
		fmt.Println("consumer copied to target kafka!")
	} else {
		fmt.Println("dry run complete")
	}
	return nil
}
