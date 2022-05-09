package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"regexp"
	"time"

	manager "github.com/rohilsurana/kafka-offset/kafka"
	"github.com/rohilsurana/sflags/gen/gpflag"
	"github.com/spf13/cobra"
)

type resetConsumerConfig struct {
	SourceKafka               *manager.Config
	ConsumerGroupID           string         `desc:"kafka consumer group"`
	SkipConsumerLivenessCheck bool           `desc:"check to verify if all consumer instances are dead or not"`
	ConsumerLivenessPollTime  time.Duration  `desc:"time between each consumer liveness check"`
	Before                    time.Duration  `desc:"duration to seek consumer back in time"`
	TopicPattern              *regexp.Regexp `desc:"topic pattern to match list of topics for which the consumer groups offsets will be moved"`
	Execute                   bool           `desc:"perform execution to actually reset consumer offsets. If unset, only does a dry run"`
}

func cmdResetConsumer(ctx context.Context) *cobra.Command {
	cfg := resetConsumerConfig{
		SourceKafka:               manager.GetDefaultConfig(),
		Execute:                   false,
		ConsumerLivenessPollTime:  10 * time.Second,
		SkipConsumerLivenessCheck: false,
	}

	cmd := &cobra.Command{
		Use:     "reset-consumer",
		Short:   "reset consumer to a given timestamp for provided topic pattern",
		Aliases: []string{"reset", "rc"},
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		if cfg.ConsumerGroupID == "" {
			return errors.New("consumer group id can't be empty")
		}

		if cfg.Before.Nanoseconds() == 0 {
			return errors.New("set before to duration greater than 0s")
		}

		return resetConsumer(ctx, cfg)
	}

	err := gpflag.ParseTo(&cfg, cmd.Flags())
	if err != nil {
		log.Fatalf("err: %v", err)
	}

	return cmd
}

func resetConsumer(ctx context.Context, cfg resetConsumerConfig) error {
	km, err := manager.NewManager(*cfg.SourceKafka)
	if err != nil {
		return err
	}

	seekTime := time.Now().Add(-1 * cfg.Before)

	for !cfg.SkipConsumerLivenessCheck {
		doe, err := km.IsConsumerDeadOrEmpty(ctx, cfg.ConsumerGroupID)
		if err != nil {
			return err
		} else if doe {
			break
		}
		fmt.Println("consumer not dead or empty")
		if !cfg.Execute {
			break
		}
		time.Sleep(10 * time.Second)
	}

	topicPartitionList, err := km.GetTopicPartitionList(ctx, *cfg.TopicPattern)
	if err != nil {
		return err
	}

	if !cfg.Execute {
		fmt.Println("list of topics and partitions:")
		printJSON(topicPartitionList)
	}

	consumerOffsets, err := km.GetConsumerOffsets(ctx, cfg.ConsumerGroupID, topicPartitionList)
	if err != nil {
		return err
	}

	if !cfg.Execute {
		fmt.Println("current consumer offsets:")
		printJSON(consumerOffsets)
	}

	topicPartitionOffsets, err := km.GetTopicPartitionOffsetsForTimestamp(ctx, topicPartitionList, seekTime.UnixMilli())
	if err != nil {
		return err
	}

	if !cfg.Execute {
		fmt.Println("list of offsets:")
		printJSON(topicPartitionOffsets)
	}

	if cfg.Execute {
		if err = km.MoveConsumerOffsets(ctx, cfg.ConsumerGroupID, topicPartitionOffsets); err != nil {
			return err
		}
		fmt.Println("consumer offsets are set!")
	} else {
		fmt.Println("dry run complete")
	}
	return nil
}

func printJSON(v any) {
	j, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(j))
	fmt.Println("")
}
