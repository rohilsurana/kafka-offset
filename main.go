package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	"github.com/AlecAivazis/survey/v2"
	manager "github.com/rohilsurana/kafka-offset/kafka"
)

var qs = []*survey.Question{
	{
		Name: "brokers",
		Prompt: &survey.Input{
			Message: "Comma separated kafka broker string:",
			Default: "localhost:9092",
		},
	},
	{
		Name: "consumer_id",
		Prompt: &survey.Input{
			Message: "Consumer group to move:",
		},
		Validate: survey.Required,
	},
	{
		Name:     "topic_pattern",
		Prompt:   &survey.Input{Message: "Kafka topic pattern regex:"},
		Validate: survey.Required,
	},
	{
		Name:     "timestamp",
		Prompt:   &survey.Input{Message: "Timestamp in milliseconds to move the consumer offsets to:"},
		Validate: survey.Required,
	},
	{
		Name: "dry_run",
		Prompt: &survey.Confirm{
			Message: "Perform a dry run to print change details:",
			Default: true,
		},
	},
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	answers := struct {
		Brokers      string `survey:"brokers"`
		ConsumerID   string `survey:"consumer_id"`
		TopicPattern string `survey:"topic_pattern"`
		Timestamp    int64  `survey:"timestamp"`
		DryRun       bool   `survey:"dry_run"`
	}{}

	err := survey.Ask(qs, &answers)
	if err != nil {
		panic(err)
	}

	km, err := manager.NewManager(answers.Brokers, 60)
	if err != nil {
		panic(err)
	}

	for {
		doe, err := km.IsConsumerDeadOrEmpty(ctx, answers.ConsumerID)
		if err != nil {
			panic(err)
		} else if doe {
			break
		}
		fmt.Println("consumer not dead or empty")
		if answers.DryRun {
			break
		}
		time.Sleep(10 * time.Second)
	}

	topicPartitionList, err := km.GetTopicPartitionList(ctx, answers.TopicPattern)
	if err != nil {
		panic(err)
	}

	if answers.DryRun {
		fmt.Println("list of topics and partitions:")
		printJSON(topicPartitionList)
	}

	consumerOffsets, err := km.GetConsumerOffsets(ctx, answers.ConsumerID, topicPartitionList)
	if err != nil {
		panic(err)
	}

	if answers.DryRun {
		fmt.Println("current consumer offsets:")
		printJSON(consumerOffsets)
	}

	offsetTimestamps, err := km.GetOffsetTimestamps(ctx, consumerOffsets)
	if err != nil {
		panic(err)
	}

	if answers.DryRun {
		fmt.Println("current offset timestamps:")
		printJSON(offsetTimestamps)
	}

	topicPartitionOffsets, err := km.GetTopicPartitionOffsetsForTimestamp(ctx, topicPartitionList, answers.Timestamp)
	if err != nil {
		panic(err)
	}

	if answers.DryRun {
		fmt.Println("list of offsets:")
		printJSON(topicPartitionOffsets)
	}

	if !answers.DryRun {
		if err = km.MoveConsumerOffsets(ctx, answers.ConsumerID, topicPartitionOffsets); err != nil {
			panic(err)
		}
		fmt.Println("consumer offsets are set!")
	} else {
		fmt.Println("dry run complete")
	}
}

func printJSON(v any) {
	j, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(j))
	fmt.Println("")
}
