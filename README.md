## kafka-offset

This project aims to simplify accessing kafka offset details easily and 
moving offsets of a consumer to offsets near a specific timestamp.

## How to use

```
kafka-offset tool can be used to manipulate kafka consumer group offsets

Usage:
  kafka-offset [command]

Available Commands:
  completion     Generate the autocompletion script for the specified shell
  copy-consumer  copy consumer from source kafka to target kafka
  help           Help about any command
  reset-consumer reset consumer to a given timestamp for provided topic pattern

Flags:
  -h, --help   help for kafka-offset

Use "kafka-offset [command] --help" for more information about a command.
```

Example -
```sh
# to reset consumer to 10 min before current time 
kafka-offset reset-consumer \
  --before 10m \
  --topic-pattern test-topic  \
  --source-consumer-group-id test-kafka-consumer-id  \
  --source-kafka-brokers localhost:9092 \
  --execute # if omitted will result in a dry-run
  
# to copy consumer to another kafka with a different name, and additional time buffer to account for delays due to mirroring etc
kafka-offset copy-consumer \
  --buffer 30s \
  --topic-pattern test-topic \
  --source-consumer-group-id p-godata-id-test-offset-copy-firehose-0001 \
  --source-kafka-brokers localhost:9092 \
  --target-consumer-group-id p-godata-id-test-offset-copy-firehose-0003 \
  --target-kafka-brokers localhost:9093 \
  --execute # if omitted will result in a dry-run
```

### Understanding how it works

On high level we have the consumer group id, list of topics or pattern,
and brokers. We call the brokers to get list of partitions for all the matching topics.
For each topic-partition we query the offset available in [TimeIndex](#timeindex) just after given timestamp.

### Kafka Indexes

There are 3 different types of indexes in kafka -

- **TimeIndex** - index of record timestamps and it's offsets (sparse,
each offset is not tracked)
- **OffsetIndex** - index of record offset to segment file
- **TransactionIndex** - index of metadata of aborted transactions

For scope of this project we will deal only with TimeIndex.

### TimeIndex

TimeIndex uses the max record timestamp of a batch and it's offset.
It does not contain entry for timestamps for all offsets.

The timestamp used depends on the record timestamps. Hence, it could be either CreateTime or LogAppendTime.
