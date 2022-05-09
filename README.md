## kafka-offset

This project aims to simplify accessing kafka offset details easily and 
moving offsets of a consumer to offsets near a specific timestamp.

## How to use

```sh
$ 
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
