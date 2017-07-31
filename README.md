# Randscripts
Random scripts/small programs needed for some task

## topic_compare.py 

Enables to check the progress of a kafka topic using ConsumerGroupCommand. This is needed because GetOffsetShell does not support SSL brokers.

Example call:

```bash
python topic_compare.py --kafka-home /path/to/kafka_2.11-0.10.2.0 --bootstrap-server  some-kafka-broker.ch:9093 --command-config consumer.properties --group kafka-console-group --topics topic1 topic2
```

Example output:

```bash
Starting monitoring for topics ['topic1', 'topic2']
------------------------------Offset diff------------------------------
Progress for Topic topic1 is overall 807 since last update 807
Progress for Topic topic2 is overall 0 since last update 0
-----------------------------------------------------------------------
Progress for Topic topic1 is overall 5391 since last update 4584
Progress for Topic topic2 is overall 0 since last update 0
-----------------------------------------------------------------------
^C
You killed the monitoring so I'll kill your consumers!
Killing consumer with PID 27177
Killing consumer with PID 27178
```

## topic_backlog.py 

Enables to check the backlog by taking the timestamp of the consumed message of a kafka topic using kafka-consumer.sh.

Example call:

```bash
python topic_backlog.p --kafka-home /path/to/kafka_2.11-0.10.2.0 --bootstrap-server  some-kafka-broker.ch:9093 --command-config consumer.properties --topics topic1 topic2 topic3 -n 1
```

Note that the consumer.config must NOT have any consumer group configured so the new consumer created are not stucked in old consumer queues with old timestamps.

Example output:

```bash
Checking backlog for topics ['topic1', 'topic2', 'topci3']
---------------------Timestamp diff----------------------
|    topic1             |   2017-07-31T15:31:23Z        |
|    topic2             |   2017-07-31T15:31:17Z        |
|    topci3             |   2017-07-31T12:04:57Z        |
---------------------------------------------------------
```

