# Randscripts
Random scripts/small programs needed for some task

## topics_compare.py 

Enables to check the progress of a kafka topic using ConsumerGroupCommand. This is needed because GetOffsetShell does not support SSL brokers.

Example call:

```bash
python compare_topics.py --kafka-home /path/to/kafka_2.11-0.10.2.0 --bootstrap-server  some-kafka-broker.ch:9093 --command-config consumer.properties --group kafka-console-group --topics topic1 topic2
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
