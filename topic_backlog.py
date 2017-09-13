#!/usr/bin/env python
import argparse
import subprocess
import time
import os
import json
from datetime import datetime


# Argument parsing
parser = argparse.ArgumentParser(description='Monitor Kafka - Compare topics processing')

parser.add_argument('--kafka-home',
                     help='Directory where kafka bin folder is placed')
parser.add_argument('--bootstrap-server',
                     help='Kafka bootstrap server including port (e.j. kafka-broker.something.ch:9093)')
parser.add_argument('--command-config',
                     help='Property file containing configs to be passed to Admin client and Consumer. Please note that if there is a consumer group the timestamp will not be accurate')
parser.add_argument('--topics',  metavar='topic', nargs='+',
                     help='Topic to check')
parser.add_argument('-n',  metavar='nmesg',
                     help='Number of messages to get the timestamp from')

args = parser.parse_args()
	
print("Checking backlog for topics {0}".format(args.topics))

# Need to open the consumer for each topic. Please note that in general the fact of opening consumers 
# (and also taking the measure) sequentially might result in some small error in the topic's progress
# kafka.tools.GetOffsetShell does not support brokers over SSL. Therefore we need to opt for 
# consumerGroupCommand which needs consumers

# Opening consumers

FNULL = open(os.devnull, 'w') # To avoid the stdout of the process to be displayed

print("---------------------Timestamp diff----------------------")
for topic in args.topics:
	consumed_value = subprocess.check_output(
		[
		"{0}{1}".format(args.kafka_home, "/bin/kafka-console-consumer.sh"),
		"--bootstrap-server", 
		args.bootstrap_server,
		"--consumer.config",
		"{0}/config/{1}".format(args.kafka_home, args.command_config),
		"--topic",
		topic,
		"--max-messages",
		str(args.n),
		"--new-consumer"
		],
		stderr=FNULL)
	dtts = datetime.fromtimestamp(json.loads(consumed_value)['timestamp']/1000)
	ts = dtts.strftime("%Y-%m-%dT%H:%M:%S")
	print("|\t {0} \t|\t{1}\t|".format(topic, ts))
	
print("---------------------------------------------------------")
