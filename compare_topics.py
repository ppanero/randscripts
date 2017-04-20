#!/usr/bin/env python
import argparse
import subprocess
import time
import os
import signal
import sys

# Add shutdown hook for stoping process when finishing the monitoring

def signal_handler(signal, frame):
    print('\nYou killed the monitoring so I\'ll kill your consumers!')
    for consumer in consumers:
    	print("Killing consumer with PID {0}".format(consumer.pid))
    	consumer.kill()

    sys.exit(0)

# Function to obtain the current sum of log end offsets per topic
def get_offsets_from_topics(topics):
	consumer_group_offsets = subprocess.check_output(
	[
	"{0}{1}".format(args.kafka_home, "/bin/kafka-run-class.sh"),
	"kafka.admin.ConsumerGroupCommand",
	"--bootstrap-server", 
	args.bootstrap_server,
	"--command-config",
	"{0}/config/{1}".format(args.kafka_home, args.command_config),
	"--group",
	args.group,
	"--describe"
	],
	stderr=FNULL)

	lines = consumer_group_offsets.split('\n')
	offsets = {}
	for topic in topics:
		offsets[topic] = 0
	for line in lines:
		for topic in topics:
			if topic in line:
				offsets[topic] = int(offsets[topic]) + int(line.split()[3])
	return offsets

signal.signal(signal.SIGINT, signal_handler)

# Argument parsing
parser = argparse.ArgumentParser(description='Monitor Kafka - Compare topics processing')

parser.add_argument('--kafka-home',
                     help='Directory where kafka bin folder is placed')
parser.add_argument('--bootstrap-server',
                     help='Kafka bootstrap server including port (e.j. kafka-broker.something.ch:9093)')
parser.add_argument('--command-config',
                     help='Property file containing configs to be passed to Admin lient and Consumer. The same file is used to create the consumers.')
parser.add_argument('--group',
                     help='Kafka consumer group (has to be the same than established in the file passed to --command-config)')
parser.add_argument('--topics',  metavar='topic', nargs='+',
                     help='Topics to compare')
parser.add_argument('--interval',  type=int,
                     help='Monitoring interval in seconds')

args = parser.parse_args()
	
print("Starting monitoring for topics {0}".format(args.topics))

# Need to open the consumer for each topic. Please note that in general the fact of opening consumers 
# (and also taking the measure) sequentially might result in some small error in the topic's progress
# kafka.tools.GetOffsetShell does not support brokers over SSL. Therefore we need to opt for 
# consumerGroupCommand which needs consumers

# Opening consumers

FNULL = open(os.devnull, 'w') # To avoid the stdout of the process to be displayed
consumers = []
offset_dict = {}
for topic in args.topics:
	consumers.append(subprocess.Popen(
		[
		"{0}{1}".format(args.kafka_home, "/bin/kafka-console-consumer.sh"),
		"--bootstrap-server", 
		args.bootstrap_server,
		"--consumer.config",
		"{0}/config/{1}".format(args.kafka_home, args.command_config),
		"--topic",
		topic,
		], 
		stdout=FNULL, stderr=subprocess.STDOUT))
	offset_dict[topic] = 0

initial_offsets = get_offsets_from_topics(args.topics)
previous_offsets = initial_offsets
print("------------------------------Offset diff------------------------------")
while True:

	current_offsets = get_offsets_from_topics(args.topics)
	for key in initial_offsets.keys():
		print("Progress for topic {0} is overall {1} since last update {2}".format(key, 
			current_offsets.get(key) - initial_offsets.get(key),
			current_offsets.get(key) - previous_offsets.get(key),
		))
	previous_offsets = current_offsets
	print("-----------------------------------------------------------------------")
	time.sleep(10)



