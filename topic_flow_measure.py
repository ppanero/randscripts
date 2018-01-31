#!/usr/bin/env python
import argparse, re, subprocess, time, os, signal, sys
from datetime import datetime

# Add shutdown hook for stoping process when finishing the monitoring

def signal_handler(signal, frame):
	print("Killing consumer with PID {0}".format(consumer.pid))
	consumer.kill()
	sys.exit(0)


# Function to obtain the current sum of log end offsets per topic
def get_offsets_from_topics(topic):
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
	stderr=-1)
	
        lines = consumer_group_offsets.split('\n')
	offsets = {}
	for line in lines:
		if re.match("^{0} ".format(topic), line) is not None:
			offsets[line.split()[1]] = int(line.split()[3])
	return offsets

signal.signal(signal.SIGINT, signal_handler)

# Argument parsing
parser = argparse.ArgumentParser(description='Monitor Kafka - Topic Flow Measure')

parser.add_argument('--kafka-home',
                     help='Directory where kafka bin folder is placed')
parser.add_argument('--bootstrap-server',
                     help='Kafka bootstrap server including port (e.j. kafka-broker.something.ch:9093)')
parser.add_argument('--command-config',
                     help='Property file containing configs to be passed to Admin lient and Consumer. The same file is used to create the consumers.')
parser.add_argument('--group',
                     help='Kafka consumer group (has to be the same than established in the file passed to --command-config)')
parser.add_argument('--topic',  metavar='topic',
                     help='Topic')

args = parser.parse_args()

print("Opening consumer for topic {0}".format(args.topic))
consumer = subprocess.Popen(
		[
		"{0}{1}".format(args.kafka_home, "/bin/kafka-console-consumer.sh"),
		"--bootstrap-server", 
		args.bootstrap_server,
		"--consumer.config",
		"{0}/config/{1}".format(args.kafka_home, args.command_config),
		"--topic",
		args.topic,
		"--group",
		args.group
		],
		stdout=-1, stderr=subprocess.STDOUT)

print("Starting flow measurements for topic {0}".format(args.topic))

starting_offsets = get_offsets_from_topics(args.topic)
starting_time = datetime.now()

print("First sample taken...")
time.sleep(5)

ending_offsets = get_offsets_from_topics(args.topic)
ending_time = datetime.now()
print("Second sample taken...")
print("Calculating...")
offsets_diff = 0
if len(starting_offsets) != len(ending_offsets):
	print("ERROR: First sample has {0} offsets and second sample has {1}".format(len(starting_offsets), len(ending_offsets)))
	exit(1)

print(starting_offsets)
print(ending_offsets)
for key,offset in starting_offsets.iteritems():
	offsets_diff = offsets_diff + (int(ending_offsets[key]) - int(offset))

time_elapsed = ending_time - starting_time
print(time_elapsed)
print("\n")
print("{0} processed in {1} seconds".format(offsets_diff, time_elapsed.seconds))
print("Average {0} messages/second".format(offsets_diff/time_elapsed.seconds))
consumer.kill()
sys.exit(0)
