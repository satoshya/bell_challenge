#!/bin/zsh

while :; do
  tail -40 /tmp/stream_data | grep data | sed 's/data://' | /opt/mapr/kafka/kafka-0.9.0/bin/kafka-console-producer.sh --broker-list 1:1 --topic /tmp/spark-test-stream:topic1
  sleep 5
done
