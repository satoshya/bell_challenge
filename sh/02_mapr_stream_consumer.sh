#!/bin/zsh

/opt/mapr/spark/spark-1.6.1/bin/spark-submit --master local\[2\] --class com.sparkkafka.example.SparkKafkaConsumer ./streams_demo2-1.0.jar
