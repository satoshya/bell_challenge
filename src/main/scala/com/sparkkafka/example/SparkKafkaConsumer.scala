package com.sparkkafka.example

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import java.io.PrintWriter
import scala.io.Source
import util.control.Breaks._
import org.apache.kafka.common.serialization.StringSerializer

import org.apache.spark.rdd.RDD
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.v09.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.producer._
import org.apache.spark.streaming.dstream.ConstantInputDStream

import java.util.ArrayList
import scala.util.Try
import scala.collection.JavaConversions._
import java.io.{ FileOutputStream=>FileStream, OutputStreamWriter=>StreamWriter }
import math._
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, CoordinateMatrix, MatrixEntry}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.clustering.BisectingKMeans
import org.apache.spark.mllib.clustering.KMeans

import org.apache.log4j.{Level,Logger}

import java.util.Properties

object SparkKafkaConsumer {

case class BellAlarm(name: String, items: Array[String])

def main(args: Array[String]) {

    Logger.getRootLogger.setLevel(Level.ERROR)

    val sparkConf = new SparkConf().setAppName("SparkKafkaConsumer")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val basefileName = "/tmp/file_to_be_trained.txt"
    val data = ssc.sparkContext.textFile("file:///tmp/file_to_be_trained.txt")
    val parsedData = data.map { s =>
        Vectors.dense(
            s.split(",").map(_.toDouble))
    }.cache()

    val numClusters = 3 // silence, randome noise, bell sound
    val numIterations = 20
    val clusters = KMeans.train(parsedData, numClusters, numIterations)
    println(s"cluster is ${clusters.k}")
    println(s"clustersCenter is ${clusters.clusterCenters}")
    val silentState = Vectors.dense(1.0,0.0)
    val bellRingState = Vectors.dense(1024.0,0.0)
    val temporalNoiseState = Vectors.dense(1024.0,899.0)
    val silentStateCluster = clusters.predict(silentState)
    val bellRingStateCluster = clusters.predict(bellRingState)
    val temporalNoiseStateCluster = clusters.predict(temporalNoiseState)
    println(s"silentState is ${silentStateCluster}")
    println(s"bellRingState is ${bellRingStateCluster}")
    println(s"temporalNoiseState is ${temporalNoiseStateCluster}")

    // MapR Stream settings
    val groupId = "testgroup"
    val offsetReset = "latest";
    val pollTimeout = "1000"
    val brokers = "dummy1:0.0.0.0,dummy2:0.0.0.0"  //dummy

    // Create Kafka producer
    val kafkaParams = Map[String, String](
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
        ConsumerConfig.GROUP_ID_CONFIG -> groupId,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> offsetReset,
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false", "spark.kafka.poll.time" -> pollTimeout
    )

    val lines = KafkaUtils.createDirectStream[String, String](ssc, kafkaParams, Set("/tmp/spark-test-stream:topic1"))

    lines.foreachRDD(
        rdd => {
            var count = 0
            var sum_absolute = 0.0
            var absolute_array = new Array[Int](20)
            rdd.foreach { row =>
                Try {
                    count += 1
                    sum_absolute += row._2.split(",")(3).split(":")(1).toInt
                    absolute_array(count-1) = row._2.split(",")(3).split(":")(1).toInt
                    if (count == 20) {
                        val encode = "UTF-8"
                        val append = true
                        val fileName1 = "/tmp/sound_check_log.csv"
                        val fileOutPutStream1 = new FileStream(fileName1, append)
                        val writer1 = new StreamWriter( fileOutPutStream1, encode )
                        val avg = sum_absolute / count
                        var abs_sum = absolute_array.map { abs_v =>
                            scala.math.pow(abs_v - sum_absolute/count, 2)
                        }.reduce (_ + _)
                        var s_dev = scala.math.sqrt(abs_sum / count)
                        writer1.write(avg + "," + s_dev + "\n")
                        writer1.close()
                        println(s"avg is ${avg} and standard deviation is ${s_dev}")

                        val currentState = Vectors.dense(avg, s_dev)
                        println(s"current data predict ${clusters.predict(currentState)} and bellRingStateCluster is ${bellRingStateCluster}")
                        if (bellRingStateCluster == clusters.predict(currentState)) {
                             val notificationFile = "/tmp/fire"
                             val fileOutputStream = new FileStream(notificationFile, append)
                             val writeFire = new StreamWriter (fileOutputStream, encode)
                             writeFire.write("fire notification")
                             writeFire.close()
                        }
                    }
                }
            }
        }
    )

    ssc.start()
    ssc.awaitTermination()
  }

}
