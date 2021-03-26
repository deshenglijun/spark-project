package com.desheng.bigdata.spark.streaming.p1

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
    SparkStremaing从kafka中读取数据
        从指定的offset位置来读取数据
 */
object _04StreamingFromKafkaOps {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setMaster("local")
            .setAppName("_04StreamingFromKafkaOps")
        //两次流式计算之间的时间间隔，batchInterval
        val batchDuration = Seconds(2) // 每隔2s提交一次sparkstreaming的作业
        val ssc = new StreamingContext(conf, batchDuration)


        val topics = Set("hadoop")
        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> "bigdata01:9092,bigdata02:9092,bigdata03:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "spark-kafka-group-0817",
            "auto.offset.reset" -> "earliest",
            "enable.auto.commit" -> "false"
        )

//0 449 1 449 2 445
        val offsets = Map[TopicPartition, Long](
            new TopicPartition("hadoop", 0) -> 451,
            new TopicPartition("hadoop", 1) -> 452,
            new TopicPartition("hadoop", 2) -> 447
        )
        val message:InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
                                                                    ConsumerStrategies.Subscribe(topics, kafkaParams, offsets))
//        message.print()//直接打印有序列化问题

        message.foreachRDD((rdd, bTime) => {
            if(!rdd.isEmpty()) {
                println("-------------------------------------------")
                println(s"Time: $bTime")
                println("-------------------------------------------")
                rdd.foreach(record => {
                    println(record)
                })
            }
        })


        ssc.start()
        ssc.awaitTermination()
    }
}
