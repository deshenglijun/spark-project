package com.desheng.bigdata.spark.streaming.p2

import java.util

import com.desheng.bigdata.common.db.HBaseConnectionPool
import org.apache.hadoop.hbase.client.{Get, Put}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * SparkStreaming和kafka整合，基于hbase进行手动管理offset
 *  编程步骤
 *      1. 先到hbase中读取上一次存储（某一个消费者组的某一个topic对应的某一个partition的）offset
 *      2. 如果读取到了offset，那么就从指定的offset位置开始消费数据
 *      3. 如果没有读取到offset，那么久从最早的位置开始消费数据
 *      4. 读取到数据之后进行业务处理
 *      5. 业务处理完毕之后再更新offset回hbase
 *
 *  hbase操作：
 *      create_namespace 'ns_0817'
 *      create 'ns_0817:spark-kafka-group-0817', 'cf'
 */
object _01StreamingWithKafkaOffsetManageOps {

    def main(args: Array[String]): Unit = {
//        Logger.getLogger("org.apache.spark").setLevel(Level.DEBUG)
        val conf = new SparkConf()
            .setMaster("local[*]")
            .setAppName("_01StreamingWithKafkaOffsetManageOps")
        //两次流式计算之间的时间间隔，batchInterval
        val batchDuration = Seconds(2) // 每隔2s提交一次sparkstreaming的作业
        val ssc = new StreamingContext(conf, batchDuration)

        val topics = Set("hadoop")
        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> "bigdata01:9092,bigdata02:9092,bigdata03:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "ns_0817:spark-kafka-group-0817",
            "auto.offset.reset" -> "earliest",
            "enable.auto.commit" -> "false"
        )
        //从kafka中创建数据集
        val messages: InputDStream[ConsumerRecord[String, String]] = createMsg(ssc, topics, kafkaParams)
        messages.foreachRDD((rdd, bTime) => {
            if(!rdd.isEmpty()) {
                println("-------------------------------------------")
                println(s"Time: $bTime")
                println("-------------------------------------------")
                //step 4 读取到数据之后进行业务处理
                println("##############rdd'count: " + rdd.count())//

                //step 5. 业务处理完毕之后再更新offset回hbase
                storeOffset(rdd.asInstanceOf[HasOffsetRanges].offsetRanges, kafkaParams("group.id").toString)
            }
        })
        ssc.start()
        ssc.awaitTermination()
    }

    def storeOffset(offsetRanges: Array[OffsetRange], group: String): Unit = {
        val connection = HBaseConnectionPool.getConnection
        val table = HBaseConnectionPool.getTable(connection, group)
        val puts = new util.ArrayList[Put]()
        for(offsetRange <- offsetRanges) {
            val partition = offsetRange.partition
            val topic = offsetRange.topic
            val offset = offsetRange.untilOffset
            val put = new Put(topic.getBytes())
            put.addColumn("cf".getBytes(), (partition + "").getBytes(), (offset + "").getBytes())
            puts.add(put)
        }
        table.put(puts)
        table.close()
        HBaseConnectionPool.release(connection)
    }

    def createMsg(ssc: StreamingContext, topics: Set[String], kafkaParams: Map[String, Object]): InputDStream[ConsumerRecord[String, String]] = {
        //step 1. 从hbase中读取offset
        val offsets:Map[TopicPartition, Long] = getOffsetsFromHBase(topics, kafkaParams("group.id").toString)
        var messages: InputDStream[ConsumerRecord[String, String]] = null
        //step 2 和 3，根据读取到的offset的有无，来从kafka中读取数据
        if(offsets.isEmpty) {//从最开始的位置读取数据
            messages = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
                ConsumerStrategies.Subscribe(topics, kafkaParams))
        } else {//从指定的offset位置读取数据
            messages = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
                ConsumerStrategies.Subscribe(topics, kafkaParams, offsets))
        }
        messages
    }

    /*
        step 1. 从hbase中读取offset
        namespace
            table
                rk   columnFamily column value
            某一个消费者组的某一个topic对应的某一个partition的offset

            让value成为offset
            列族：cf
            rk  --->topic
            column --->partition
            table --->group
     */
    def getOffsetsFromHBase(topics: Set[String], group: String):Map[TopicPartition, Long] = {
        val connection = HBaseConnectionPool.getConnection
        val table = HBaseConnectionPool.getTable(connection, group)
        val map = mutable.Map[TopicPartition, Long]()
        for(topic <- topics) {
            val result = table.get(new Get(topic.getBytes()))
            if(!result.isEmpty) {
                val par2Offsets = result.getFamilyMap("cf".getBytes())
                for((pBytes, offsetBytes) <- par2Offsets) {
                    val partition = new String(pBytes).toInt
                    val offset = new String(offsetBytes).toLong
                    map.put(new TopicPartition(topic, partition), offset)
                }
            }
        }
        table.close()
        HBaseConnectionPool.release(connection)
        map.toMap
    }
}
