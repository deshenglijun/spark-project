package com.desheng.bigdata

import com.desheng.bigdata.common.db.HBaseConnectionPool
import org.apache.hadoop.hbase.client.Get
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable
import scala.collection.JavaConversions._

object HBaseTest {
    def main(args: Array[String]): Unit = {
        val topics = Set("hadoop")
        val group = "ns_0817:spark-kafka-group-0817"
        val map = getOffsetsFromHBase(topics, group)
        for((topicPartition, offset) <- map) {
            println(s"topic: ${topicPartition.topic()}, partition: ${topicPartition.partition()}, offset: ${offset}")
        }
    }

    def getOffsetsFromHBase(topics: Set[String], group: String):Map[TopicPartition, Long] = {
        val connection = HBaseConnectionPool.getConnection
        val table = HBaseConnectionPool.getTable(connection, group)
        val map = mutable.Map[TopicPartition, Long]()
        for(topic <- topics) {
            val result = table.get(new Get(topic.getBytes()))
            if(!result.isEmpty) {
                val cells = result.listCells()
                val par2Offsets = result.getFamilyMap("cf".getBytes())
                for((pBytes, offsetBytes) <- par2Offsets) {
                    val partition = new String(pBytes).toInt
                    val offset = new String(offsetBytes).toLong
                    map.put(new TopicPartition(topic, partition), offset)
                }
            }
        }
        HBaseConnectionPool.release(connection)
        map.toMap
    }
}
