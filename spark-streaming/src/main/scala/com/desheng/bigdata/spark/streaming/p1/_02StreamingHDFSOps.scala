package com.desheng.bigdata.spark.streaming.p1

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
 *  从hdfs读取数据：
 *      streaming只能做到读取一个目录下的新增文件，不能做到读取文件中的新增数据
 */
object _02StreamingHDFSOps {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setMaster("local")
            .setAppName("_01StreamingNetWorkOps")
        //两次流式计算之间的时间间隔，batchInterval
        val batchDuration = Seconds(2) // 每隔2s提交一次sparkstreaming的作业
        val ssc = new StreamingContext(conf, batchDuration)

        //加载外部数据
        val input = ssc.textFileStream("hdfs://ns1/data/spark/monitor/")
        val words: DStream[String] = input.flatMap(line => line.split("\\s+"))
        val pairs:DStream[(String, Int)] = words.map(word => (word, 1))
        val ret = pairs.reduceByKey(_+_)
        ret.print()


        ssc.start()
        ssc.awaitTermination()
    }
}
