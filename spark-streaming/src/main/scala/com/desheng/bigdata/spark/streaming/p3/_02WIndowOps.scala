package com.desheng.bigdata.spark.streaming.p3

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * window窗口操作
 *    流式无界，所以我们要向进行全局的统计肯定是行不通的，那么我们可以对这个无界的数据集进行切分，
 *    被切分的这每一个小的区间，我们可以理解为window，
 *    而sparkstreaming是准实时流计算，微小的批次操作，可以理解为是一个特殊的window窗口操作。
 *
 * 理论上，对这个窗口window的划分，有两种情况，一种就是按照数据的条数，另外一种就是按照时间。
 * 但是在sparkstreaming中目前仅支持后者，也就是说仅支持基于时间的窗口，需要提供两个参数
 * 一个参数是窗口的长度：window_length
 * 另外一个参数是窗口的计算频率：sliding_interval ，每隔多长时间计算一次window操作
 *
 * streaming程序中还有一个interval是batchInterval，那这两个interval有什么关系？
 * batchInterval，每隔多长时间启动一个spark作业，而是每隔多长时间为程序提交一批数据
 *
 * 特别需要注意的是：
 *  window_length和sliding_interval都必须是batchInterval的整数倍。
 *
 *  总结：
 *      window操作
 *          每隔M长的时间，去统计N长时间内产生的数据
 *          M被称之sliding_interval，窗口的滑动频率
 *          N被称之window_length，窗口的长度
 *     该window窗口是一个滑动的窗口。
 *
 *   当sliding_interval > window_length的时候，会出现窗口的空隙
 *   当sliding_interval < window_length的时候，会出现窗口的重合
 *   当sliding_interval = window_length的时候，两个窗口是严丝合缝的
 *
 *   batchInterval=2s
 *   sliding_interval=4s
 *   window_length=6s
 */
object _02WIndowOps {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setMaster("local[*]")
            .setAppName("_02WIndowOps")
        //两次流式计算之间的时间间隔，batchInterval
        val batchDuration = Seconds(2) // 每隔2s提交一次sparkstreaming的作业
        val ssc = new StreamingContext(conf, batchDuration)

        val lines = ssc.socketTextStream("bigdata01", 9999)
        val words = lines.flatMap(line => line.split("\\s+"))
        val pairs = words.map(word => (word, 1))
        val ret = pairs.reduceByKeyAndWindow(_+_, windowDuration = Seconds(6), slideDuration = Seconds(4))

        ret.print

        ssc.start()
        ssc.awaitTermination()
    }
}
