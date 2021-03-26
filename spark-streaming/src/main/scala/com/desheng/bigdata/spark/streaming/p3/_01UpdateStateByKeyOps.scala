package com.desheng.bigdata.spark.streaming.p3

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * DStream的updateStateByKey
 *  这个是一个状态算子，
 *      状态：说白了就是key所对应的value。
 *      state operator      ： 计算当前key对应value，不仅仅基于当前的纪录，还得要看历史的纪录
 *          统计截止到目前为止某一个key出现的次数
 *      none state operator ： 计算当前key对应value，仅仅基于当前的纪录
 *
 *      在sparkstreaming中状态算子涉及的的不多：updateStateByKey，xxxxWindow操作
 *      其他都是非状态算子，比如：map,flatMap,filter
 * updateStateByKey(updateFunc)
 *      按照key，来更新value的状态，所以key所对应的value，可能存在，可能不存在(当前key第一次出现的时候，其历史的value就不存在)
 *      统计截止到目前为止某一个key出现的次数
 *     该算子需要一个状态更新函数，来更新key对应的value，该函数需要至少两个参数
 *     updateFunc(currentValue:Seq[T], historyValue:Option[T])
 *
 * 为了持久化保存历史的状态，那么需要一个checkpoint存储目录来管理历史的状态值，在内存是相对不安全的。
 */
object _01UpdateStateByKeyOps {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setMaster("local[*]")
            .setAppName("_01UpdateStateByKeyOps")
        //两次流式计算之间的时间间隔，batchInterval
        val batchDuration = Seconds(2) // 每隔2s提交一次sparkstreaming的作业
        val ssc = new StreamingContext(conf, batchDuration)

        ssc.checkpoint("file:/E:/data/minitor/chk")

        val lines = ssc.socketTextStream("bigdata01", 9999)
        val words = lines.flatMap(line => line.split("\\s+"))

        val pairs = words.map(word => (word, 1))

        val ret = pairs.updateStateByKey(udpateFunc)

        ret.print()



        ssc.start()
        ssc.awaitTermination()
    }

    def udpateFunc(current: Seq[Int], history: Option[Int]):Option[Int] ={
//        val currentState = current.sum
//        val h = history.getOrElse(0)
//        Some(currentState + h)
//        Option(currentState + h)
        Option(current.sum + history.getOrElse(0))
    }
}
