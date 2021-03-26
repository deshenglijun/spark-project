package com.desheng.bigdata.spark.streaming.p1

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * SparkStreaming的入门案例
 *      通过网络socket来模拟实时不断产生数据并处理
 *  linux的安装：
 *      yum -y installl nc
 *  StreamingContext剖析：
 *      local       ：是当前程序分配一个工作线程
 *      local[*]    ：给当前程序分配可以用工作线程（通常会>=2）
 *   这个改动造成程序只接收数据，而不进行处理，那么也就意味着，streaming需要使用线程资源既要接收数据，还有消费数据，而且优先接收数据，接收数据要独占一个线程字段。
 *
 *   在本地模式下面，去处理receiver到的数据的时候，线程资源最好设置大于1。
 *
 */
object _01StreamingNetWorkOps {
    def main(args: Array[String]): Unit = {

        val conf = new SparkConf()
            .setMaster("local[*]")
            .setAppName("_01StreamingNetWorkOps")
        //两次流式计算之间的时间间隔，batchInterval
        val batchDuration = Seconds(2) // 每隔2s提交一次sparkstreaming的作业
        val ssc = new StreamingContext(conf, batchDuration)

        //加载外部数据
        val input:ReceiverInputDStream[String] = ssc.socketTextStream("bigdata01", 9999,
            storageLevel = StorageLevel.MEMORY_AND_DISK_SER_2)

        val words: DStream[String] = input.flatMap(line => line.split("\\s+"))

        val pairs:DStream[(String, Int)] = words.map(word => (word, 1))

        val ret = pairs.reduceByKey(_+_)

        ret.print()
        /*
            start操作，是用来启动streaming程序的计算，如果不执行start操作，程序压根儿不会执行
         */
        ssc.start()
        /*
            Adding new inputs, transformations, and output operations after starting a context is not supported
            在程序启动之后，也就是start之后，不可以再添加任何的业务逻辑
         */
//        ret.print()
        /*
            如果没有awaitTermination方法，程序不会持续不断的运行，说白就是把driver设置成为一个后台的守护线程
         */
        ssc.awaitTermination()
        println("--------termination-------")
    }
}
