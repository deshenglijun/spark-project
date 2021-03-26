package com.desheng.bigdata.spark.streaming.p3

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
    transform,是一个transformation操作
        transform(p:(RDD[A]) => RDD[B]):DStream[B]
     类似的操作
        foreachRDD(p: (RDD[A]) => Unit)
     transform的一个非常重要的一个操作，就是来构建DStream中没有的操作，DStream的大多数操作都可以用transform来模拟
     比如map(p: (A) => B) ---> transform(rdd => rdd.map(p: (A) => B))

   最重要的操作：
      dstream.join(rdd)

   在线黑名单过滤
        一个离线的数据集RDD和一个在线的数据集DStream之间的join操作

   数据格式：
    27.19.74.143##2016-05-30 17:38:20##GET /static/image/common/faq.gif HTTP/1.1##200##1127
   黑名单数据：
        27.19.74.143
        110.52.250.126
 */
object _03TransformOps {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setMaster("local[*]")
            .setAppName("_03TransformOps")
        //两次流式计算之间的时间间隔，batchInterval
        val batchDuration = Seconds(2) // 每隔2s提交一次sparkstreaming的作业
        val ssc = new StreamingContext(conf, batchDuration)

        //数据的重分区
//        lines.repartition()//增大分区
//        lines.transform(rdd => rdd.coalesce())
        //key是黑名单标识ip，value为0或者1,0标识已经被解禁，但是还没有进行删除，1标识为启动该黑名单数据
        val blacklist:RDD[(String, Int)] = ssc.sparkContext.parallelize(List(
            "27.19.74.143" -> 1,
            "110.52.250.126" -> 1,
            "8.35.201.163" -> 0
        ))

        val lines = ssc.socketTextStream("bigdata01", 9999)

        val ip2Info = lines.transform(rdd => {
            rdd.map(line => {
                val index = line.indexOf("##")
                val ip = line.substring(0, index)
                val info = line.substring(index + 2)
                (ip, info)
            })
        })
        //黑名单过滤操作 因为dstream的join需要的数据类型是dstream，所以二者是无法直接进行join的
        val filteredIp2Info = ip2Info.transform(rdd => {
            val joinedRDD:RDD[(String, (String, Option[Int]))] = rdd.leftOuterJoin(blacklist)
            //进行数据过滤
            //第一步说明右表没有关联成功，第二部说明关联成功，但是解禁了
            val filtered = joinedRDD.filter{case (ip, (info, flag)) => {
                 flag.isEmpty || flag.get == 0
            }}

            filtered.map{case (ip, (info, flag)) => {
                (ip, info)
            }}
        })

        filteredIp2Info.print()


        ssc.start()
        ssc.awaitTermination()

    }
}
