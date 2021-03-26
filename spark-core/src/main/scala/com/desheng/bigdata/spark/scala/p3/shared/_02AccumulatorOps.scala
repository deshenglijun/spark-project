package com.desheng.bigdata.spark.scala.p3.shared

import org.apache.spark.{SparkConf, SparkContext}

/**
 * spark的累加器操作
 */
object _02AccumulatorOps {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName("_02AccumulatorOps")
            .setMaster("local[*]")
        val sc = new SparkContext(conf)

        val listRDD = sc.parallelize(
            List(
                "a second spark a spark is shared second",
                "spark shared be shared in second spark"
            )
        )

        //需求是统计每一个单词出现次数
        val ret = listRDD.flatMap(_.split("\\s+")).map((_, 1)).reduceByKey(_+_)

        ret.foreach(println)
        println("---------------只需要统计spark出现了多少次------------------")
        //现在只需要统计spark出现了多少次
        ret.filter{case (word, count) => word == "spark"}.foreach(println)
        sc.stop()
    }
}
