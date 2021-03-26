package com.desheng.bigdata.spark.scala.p1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * scala版本的wordcount
 */
object RemoteScalaWordCountApp {
    def main(args: Array[String]): Unit = {
        if(args == null || args.length != 2) {
            println(
                """
                  |Usage: <input> <output>
                  |""".stripMargin)
            System.exit(-1)
        }

        val Array(input, output) = args

        val conf = new SparkConf()
            .setAppName(s"${RemoteScalaWordCountApp.getClass.getSimpleName}")
        val sc = new SparkContext(conf)
        val lines: RDD[String] = sc.textFile(input)//加载hdfs的文件

        println("lines rdd's partition is: " + lines.getNumPartitions)
        //calc word's count
        val words:RDD[String] = lines.flatMap(line => line.split("\\s+"))
        val pairs:RDD[(String, Int)] = words.map(word => (word, 1))
        val ret:RDD[(String, Int)] = pairs.reduceByKey(myReduceFunc)

        //export data to external system
//        ret.foreach{case (word, count) => {
//            println(word + "--->" + count)
//        }}
        ret.saveAsTextFile(output)

        sc.stop()
    }
    def myReduceFunc(v1: Int, v2: Int): Int = {
//        val i = 1 / 0 //为了验证lazy
        v1 + v2
    }
}
