package com.desheng.bigdata.spark.scala.p2

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object _07SparkPersistOps {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName(s"${_07SparkPersistOps.getClass.getSimpleName}")
            .setMaster("local[*]")

        val sc = new SparkContext(conf)
        //读取外部数据
        var start = System.currentTimeMillis()
        val lines = sc.textFile("file:///E:/data/spark/core/sequences.txt")
        var count = lines.count()
        println("没有持久化：#######lines' count: " + count + ", cost time: " + (System.currentTimeMillis() - start) + "ms")
        lines.persist(StorageLevel.DISK_ONLY) //lines.cache()
        start = System.currentTimeMillis()
        count = lines.count()
        println("持久化之后：#######lines' count: " + count + ", cost time: " + (System.currentTimeMillis() - start) + "ms")
        lines.unpersist()//卸载持久化数据
        sc.stop()
    }
}