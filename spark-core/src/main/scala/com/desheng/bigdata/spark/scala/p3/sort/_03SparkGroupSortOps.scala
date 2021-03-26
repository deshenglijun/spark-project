package com.desheng.bigdata.spark.scala.p3.sort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark分组排序
 */
object _03SparkGroupSortOps {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName("_03SparkGroupSortOps")
            .setMaster("local[2]")
        val sc = new SparkContext(conf)

        val lines = sc.parallelize(List(
                "chinese ls 91",
                "english ww 56",
                "chinese zs 90",
                "chinese zl 76",
                "english zq 88",
                "chinese wb 95",
                "chinese sj 74",
                "english ts 87",
                "english ys 67",
                "english mz 77",
                "chinese yj 98",
                "english gk 96"
                ))

        val course2Info:RDD[(String, String)] = lines.map(line => {
             val fields = line.split("\\s+")
             val course = fields(0)
             val name = fields(1)
             val score = fields(2)
             (course, name + "|" + score)
        })
        //分组
        val course2Infos:RDD[(String, Iterable[String])] = course2Info.groupByKey()

//        course2Infos.foreach{case (course, infos) => {
//            println(s"${course}\t${infos.mkString("[", ", ", "]")}")
//        }}
        /*
            分组排序
            排序前后的数据内容没有变化，仅仅是顺序变化，这里显然就应该用到map算子
         */
        course2Infos.map{case (course, infos) => {
            val sorted = infos.toList.sortWith((info1, info2) => {
                val score1 = info1.split("\\|")(1).toDouble
                val score2 = info2.split("\\|")(1).toDouble
                score1 > score2
            }).take(3)
            (course, sorted)
        }}.foreach{case (course, infos) => {
            println(s"${course}\t${infos.mkString("[", ", ", "]")}")
        }}

        sc.stop()
    }
}
