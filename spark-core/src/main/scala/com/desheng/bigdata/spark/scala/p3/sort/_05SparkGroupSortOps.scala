package com.desheng.bigdata.spark.scala.p3.sort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * Spark分组排序
 * 使用combineByKey对groupByKey进行优化
 * 第一步，完成代码改写
 * 第二部，彻底进行优化
 */
object _05SparkGroupSortOps {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName("_05SparkGroupSortOps")
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
        val course2Infos:RDD[(String, mutable.TreeSet[String])] = course2Info.combineByKey(createCombiner, mergeValue, mergeCombiners)

        course2Infos.foreach{case (course, infos) => {
            println(s"${course}\t${infos.mkString("[", ", ", "]")}")
        }}

        sc.stop()
    }
    def createCombiner(info: String): mutable.TreeSet[String] = {
        mutable.TreeSet(info)(new Ordering[String](){
            override def compare(x: String, y: String): Int = {
                val score1 = x.split("\\|")(1).toDouble
                val score2 = y.split("\\|")(1).toDouble
                score2.compareTo(score1)
            }
        })
    }
    def mergeValue(infos: mutable.TreeSet[String], info: String): mutable.TreeSet[String] = {
        infos.add(info)
        if(infos.size > 3) {
            infos.dropRight(1)
        } else {
            infos
        }
    }

    def mergeCombiners(infos: mutable.TreeSet[String], infos1: mutable.TreeSet[String]) : mutable.TreeSet[String] = {
        for(info <- infos1) {
            infos.add(info)
        }
        if(infos.size > 3) {
            infos.dropRight(infos.size - 3)
        } else {
            infos
        }
    }
}
