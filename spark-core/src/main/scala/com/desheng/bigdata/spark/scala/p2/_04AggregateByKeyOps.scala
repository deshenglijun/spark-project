package com.desheng.bigdata.spark.scala.p2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * combineByKey和aggregateByKey的区别就相当于reduceByKey和foldByKey
 */
object _04AggregateByKeyOps {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName("_04AggregateByKeyOps")
            .setMaster("local[*]")
        val sc = new SparkContext(conf)

//        abk2rbk(sc)
        abk2gbk(sc)
        sc.stop()
    }
    def abk2gbk(sc: SparkContext): Unit = {
        case class Student(id: Int, name:String, province: String)
        val stuRDD = sc.parallelize(List(
            Student(1, "唐玉峰", "安徽"),
            Student(11, "王世伟", "安徽"),
            Student(3, "胡国权", "甘肃"),
            Student(44, "old李", "甘肃"),
            Student(47, "董卓", "甘肃"),
            Student(5, "马惠", "黑吉辽"),
            Student(55, "刘龙沛", "黑吉辽"),
            Student(2, "李梦", "安徽"),
            Student(4, "陈延年", "甘肃"),
            Student(10086, "刘炳文", "黑吉辽")
        ), 2).mapPartitionsWithIndex((index, partition) => {
            val list = partition.toList
            println(s"-->stuRDD的分区编号为<${index}>中的数据为：${list.mkString("[", ", ", "]")}")
            list.toIterator
        })

        val pairs = stuRDD.map(stu => (stu.province, stu))
        def seqOp(stus: Array[Student], stu: Student): Array[Student] = {
            stus.+:(stu)
        }
        //全局聚合
        def combOp(stus: Array[Student], stuI: Array[Student]): Array[Student] = {
            stus ++ stuI
        }
        val province2Infos: RDD[(String, Array[Student])] = pairs.aggregateByKey(Array[Student]())(seqOp, combOp)

        province2Infos.foreach{case (province, stus) => {
            println(s"province2Infos>>> 省份：${province}, 学生信息：${stus.mkString(", ")}, 人数：${stus.size}")
        }}
    }


    def abk2rbk(sc: SparkContext): Unit = {
        val array = sc.parallelize(Array(
            "hello you",
            "hello me",
            "hello you",
            "hello you",
            "hello me",
            "hello you"
        ), 2)

        val pairs = array.flatMap(_.split("\\s+")).map((_, 1))

        val ret = pairs.aggregateByKey(0)(_+_, _+_)

        ret.foreach{case (key, count) => {
            println(s"key: ${key}, count: ${count}")
        }}
    }

}
