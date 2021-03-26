package com.desheng.bigdata.spark.scala.p3.sort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * spark中的一个排序操作
 *  普通的排序
 *  TopN
 *  二次排序
 *  分组排序
 *
 *  排序的算子主要有：
 *    transformation:
 *          sortByKey
 *              按照key进行排序，默认升序，对应的数据类型是K-V
 *              ascending         升序(true)或者降序(false)
 *              numPartitions     排序之后的分区个数
 *          sortBy
 *              可以不用有key来进行排序，但是需要指定进行排序的字段，底层其实就是
 *              map(data => (K, data)).sortBykey()
 *          区别就是groupByKey和groupBy，二者的排序操作，默认是分区内局部有序，全局不一定，如果要做到全局有序，那么partition个数就设置为1，
 *          但是此时的计算性能非常差
 *    action:
 *      takeOrdered: 在拉取数据集的同时，对数据进行排序，如果一些操作，需要进行排序，并返回结果值，此时可以使用takeOrdered而不是先sortBy在take
 *
 */
object _01SparkSortOps {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName("_01SparkSortOps")
            .setMaster("local[2]")
        val sc = new SparkContext(conf)

        val stus = sc.parallelize(List(
            Student("卫超", 19, 187),
            Student("王礼鹏", 29, 177),
            Student("乌欣欣", 18, 168),
            Student("陈延年", 19, 157),
            Student("刘龙沛", 20, 175)
        ))

        //按照升高进行排序
//        sbkOps(stus)
//        sbOps(stus)

        val sorted: Array[Student] = stus.takeOrdered(2)(new Ordering[Student](){
            override def compare(x: Student, y: Student): Int = {
                x.height.compareTo(y.height)
            }
        })
        sorted.foreach(println)

        sc.stop()
    }

    def sbOps(stus: RDD[Student]): Unit = {
        val sorted = stus.sortBy(stu => stu.height, numPartitions = 1).mapPartitionsWithIndex((index, partition) => {
            val list = partition.toList
            println(s"分区编号为<${index}>中的数据为：${list.mkString("[", ", ", "]")}")
            list.toIterator
        })
        sorted.foreach(println)
    }
    def sbkOps(stus: RDD[Student]): Unit = {
        val sorted = stus.map(stu => (stu.height, stu))
            .sortByKey(ascending=false, numPartitions = 1)
            .mapPartitionsWithIndex((index, partition) => {
                val list = partition.toList
                println(s"分区编号为<${index}>中的数据为：${list.mkString("[", ", ", "]")}")
                list.toIterator
            })
        sorted.foreach(println)
    }
    case class Student(name: String, age: Int, height: Double)
}

