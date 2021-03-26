package com.desheng.bigdata.spark.scala.p2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
 * groupByKey/groupBy
 * reduceByKey
 * foldByKey
 * combineByKey
 * aggregateByKey
 */
object _02TransformationOps {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName("TransformationOps")
            .setMaster("local[*]")
        val sc = new SparkContext(conf)

//        groupByOps(sc)
//        reduceByKeyOps(sc)
        foldByKeyOps(sc)

        sc.stop()
    }
    /*
        foldByKey(zeroValue)((A1, A2) => A3)
        其作用和reduceByKey一样，唯一的区别就是zeroValue初始化值不一样
        相当于在scala集合操作中的reduce和fold的区别
     */
    def foldByKeyOps(sc: SparkContext): Unit = {
        case class Student(id: Int, name:String, province: String)
        val stuRDD = sc.parallelize(List(
            Student(1, "唐玉峰", "安徽"),
            Student(3, "胡国权", "甘肃"),
            Student(5, "马惠", "黑吉辽"),
            Student(2, "李梦", "山东"),
            Student(4, "陈延年", "甘肃"),
            Student(10086, "刘炳文", "黑吉辽")
        ), 2).mapPartitionsWithIndex((index, partition) => {
            val list = partition.toList
            println(s"-->stuRDD的分区编号为<${index}>中的数据为：${list.mkString("[", ", ", "]")}")
            list.toIterator
        })

        val ret = stuRDD.map(stu => (stu.province, 1)).foldByKey(0)((v1, v2) => v1 + v2)
        ret.foreach{case (province, count) => {
            println(s"province: ${province}, count: ${count}")
        }}
    }
    /*
        reduceByKey((A1, A2) => A3)
            前提不是对全量的数据集进行reduce操作，二十对每一个key所对应的所有的value进行reduce操作，相当于：
        rdd: RDD[K, V]
        val k2vs:RDD{(K, Iterable[V])] = rdd.groupByKey()
        k2vs.map{case (k, vs) => {
            (k, vs.reduce((A1, A2) => A3))
        }}
     */
    def reduceByKeyOps(sc: SparkContext): Unit = {
        case class Student(id: Int, name:String, province: String)
        val stuRDD = sc.parallelize(List(
            Student(1, "唐玉峰", "安徽"),
            Student(2, "李梦", "山东"),
            Student(3, "胡国权", "甘肃"),
            Student(4, "陈延年", "甘肃"),
            Student(5, "马惠", "黑吉辽"),
            Student(10086, "刘炳文", "黑吉辽")
        ))
        val ret = stuRDD.map(stu => (stu.province, 1)).reduceByKey((v1, v2) => v1 + v2)
        ret.foreach{case (province, count) => {
            println(s"province: ${province}, count: ${count}")
        }}
    }

    /**
     * groupByKey(numPartition):[K, Iterable[V]]
     *      按照key来进行分组，numPartition指的是分组之后的分区个数。
     *      这是一个宽依赖操作，但是需要注意一点的是，groupByKey相比较reduceByKey而言，没有本地预聚合操作，
     *      显然其效率并没有reduceByKey效率高，在使用的时候如果可以，尽量使用reduceByKey等去代替groupByKey。
     * groupBy其是就是对不是k-v键值对的数据提供的，其本质仍然是groupByKey
     *  groupBy => data.map(data => (k, v)).groupByKey
     * 用户表信息：
     *  按照省份来进行分组
     */
    def groupByOps(sc: SparkContext): Unit = {
        case class Student(id: Int, name:String, province: String)
        val stuRDD = sc.parallelize(List(
            Student(1, "唐玉峰", "安徽"),
            Student(2, "李梦", "山东"),
            Student(3, "胡国权", "甘肃"),
            Student(4, "陈延年", "甘肃"),
            Student(5, "马惠", "黑吉辽"),
            Student(10086, "刘炳文", "黑吉辽")
        ))

//        val province2Infos: RDD[(String, Iterable[Student]] = stuRDD.groupBy(stu => stu.province)
        val province2Infos: RDD[(String, Iterable[Student])] = stuRDD.map(stu => (stu.province, stu)).groupByKey()

        province2Infos.foreach{case (province, stus) => {
            println(s"省份：${province}, 学生信息：${stus.mkString(", ")}, 人数：${stus.size}")
        }}
    }
}
