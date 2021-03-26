package com.desheng.bigdata.spark.scala.p2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * combineByKey,也是按照key进行聚合，那么他和groupByKey还有reduceByKey之间有什么区别：
 *  reduceByKey和groupByKey顶层都是通过combineByKeyWithClassTag来实现的，
 *  而combineByKey是combineByKeyWithClassTag的一个简化的版本，
 *  ClassTag作用用来存储在运行时一个类被擦除的泛型，以便于在运行时来访问这个类型的字段泛型信息。
 *  比如对于一个编译器不知道类型的数组，在运行时久非常有用。
 *
 *  模拟reduceByKey和groupByKey
 */
object _03CombineByKeyOps {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName("_03CombineByKeyOps")
            .setMaster("local[*]")
        val sc = new SparkContext(conf)

//        cbk2rbk(sc)
        cbk2gbk(sc)


        sc.stop()
    }

    def cbk2gbk(sc: SparkContext): Unit = {
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
        //局部初始化
        def createCombiner(stu: Student): Array[Student] = {
            println(s"---createCombiner---->${stu}-----")
            Array[Student](stu)
        }
        //局部聚合
        def mergeValue(stus: Array[Student], stu: Student): Array[Student] = {
            println(s"---mergeValue---->${stus.mkString("[", ", ", "]")}--, stu: ${stu}---")
            stus.+:(stu)
        }
        //全局聚合
        def mergeCombiner(stus: Array[Student], stuI: Array[Student]): Array[Student] = {
            println(s"---mergeCombiner---->${stus.mkString("[", ", ", "]")}--, stu: ${stuI.mkString("[", ", ", "]")}---")
            stus ++ stuI
        }

        val province2Infos: RDD[(String, Array[Student])] = pairs.combineByKey(createCombiner, mergeValue, mergeCombiner)

        province2Infos.foreach{case (province, stus) => {
            println(s"province2Infos>>> 省份：${province}, 学生信息：${stus.mkString(", ")}, 人数：${stus.size}")
        }}
    }

    /**
     * 模拟reduceByKey
     * createCombiner:初始化
     * mergeValue   : （分区内）局部聚合
     * mergeCombiner:（分区间）全局聚合
     */
    def cbk2rbk(sc: SparkContext): Unit = {
        val array = sc.parallelize(Array(
            "hello you",
            "hello me",
            "hello you",
            "hello you",
            "hello me",
            "hello you"
        ), 2)

        val pairs = array.flatMap(_.split("\\s+")).map((_, 1))

        val ret = pairs.combineByKey(createCombiner, mergeValue, mergeCombiners)

        ret.foreach{case (key, count) => {
            println(s"key: ${key}, count: ${count}")
        }}
    }
    //该参数num，就是这个key在该分区内出现的第一个元素，用来进行初始化 i= 21
    def createCombiner(num: Int): Int = {
        num
    }
    //在每一个分区内，完成局部sumi和新遍历到的元素进行聚合  sumi = sumi + i
    def mergeValue(sumI: Int, num: Int): Int = {
            sumI + num
    }
    //分区间在分区内局部聚合的基础之上进行全局聚合 相当于  sum = sum + sumI
    def mergeCombiners(sum: Int, sumI: Int) = {
        sum + sumI
    }
}
