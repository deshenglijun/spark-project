package com.desheng.bigdata.spark.scala.p3.partition

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
 * Spark rdd操作分区
 * 涉及到分区的操作，主要集中在这么几处：
 * 1、就是在输入操作
 *
 * 2、shuffle操作的时候会有分区的操作
 *
 * spark提供的分区方式，默认有2种：
 *  HashPartitioner（默认的）
 *  RangePartitioner(输入算子通常所使用)
 *
 * 自定义分区操作
 *
 *  需求：按照每一个科目一个文件，将数据进行重新的划分
 *      显然这就得需要用到重分区操作，但是该重分区不是repartition或者coleasce
 *  在spark rdd的操作中有一个算子专门可以进行分区--partitionBy
 *    当然，除了该算子以外，其他的只要是shuffle操作，通常都会有第二个参数，叫partitioner，也可以通过该参数来完成分区
 *
 */
object _01CustomerPartitionOps {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName("_01CustomerPartitionOps")
            .setMaster("local[2]")
        val sc = new SparkContext(conf)

        val lines = sc.parallelize(List(
            "chinese ls 91",
            "english ww 56",
            "chinese zs 90",
            "math zl 76",
            "english zq 88",
            "chinese wb 95",
            "chinese sj 74",
            "english ts 87",
            "math ys 67",
            "english mz 77",
            "chinese yj 98",
            "english gk 96",
            "math zq 88",
            "chinese wb 95",
            "math sj 74",
            "english ts 87",
            "math ys 67",
            "english mz 77",
            "math yj 98",
            "english gk 96"
        ))
        val course2Info = lines.map(line => {
            val index = line.indexOf(" ")
            val course = line.substring(0, index)
            val info = line.substring(index + 1)
            (course, info)
        })

        val keys = course2Info.keys.distinct().collect()
        //重分区
        course2Info.partitionBy(new MyGroupPartitioner(keys))
                .saveAsTextFile("file:/E:/data/out/spark/partition")

        sc.stop()
    }
}

class MyGroupPartitioner(keys: Array[String]) extends Partitioner {
    val key2PartitionId = {
        /*
        val tuples = for(i <- 0 until keys.length) yield (keys(i), i)
        tuples.toMap
         */
        val map = mutable.Map[String, Int]()
        for(i <- 0 until keys.length) {
            map.put(keys(i), i)
        }
        map
    }
    //最后的分区的个数
    override def numPartitions: Int = keys.length

    //将对应的key映射到相匹配的partitionId
    override def getPartition(key: Any): Int = {
        key2PartitionId.getOrElse(key.toString, 0)
    }
}