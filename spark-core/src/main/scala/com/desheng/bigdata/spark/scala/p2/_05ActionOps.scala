package com.desheng.bigdata.spark.scala.p2

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * action行动算子操作：
 *  是spark作业执行的动因
 *  foreach
 *  count：返回rdd中的记录数，相当于集合.size()
 *  collect
 *  take
 *  first
 *  reduce
 *  countByKey
 *  saveAsXxx
 *  foreachPartition
 */
object _05ActionOps {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName("_05ActionOps")
            .setMaster("local[*]")
        val sc = new SparkContext(conf)

        val array = sc.parallelize(Array(
            "hello you",
            "hello me",
            "hello you",
            "hello you",
            "hello me",
            "hello you"
        ), 2)

        val pairs = array.flatMap(_.split("\\s+")).map((_, 1))

        val ret:RDD[(String, Int)] = pairs.aggregateByKey(0)(_+_, _+_)

        //学习action操作
        /*
            collect的作用就是将分散在executor上面的rdd-partition数据，通过网络拉回到driver之上，返回值是一个数组
            尽量避免对全量数据集进行collect操作，搞不好，driver要崩溃
            所以，在collect之前先filter一部分数据，或者使用其他算子，比如take
         */
        var array1 = ret.collect()
        println("--collect---" + array1.mkString("[", ", ", "]"))

        /*
            take(N)就是从rdd中zhi只拉去其中的N条记录，返回值是一个数组
            如果rdd中的数据都是有序的，take(N) ---> topN
            take(N)的一个特例就是first，获取第一条记录
      */
        array1 = ret.take(1)
        println("--take---" + array1.mkString("[", ", ", "]"))

        /*
            reduce， reduce是一个action操作，返回值是单列内容，就相当于scala集合
            reduceByKey是一个transformation操作，返回值是一个RDD
         */

        val count = ret.values.reduce(_ + _)
        println("总单词的个数：" + count)

        /**
         * countByKey,统计key出现的次数
         *  返回值是一个Map[K, Long]
         */
        val wordcount = pairs.countByKey()
        for((word, count) <- wordcount) {
            println(s"word: ${word}, count: ${count}")
        }

        /*
            写入外部的存储系统里面
            saveAsTextFile():将数据集以普通的文本写入到文件系统中
            saveAsObjectFile: 将数据集以hadoop支持的SequenceFile格式写入文件系统，相当于saveAsSequenceFile
            saveAsHadoopFile()      ----> OutputFormat ---> interface org.apache.hadoop.mapred.OutputFormat
            saveAsNewAPIHadoopFile()   ---->NewOutputFormat --> abstract class org.apache.hadoop.mapreduce.OutputFormat

            mr中指定输出时候，需要设置一个OutputFormat
            FileOutputFormat.setOutput(path)
            job.setOutputKeyClass()
            job.setOutputValueClass()
            job.setOutputFormatClass(TextOutputFormat.classs)
         */
        ret
//            saveAsTextFile("hdfs://ns1/data/output/spark/wc")
//            saveAsObjectFile("file:/E:/data/output/spark/obj")
//            .saveAsHadoopFile()
             .saveAsNewAPIHadoopFile(
                    path = "file:/E:/data/output/spark/hadoop",
                    keyClass = classOf[Text],
                    valueClass = classOf[IntWritable],
                    outputFormatClass = classOf[TextOutputFormat[Text, IntWritable]]
                )
        sc.stop()
    }
}
