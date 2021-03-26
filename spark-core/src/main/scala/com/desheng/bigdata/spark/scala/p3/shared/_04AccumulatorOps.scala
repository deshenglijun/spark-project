package com.desheng.bigdata.spark.scala.p3.shared

import com.desheng.bigdata.common.CommonUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 在累加的基础之上，多具有相同业务的多个值进行累加
 *
 * 对于复杂累加操作，可以通过自定义累加器来实现
 */
object _04AccumulatorOps {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName("_04AccumulatorOps")
            .setMaster("local[*]")
        val sc = new SparkContext(conf)

        val listRDD = sc.parallelize(
            List(
                "a second spark a spark is shared second",
                "spark shared be shared in second spark"
            )
        )
        val myAccu = new MyAccumulator()
        sc.register(myAccu, "myAccu")

        //需求是统计每一个单词出现次数,在此基础之上只计算spark出现了多少次
        val pair = listRDD.flatMap(_.split("\\s+")).map(word => {
            if(word == "spark" || word == "second") {
                myAccu.add(word)
            }
            (word, 1)
        })
        val ret = pair.reduceByKey(_+_)
        println("action前，累加结果：" + myAccu.value)
        ret.foreach(println)
        println("action后，累加结果：" + myAccu.value)

        sc.stop()
    }
}
