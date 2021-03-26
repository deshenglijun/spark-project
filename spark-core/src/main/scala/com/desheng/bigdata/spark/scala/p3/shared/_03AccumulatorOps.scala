package com.desheng.bigdata.spark.scala.p3.shared

import com.desheng.bigdata.common.CommonUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 解决——01中的问题，我们可以使用累加器来解决，同时只会产生一个job
 * 但是在使用或者调用累加器的结果的时候，必须要在action触发之后才能调用，否则由于在transformation中执行，
 * 并不会去触发累加器的计算。
 * 同时在多此调用累加器的时候，一定要避免累加器的值重复，也就是重复累加，
 * 在一旦调用完累加器之后，立即释放累加器的值，acc.reset()重置
 */
object _03AccumulatorOps {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName("_03AccumulatorOps")
            .setMaster("local[*]")
        val sc = new SparkContext(conf)

        val listRDD = sc.parallelize(
            List(
                "a second spark a spark is shared second",
                "spark shared be shared in second spark"
            )
        )
        val numAccu = sc.longAccumulator("sparkAccu")
        //需求是统计每一个单词出现次数,在此基础之上只计算spark出现了多少次
        val pair = listRDD.flatMap(_.split("\\s+")).map(word => {
            if(word == "spark") {
                numAccu.add(1)
            }
            (word, 1)
        })
        val ret = pair.reduceByKey(_+_)
        println("action前，累加结果：" + numAccu.value)
        ret.foreach(println)
        println("action后，累加结果：" + numAccu.value)
        numAccu.reset()//重置累加器的值
        println("-------------重复调用--------------------")
        pair.count()
        println("重复调用累加器，累加结果：" + numAccu.value)

        CommonUtil.sleep(1000000)
        sc.stop()
    }
}
