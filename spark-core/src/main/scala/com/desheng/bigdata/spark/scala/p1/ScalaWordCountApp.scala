package com.desheng.bigdata.spark.scala.p1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * scala版本的wordcount
 */
object ScalaWordCountApp {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName(s"${ScalaWordCountApp.getClass.getSimpleName}")
        /*
              master: 代表的spark作业运行的方式，常见的运行方式有local模式、standalone模式、yarn模式
              local模式的写法： spark作业在本地来进行运行，没有提交到集群中去
                    local       ：为当前spark作业分配一个工作线程
                    local[N]    ：为当前spark作业分配N个工作线程，但是不能超过电脑可用个工作线程
                    local[*]    ：为当前spark作业分配可用个工作线程
                    local[R, N] ：为当前spark作业分配N个工作线程，如果提交作业失败，会进行最多R次的重试
              standalone模式的写法
                    普通的分布式：spark://<ip>:<port>
                    ha   ：     spark://<ip1>:<port1>,<ip2>:<port2>
              yarn模式的写法
                   master=yarn
              注意：
                  standalone式和yarn模式要通常配合deploy-mode来进行使用

                  前提概念：spark作业在执行的时候，主要分为了这么两个部分，
                        一个部分就是executor（真正执行作业的进程）
                        还有一个executor的守护进程——driver（最重要的一个内容，就是SparkContext）
                  deploy-mode意为作业运行的方式，有两个选项：
                    client  ：默认值，driver或者sparkContext的创建与运行实在作业提交的机器上面完成
                    cluster ：driver的创建和运行，与executor的创建和运行都是在集群中来完成，也就是都在worker上面来完成创建初始化并运行
                  通常，生产的deploy-mode用cluster，测试和开发用client。
         */
            .setMaster("local[*]")
        val sc = new SparkContext(conf)

        //load external data into spark

        val lines: RDD[String] = sc.textFile("file:/E:/data/spark/hello.txt") //加载的本地文件

        println("lines rdd's partition is: " + lines.getNumPartitions)
        //calc word's count
        val words:RDD[String] = lines.flatMap(line => line.split("\\s+"))
        val pairs:RDD[(String, Int)] = words.map(word => (word, 1))
        val ret:RDD[(String, Int)] = pairs.reduceByKey(myReduceFunc)

        //export data to external system
        ret.foreach{case (word, count) => {
            println(word + "--->" + count)
        }}

        sc.stop()
    }
    def myReduceFunc(v1: Int, v2: Int): Int = {
//        val i = 1 / 0 //为了验证lazy
        v1 + v2
    }
}
