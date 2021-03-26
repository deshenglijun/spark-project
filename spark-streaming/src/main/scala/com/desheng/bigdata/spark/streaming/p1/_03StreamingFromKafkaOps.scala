package com.desheng.bigdata.spark.streaming.p1

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/*
    SparkStremaing从kafka中读取数据
  PerPartitionConfig
    spark.streaming.kafka.maxRatePerPartition
    spark.streaming.kafka.minRatePerPartition
            都是代表了streaming程序消费kafka速率，
                max: 每秒钟从每个分区读取的最大的纪录条数
                    max=10，分区个数为3，间隔时间为2s
                        所以这一个批次能够读到的最大的纪录条数就是：10*3*2=60
                        如果配置的为0，或者不设置，起速率没有上限
                min: 每秒钟从每个分区读取的最小的纪录条数
              那么也就意味着，streaming从kafka中读取数据的速率就在[min, max]之间
    执行过程中出现序列化问题：
        serializable (class: org.apache.kafka.clients.consumer.ConsumerRecord, value: ConsumerRecord
        spark中有两种序列化的方式
            默认的就是java序列化的方式，也就是写一个类 implement Serializable接口，这种方式的有点事非常稳定，但是一个非常的确定是性能不佳
            还有一种高性能的序列化方式——kryo的序列化，性能非常高，官方给出的数据是超过java的序列化性能10倍，同时在使用的时候只需要做一个声明式的注册即可
                sparkConf.set("spark.serializer", classOf[KryoSerializer].getName)//指定序列化的方式
                    .registerKryoClasses(Array(classOf[ConsumerRecord[String, String]]))//注册要序列化的类

 */
object _03StreamingFromKafkaOps {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setMaster("local")
            .setAppName("_03StreamingFromKafkaOps")
//            .set("spark.serializer", classOf[KryoSerializer].getName)
//            .registerKryoClasses(Array(classOf[ConsumerRecord[String, String]]))
        //两次流式计算之间的时间间隔，batchInterval
        val batchDuration = Seconds(2) // 每隔2s提交一次sparkstreaming的作业
        val ssc = new StreamingContext(conf, batchDuration)


        val topics = Set("hadoop")
        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> "bigdata01:9092,bigdata02:9092,bigdata03:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "spark-kafka-grou-0817",
            "auto.offset.reset" -> "earliest",
            "enable.auto.commit" -> "false"
        )

//0 449 1 449 2 445

        /*
            从kafka中读取数据
            locationStrategy：位置策略
                制定如何去给特定的topic和partition来分配消费者进行调度，可以通过LocationStrategies来得到实例。
                在kafka0.10之后消费者先拉取数据，所以在适当的executor来缓存executor操作对于提高性能是非常重要的。
                PreferBrokers：
                    如果你的executor在kafka的broker实例在相同的节点之上可以使用这种方式。
                PreferConsistent：
                    大多数情况下使用这个策略，会把partition分散给所有的executor
                PreferFixed：
                    当网络或者设备性能等个方便不均衡的时候，可以蚕蛹这种方式给特定executor来配置特定partition。
                    不在这个map映射中的partition使用PreferConsistent策略
            consumerStrategy：消费策略
                配置在driver或者在executor上面创建的kafka的消费者。该接口封装了消费者进程信息和相关的checkpoint数据
                消费者订阅的时候的策略：
                    Subscribe           ： 订阅多个topic进行消费，这多个topic用集合封装
                    SubscribePattern    ： 可以通过正则匹配的方式，来订阅多个消费者，比如订阅的topic有 aaa,aab,aac,adc,可以通过a[abc](2)来表示
                    Assign              ：  指定消费特定topic的partition来进行消费，是更加细粒度的策略
         */
        val message:InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
                                                                    ConsumerStrategies.Subscribe(topics, kafkaParams))
//        message.print()//直接打印有序列化问题

        message.foreachRDD((rdd, bTime) => {
            if(!rdd.isEmpty()) {
                println("-------------------------------------------")
                println(s"Time: $bTime")
                println("-------------------------------------------")
                rdd.foreach(record => {
                    println(record)
                })
            }
        })


        ssc.start()
        ssc.awaitTermination()
    }
}
