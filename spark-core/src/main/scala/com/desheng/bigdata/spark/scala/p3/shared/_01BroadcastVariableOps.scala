package com.desheng.bigdata.spark.scala.p3.shared

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * 共享变量之广播变量
 */
object _01BroadcastVariableOps {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName("_01BroadcastVariableOps")
            .setMaster("local[*]")
        val sc = new SparkContext(conf)

        val stus = List(
            Student(1, "唐玉峰", "安徽·合肥"),
            Student(2, "李梦", "山东·济宁"),
            Student(3, "胡国权", "甘肃·白银"),
            Student(4, "陈延年", "甘肃·张掖"),
            Student(5, "马惠", "辽宁·葫芦岛"),
            Student(10086, "刘炳文", "吉林·通化")
        )
        val scoreRDD = sc.parallelize(List(
            Score(1, "chinese", 95.5),
            Score(2, "english", 55.5),
            Score(3, "math", 20.5),
            Score(4, "pe", 32.5),
            Score(5, "physical", 59),
            Score(10000, "Chemistry", 99.5)
        ))

//        joinOps(stus, scoreRDD)
        broadcastOps(stus, scoreRDD)
        sc.stop()
    }
    /*
        使用广播变量的方式来完成如下的关联操作
        map join-->大表+小表
     */
    def broadcastOps(stus: List[Student], scoreRDD:RDD[Score]): Unit = {
        val id2Stu = stus.map(stu => (stu.id, stu)).toMap
        //构建广播变量
        val bc: Broadcast[Map[Int, Student]] = scoreRDD.sparkContext.broadcast(id2Stu)

        scoreRDD.foreach(score => {
            val id = score.sid
            val stu = bc.value.getOrElse(id, Student(-1, null, null))
            println(s"${stu.id}\t${stu.name}\t${stu.province}\t${score.course}\t${score.score}")
        })
    }

    def joinOps(stus: List[Student], scoreRDD:RDD[Score]): Unit = {
        val stuRDD = scoreRDD.sparkContext.parallelize(stus)
        val id2Stu:RDD[(Int, Student)] = stuRDD.map(stu => (stu.id, stu))
        val id2Score:RDD[(Int, Score)] = scoreRDD.map(score => (score.sid, score))
        val joinedRDD:RDD[(Int, (Student, Score))] = id2Stu.join(id2Score)
        joinedRDD.foreach{case (id, (stu, score)) => {
            println(s"id为${id}的学生信息为:${stu}，其考试成绩信息为：${score}")
        }}
    }
}
case class Student(val id: Int, name:String, province: String)
case class Score(sid: Int, course: String, score: Double)