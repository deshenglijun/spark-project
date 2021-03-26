package com.desheng.bigdata.spark.scala.p2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark RDD操作之transforamtion
 * map
 * flatMap
 * filter
 * distinct
 *      ---操作和scala集合的算子操作一模一样，没有啥不一样的，所以省略
 * sample
 * union
 * join
 * groupByKey/groupBy
 * reduceByKey
 * foldByKey
 * repartition/coalesce
 * combineByKey
 * aggregateByKey
 */
object _01TransformationOps {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
                .setAppName("_01TransformationOps")
                .setMaster("local[*]")
        val sc = new SparkContext(conf)

        //map
//        mapPartitionsOps(sc)
        // sample算子
//        sampleOps(sc)
//        unionOps(sc)
//        joinOps(sc)
        coalesceOps(sc)
        sc.stop()
    }

    /**
     *  coalesce(numPartition, shuffle=false): 分区合并的意思
     *      numPartition:分区后的分区个数
     *      shuffle：此次重分区是否开启shuffle，决定当前的操作是宽(true)依赖还是窄(false)依赖
     *  原先又100个分区，合并成10分区
     *  或者原先有2个分区，重分区之后变成了4个
     *  coalesce默认是一个窄依赖算子，如果压缩到1个分区的时候，就要开启shuffle=true，此时coalesce是一个宽依赖算子
     *  如果增大分区，shuffle=false，不会改变分区的个数，可以通过将shuffle=true来进行增大分区
     *      可以用repartition(numPartition)来进行代替= coalesce(numPartitions, shuffle = true)
     */
    def coalesceOps(sc: SparkContext): Unit = {
        val listRDD = sc.parallelize(1 to 20, 4)
            .mapPartitionsWithIndex((index, partition) => {
                val list = partition.toList
                println(s"-->listRDD的分区编号为<${index}>中的数据为：${list.mkString("[", ", ", "]")}")
                list.toIterator
            })
        //重分区，较少分区：变为2个：
        val coalesceRDD = listRDD.coalesce(2).mapPartitionsWithIndex((index, partition) => {
            val list = partition.toList
            println(s"coalesceRDD的分区编号为<${index}>中的数据为：${list.mkString("[", ", ", "]")}")
            list.toIterator
        })
        //重分区，增大分区：由2个变为4个：
        val rdd = coalesceRDD.repartition(4).mapPartitionsWithIndex((index, partition) => {
            val list = partition.toList
            println(s"增大分区之后rdd的分区编号为<${index}>中的数据为：${list.mkString("[", ", ", "]")}")
            list.toIterator
        })

        rdd.count
    }

    /**
     *  rdd1.join(rdd2) 相当于sql中的join连接操作
     *      A(id) a, B(aid) b
     *      select * from A a join B b on a.id = b.aid
     *  交叉连接: across join
     *      select * from A a across join B ====>这回产生笛卡尔积
     *  内连接： inner join，提取左右两张表中的交集
     *      select * from A a inner join B on a.id = b.aid 或者
     *      select * from A a, B b where a.id = b.aid
     *  外连接：outer join
     *      左外连接 left outer join 返回左表所有，右表匹配返回，匹配不上返回null
     *          select * from A a left outer join B on a.id = b.aid
     *      右外连接 right outer join 刚好是左外连接的相反
     *          select * from A a left outer join B on a.id = b.aid
     *  全连接 full join
     *      全外连接 full outer join
     *          = left outer join + right outer join
     * 前提：要先进行join，rdd的类型必须是K-V
     */
    def joinOps(sc: SparkContext): Unit = {
        case class Student(id: Int, name:String, province: String)
        case class Score(sid: Int, course: String, score: Double)

        val stuRDD = sc.parallelize(List(
            Student(1, "唐玉峰", "安徽·合肥"),
            Student(2, "李梦", "山东·济宁"),
            Student(3, "胡国权", "甘肃·白银"),
            Student(4, "陈延年", "甘肃·张掖"),
            Student(5, "马惠", "辽宁·葫芦岛"),
            Student(10086, "刘炳文", "吉林·通化")
        ))

        val scoreRDD = sc.parallelize(List(
            Score(1, "chinese", 95.5),
            Score(2, "english", 55.5),
            Score(3, "math", 20.5),
            Score(4, "pe", 32.5),
            Score(5, "physical", 59),
            Score(10000, "Chemistry", 99.5)
        ))
        val id2Stu:RDD[(Int, Student)] = stuRDD.map(stu => (stu.id, stu))
        val id2Score:RDD[(Int, Score)] = scoreRDD.map(score => (score.sid, score))
        println("==============inner join================")
        val joinedRDD:RDD[(Int, (Student, Score))] = id2Stu.join(id2Score)
//        joinedRDD.foreach(kv => {
//            println(s"id为${kv._1}的学生信息为:${kv._2._1}，其考试成绩信息为：${kv._2._2}")
//        })
        joinedRDD.foreach{case (id, (stu, score)) => {
            println(s"id为${id}的学生信息为:${stu}，其考试成绩信息为：${score}")
        }}
        println("==============left outer join================")
        val leftJoined: RDD[(Int, (Student, Option[Score]))] = id2Stu.leftOuterJoin(id2Score)
        leftJoined.foreach{case (id, (stu, scoreOption)) => {
            println(s"id为${id}的学生信息为:${stu}，其考试成绩信息为：${scoreOption.getOrElse("UnKnow")}")
        }}
        println("==============right outer join================")
        val rightJoined: RDD[(Int, (Option[Student], Score))] = id2Stu.rightOuterJoin(id2Score)
        rightJoined.foreach{case (id, (stuOption, score)) => {
            println(s"id为${id}的学生信息为:${stuOption.getOrElse("UnKnow")}，其考试成绩信息为：${score}")
        }}
        println("==============full outer join================")
        val fulloined: RDD[(Int, (Option[Student], Option[Score]))] = id2Stu.fullOuterJoin(id2Score)
        fulloined.foreach{case (id, (stuOption, scoreOption)) => {
            println(s"id为${id}的学生信息为:${stuOption.getOrElse("UnKnow")}，其考试成绩信息为：${scoreOption.getOrElse("UnKnow")}")
        }}
    }

    /**
     * rdd1.union(rdd2)
     *  相当于sql中的union all，进行两个rdd数据间的联合，需要说明一点是，该union是一个窄依赖操作，
     *  rdd1如果又N个分区，rdd2又M个分区，那么union之后的分区个数就为N+M
     */
    def unionOps(sc: SparkContext): Unit = {
        val listRDD1 = sc.parallelize(1 to 5, 2)
            .mapPartitionsWithIndex((index, partition) => {
                val list = partition.toList
                println(s"-->listRDD1的分区编号为<${index}>中的数据为：${list.mkString("[", ", ", "]")}")
                list.toIterator
            })
        val listRDD2 = sc.parallelize(6 to 10, 2)
            .mapPartitionsWithIndex((index, partition) => {
                val list = partition.toList
                println(s"==>listRDD2的分区编号为<${index}>中的数据为：${list.mkString("[", ", ", "]")}")
                list.toIterator
            })
        val unionRDD = listRDD1.union(listRDD2)

        unionRDD.mapPartitionsWithIndex((index, partition) => {
            val list = partition.toList
            println(s"unionRDD的分区编号为<${index}>中的数据为：${list.mkString("[", ", ", "]")}")
            list.toIterator
        }).count()
    }

    /**
     * sample(withReplacement, fraction, seed):随机抽样算子
     *   sample主要工作就是为了来研究数据本身，去代替全量研究会出现类似数据倾斜(dataSkew)等问题，无法进行全量研究，只能用样本去评估整体。
     *   withReplacement：Boolean    ： 有放回的抽样和无放回的抽样
     *   fraction：Double            ： 样本空间占整体数据量的比例，大小在[0, 1]，比如0.2， 0.65
     *   seed：Long                  ： 是一个随机数的种子，有默认值，通常不需要传参
     *
     *  需要说明一点的是，这个抽样是一个不准确的抽样，抽取的结果数可能在准确的结果上下浮动
     */
    def sampleOps(sc: SparkContext): Unit = {
        val list = sc.parallelize(1 to 100000)
        val sampled1 = list.sample(true, 0.01)
        println("sampled1 count: " + sampled1.count())
        val sampled2 = list.sample(false, 0.01)
        println("sampled2 count: " + sampled2.count())
    }

    /**
     * map(p: A => B)
     *     对集合中的每一条记录操作，将元素A转化为元素B
     * mapPartitions(p: Iterator[A] => Iterator[B])
     *    上面的map操作，一次处理一条记录；而mapPartitions一次性处理一个partition分区中的数据
     * 注意:
     *     虽说mapPartitions的执行性能要高于map，但是其一次性将一个分区的数据加载到执行内存空间，如果该分区数据集比较大，存在OOM的风险
     * mapPartitionsWithIndex((index, p: Iterator[A] => Iterator[B]))
     *     该操作比mapPartitions多了一个index，代表就是后面p所对应的分区编号
     *
     *     rdd的分区编号，命名规范，如果又N个分区，分区编号就从0，。。，N-1
     */
    def mapPartitionsOps(sc: SparkContext): Unit = {
        //把rdd中的每一个元素扩大为原来的2倍
        val array = 1 to 10
        val listRDD:RDD[Int] = sc.parallelize(array)
        var retRDD:RDD[Int] = listRDD.map(num => num * 2)
        //使用mapPartitions
        retRDD = listRDD.mapPartitions((partition: Iterator[Int]) => {
            val list = partition.toList
            println(s"分区中的数据为：${list.mkString("[", ", ", "]")}")
            list.map(_ * 2).toIterator
        })
        //action foreach去触发作业
        retRDD.foreach(println)
        println("-----------------------mapPartitionsWithIndex----------------------------------")
        listRDD.mapPartitionsWithIndex((index, partition) => {
            val list = partition.toList
            println(s"分区编号为<${index}>中的数据为：${list.mkString("[", ", ", "]")}")
            list.map(_ / 2).toIterator
        }).count()

    }
}
