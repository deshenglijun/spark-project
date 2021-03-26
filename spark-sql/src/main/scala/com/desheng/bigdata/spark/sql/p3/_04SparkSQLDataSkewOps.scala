package com.desheng.bigdata.spark.sql.p3

import org.apache.spark.sql.SparkSession

import scala.util.Random

/*
 *  sparksql对于进行group by操作过程中产生的数据倾斜的处理。
    使用sql来解决sparksql编程过程中出现的数据倾斜问题
    以group by之后的dataskew为例来说明
    dataskew：
        什么是数据倾斜，表现有如何？
            数据分布不均匀，体现在，某一/几字段的数据特别多，只有在进行聚合计算的时候，数据才会进行重新分布。
         这个过程就是shuffle操作，shuffle操作的本质是相同key的数据汇聚到同一个计算节点上面进行聚合计算，这样
         才会出现前面说的特点，某一个分区的数据可能会比其他的分区数据体积大得多，否则原始的数据本身相对是均匀。

            表现一：
                大多数task运行都非常快，但是个别task运行相对很慢，极端情况，持续执行到99%，就是不结束(mr和hive中常见)
            变现二：
                多数情况下，程序是正常运行的，但是某一天突然发生了OOM异常。
       发生数据倾斜的原因：
           就是数据分布不均匀，需要明确的是，数据分布不均匀，不一定会产生数据倾斜，但是只要有数据倾斜，则一定是由数据分布不均匀产生的。
           如何会产生这个数据倾斜呢？我们来思考，在spark中作业的执行，不就是通过transformation来完成，可以将这些操作分为两种：
           窄依赖的操作，一种是宽依赖操作。窄依赖操作都是在本地完成的，所以这种情况下就不可能产生数据倾斜的问题；宽依赖说白了也就是shuffle操作。
           shuffle操作的实质是相同key的数据汇聚到同一个计算节点上面进行聚合计算，如果某一个key的数据比较多，就会导致对应节点上面的数据量
           比正常的节点大得多，后续执行操作的时候就会有显著的差距------数据倾斜。
           ===>shuffle是发生数据倾斜的必要条件。
      怎么解决呀？
         原因就是相同key的数据太多，让着相同key的数据变少，就会减低或者消除数据倾斜，所以第一步，就需要找到这个数据倾斜的key（数据量最多的哪一个key），
         怎么找到这个数据量最多key？不能全量进行计算，只能进行sample抽样，用样本去估计整体，样本空间可以执行wordcount--->

         如何减少key——添加随机前缀，添加随机前缀之后进行聚合，聚合完毕之后要去掉随机前缀，进行第二次的聚合操作。当然这只是其中的一种处理方案（两阶段聚合）
         https://tech.meituan.com/2016/05/12/spark-tuning-pro.html
         https://tech.meituan.com/2016/04/29/spark-tuning-basic.html
         四个方面：
            编程方面
            资源调优（参数）
            数据倾斜
            shuffle
   需求：
    1、统计每一个单词的次数，要求使用sparksql来进行统计
    2、如果发生了数据倾斜，请解决
        xxxByKey | group by导致的数据倾斜，就是两阶段聚合来解决（局部聚合+全局聚合）
        局部聚合
            将原先发生数据倾斜的key，添加随机前缀之后进行打散
            比如(hello, 1) (hello, 1) (hello, 1) (hello, 1),添加2以内的随机前缀
            (1_hello, 1) (0_hello, 1) (0_hello, 1) (1_hello, 1),
            紧接着进行局部聚合，==>
            (1_hello, 2) (0_hello, 2)
        全局聚合
            将带有前缀的数据，进行转换，去掉前缀 (hello, 2) (hello, 2)，
            在去掉前缀的基础之上再进行一个聚合操作——全局聚合
            (hello, 4)
 */
object _04SparkSQLDataSkewOps {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .master("local[*]")
                .appName("_04SparkSQLDataSkewOps")
                .getOrCreate()
        import spark.implicits._
        val list = List(
            "zhang zhang wen wen wen wen yue yue",
            "gong yi can can can can can can can can can can",
            "chang bao peng can can can can can can"
        )
        val df = spark.createDataset(list).toDF("line")

        df.createOrReplaceTempView("wc")
        println("-----未处理数据倾斜之前的wordcount的版本-----------")
        var sql =
            """
              |	select
              |		tmp.word,
              |		count(tmp.word) counts
              |	from (
              |		select
              |			explode(split(line, "\\s+")) word
              |		from wc
              |	) tmp
              |	group by tmp.word
              |""".stripMargin
        spark.sql(sql).show()
        println("-----开始处理数据倾斜之前的wordcount的版本-----------")
        println("----step.1.1 开始对数据及进行拆分---------")
        sql =
            """
              |select
              |	 explode(split(line, "\\s+")) word
              |from wc
              |""".stripMargin
        spark.sql(sql).show()
        println("----step.1.2 开始对数据及进行拆分之添加N以内的随机前缀---------")
        sql =
            """
              |select
              |  t1.word,
              |  concat_ws("_", cast(floor(rand() * 2) as string), t1.word) prefix_word
              |from (
              |  select
              |    explode(split(line, "\\s+")) word
              |  from wc
              |) t1
              |""".stripMargin
        spark.sql(sql).show()

        println("----step.2 基于随机前缀的局部聚合---------")
        sql =
            """
              |select
              |  concat_ws("_", cast(floor(rand() * 2) as string), t1.word) prefix_word,
              |  count(1) counts
              |from (
              |  select
              |    explode(split(line, "\\s+")) word
              |  from wc
              |) t1
              |group by prefix_word
              |""".stripMargin
        spark.sql(sql).show()

        println("----step.3 在局部聚合的基础上去掉随机前缀进行全局聚合---------")
        sql =
            """
              |select
              |  substr(t2.prefix_word, instr(t2.prefix_word, "_") + 1) word,
              |  sum(t2.counts) counts
              |from (
              |  select
              |    concat_ws("_", cast(floor(rand() * 2) as string), t1.word) prefix_word,
              |    count(1) counts
              |  from (
              |    select
              |      explode(split(line, "\\s+")) word
              |    from wc
              |  ) t1
              |  group by prefix_word
              |) t2
              |group by word
              |""".stripMargin
        spark.sql(sql).show()
        spark.stop()
    }

    def addRandomPrefix(str: String): String = {
        val random = new Random()
        random.nextInt(2) + "_" + str
    }
}
