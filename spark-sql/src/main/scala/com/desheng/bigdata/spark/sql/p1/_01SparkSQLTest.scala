package com.desheng.bigdata.spark.sql.p1

import org.apache.spark.sql.SparkSession

/**
 * SparkSQL编程入门体验
 *   入口：SparkSesson
 */
object _01SparkSQLTest {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
            .appName("_01SparkSQLTest")
            .master("local[*]")
//            .enableHiveSupport()//支持hive的特定操作
            .getOrCreate()

        val pdf = spark.read.json("file:/E:/data/spark/sql/people.json")
        println("------获取表中的元数据信息-----------")
        pdf.printSchema()
        println("------获取表中的数据信息-----------")
        pdf.show()
        println("------筛选表中的个别字段-----------")
        //select name, age, height from xx
        pdf.select("name", "age", "height").show()
        println("------条件查询-----------")
        //select name, age, height from xx where province = "广东"
        import spark.implicits._
        pdf.select($"name", ($"age" + 1).as("age"), $"height")
                .where(" province = \"广东\"")
                .show()
        println("------统计-----------")
        //select provice, count(1) from xx group by province
        pdf.select("province").groupBy("province").count().show()
        println("------复杂统计统计-----------")
        //select provice, count(1), max(age) from xx group by province
        pdf.groupBy("province")
            .agg(Map(
                "age" -> "max",
                "province" -> "count"
            )).show()

        /**
         * 将上述的编程方式我们称之为SparkSQL的DSL(domain special language 特定领域语言)编程方式，
         *
         * 另外以中更常见的操作方式，就是sql
         */
         pdf.createOrReplaceTempView("people")//在内存中声明一张表
        val sql =
            """
              |select
              |   province,
              |   count(1) counts,
              |   max(age) maxAge
              |from people
              |group by province
              |""".stripMargin
        spark.sql(sql).show()
        spark.stop()
    }
}
