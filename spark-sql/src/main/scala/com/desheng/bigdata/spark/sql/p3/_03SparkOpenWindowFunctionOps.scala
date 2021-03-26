package com.desheng.bigdata.spark.sql.p3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/*
    开窗函数，
        常见的有:
            row_number() over() -->分组TopN
            max() over()
            min() over()
            sum() over()
 */
object _03SparkOpenWindowFunctionOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.INFO)
        val spark = SparkSession.builder()
            .master("local[*]")
            .appName("_02SparkSQLUDAFOps")
            .getOrCreate()
        val df  = spark.read.json("file:/E:/data/spark/sql/people.json")

        df.createOrReplaceTempView("person")

        var sql =
            """
              |select
              | name,
              | height,
              | province,
              | row_number() over(partition by province order by height desc) rank
              |from person
              |""".stripMargin
        spark.sql(sql).show

         sql =
            """
              |select
              |  tmp.*
              |from (
              |  select
              |   name,
              |   height,
              |   province,
              |   row_number() over(partition by province order by height desc) rank
              |  from person
              |  ) tmp
              |where tmp.rank < 3
              |""".stripMargin
        spark.sql(sql).show
        spark.stop()
    }
}
