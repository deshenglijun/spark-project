package com.desheng.bigdata.spark.sql.p3

import _01SparkSQLUDFOps.dfadfasd
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/*
    udaf 求每个省的平均升高
 */
object _02SparkSQLUDAFOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.INFO)
        val spark = SparkSession.builder()
            .master("local[*]")
            .appName("_02SparkSQLUDAFOps")
            .getOrCreate()
        //注册
        spark.udf.register("myAvg", new MyAvgUDAF)

        val df  = spark.read.json("file:/E:/data/spark/sql/people.json")
        /*
            注册一张临时表
            global
                在整个应用范围内有效，不带的话只在当前sparkSession内有效
            replace
                如果该视图存在，则会覆盖，否则新建
         */
        df.createOrReplaceTempView("person")

        val sql =
            """
              |select
              |   province,
              |   round(avg(height), 1) avg_height,
              |   round(myAvg(height), 1) my_avg_height
              |from person
              |group by province
              |""".stripMargin

        spark.sql(sql).show




        spark.stop()
    }
}
