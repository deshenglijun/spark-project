package com.desheng.bigdata.spark.sql.p3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * sparkSQL的UDF操作
 */
object _01SparkSQLUDFOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.INFO)
        val spark = SparkSession.builder()
            .master("local[*]")
            .appName("_01SparkSQLUDFOps")
            .getOrCreate()
        //注册
        spark.udf.register[Int, String]("myLen", str => dfadfasd(str))

        val df  = spark.read.json("file:/E:/data/spark/sql/people.json")
        /*
            注册一张临时表
            global
                在整个应用范围内有效，不带的话只在当前sparkSession内有效
            replace
                如果该视图存在，则会覆盖，否则新建
         */
        df.createOrReplaceTempView("person")
        //使用udf
        val sql =
            """
              |select
              |  name,
              |  length(name),
              |  myLen(name) my_len
              |from person
              |""".stripMargin
        spark.sql(sql).show
        spark.stop()
    }
    //第一步：创建一个函数来实现udf的业务
    def dfadfasd(str: String): Int = str.length
}
