package com.desheng.bigdata.spark.sql.p2

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * SparkSQL对数据的统一加载和落地操作
 *  加载使用
 *      read.load
 *           not a Parquet file. expected magic numbe ==> 默认加载的文件格式要求是parquet，是一个二进制的列式存储格式文件，twitter公司开源到apache的
 *      option
 *          https://docs.databricks.com/data/data-sources/aws/amazon-s3-select.html#csv-specific-options
 *  落地使用
 *      write.save
 */
object _01LoadAndSaveLoadOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.INFO)
        val spark = SparkSession.builder()
            .master("local[*]")
            .appName("_01LoadAndSaveLoadOps")
            .getOrCreate()
        import spark.implicits._
//        loadData(spark)
        //text的文本格式的数据，sparksql只会认为只有一列数据，一行数据只有一列
        val ds = spark.read.text("file:/E:/data/spark/sql/sql-rdd-source.txt")
                    .map(line => {
                        val fields = line.getString(0).split(",")
                        val id = fields(0).trim.toInt
                        val name = fields(1).trim
                        val age = fields(2).trim.toInt
                        val height = fields(3).trim.toInt
                        Person(id, name, age, height)
                    })
        ds.show()
        /*
            数据的落地
            SaveMode:
                ErrorIfExists 默认的
                Append        追加
                Overwrite     覆盖
                Ignore        忽略，如果目录已经存在，则忽略，如果目录不存在，则执行创建
         */
        ds.write.mode(SaveMode.Append).save("file:/E:/data/output/spark/parquet")
        ds.write.mode(SaveMode.Ignore).json("file:/E:/data/output/spark/json")
        ds.write.mode(SaveMode.Overwrite)
            .option("header", "true")
            .option("delimiter", "|")
            .option("comment", "#")
            .csv("file:/E:/data/output/spark/csv")
        //jdbc
        val url = "jdbc:mysql://localhost:3306/test"
        val table = "person"
        val properties = new Properties()
        properties.put("user", "mark")
        properties.put("password", "sorry")
        ds.write.jdbc(url, table, properties)

        spark.stop()
    }

    def loadData(spark: SparkSession): Unit = {
        var df = spark.read.load("file:/E:/data/spark/sql/sqldf.parquet")
        //        df  = spark.read.format("json").load("file:/E:/data/spark/sql/people.json")
        df  = spark.read.json("file:/E:/data/spark/sql/people.json")
        df = spark.read.csv("file:/E:/data/spark/sql/country.csv").toDF("id", "country", "code")
        //对于复杂的操作，需要设置一些option选项来完成过滤或者修正
        df = spark.read
            .option("header", "true")
            .option("delimiter", "|")
            .option("comment", "#")
            .csv("file:/E:/data/spark/sql/location-info.csv")
        //orc是以中列式存储文件，式rc的升级版本呢，式facebook用来存储数据的文件格式
        df = spark.read.orc("file:/E:/data/spark/sql/student.orc")
        //加载jdbc对应的数据
        val url = "jdbc:mysql://localhost:3306/test"
        val table = "wordcounts"
        val properties = new Properties()
        properties.put("user", "mark")
        properties.put("password", "sorry")
        df = spark.read.jdbc(url, table, properties)
    }
}
case class Person(id: Int, name: String, age: Int, height: Int)
