package com.desheng.bigdata.spark.sql.p1

import org.apache.spark.sql.{Row, SparkSession}

/**
 * 编程模型之间的互相转换：
 *   rdd--dataframe/dataset
 *   dataframe-->rdd/dataset
 *   dataset=-->dataframe/rdd
 */
object _04ProgramModeConversionOps {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
            .master("local[*]")
            .appName("_04ProgramModeConversionOps")
            .getOrCreate()
        import spark.implicits._
        //rdd --> dataset dataframe
        val rdd = spark.sparkContext.parallelize(List(
                    Student("韩香彧", 17, 167.5),
                    Student("石云涛", 88, 147.5),
                    Student("刘炳文", 20, 170.5),
                    Student("乔钰芹", 16, 167.5)
                ))
        println("rdd--->dataframe")
        val df = rdd.toDF
        df.show()
        println("rdd--->dataset")
        val ds = rdd.toDS()
        ds.show()
        println("dataframe-----------------rdd------------------>")
        val newRDD = df.rdd
        newRDD.foreach(row => {
            val name = row.getString(0)
            val age = row.getAs[Int]("age")
            val height = row.getAs[Double]("height")
            println(s"name: ${name}, age: ${age}, height: ${height}")
        })

        newRDD.foreach{case Row(name, age, height) => {
            println(s"case -- name: ${name}, age: ${age}, height: ${height}")
        }}
        println(
            """
              | dataframe 不能直接转化为Dataset
              | 为什么？我们前了解到dataframe中的泛型是Row，那么转化为Dataset其实就成了Dataset[Row]
              | 由于Row并不是Product的子类，并没有提供一个Encoder所以不能作为dataset的数据类型
              | 故而，不可直接转化为dataset
              |""".stripMargin)

        println("dataset-----------------rdd------------------>")
        ds.rdd
        ds.toDF("n", "a", "h")//给转化之后的dataframe指定特定的列名称
            .show()
        spark.stop()
    }
}
