package com.desheng.bigdata.spark.sql.p1

import org.apache.spark.sql.SparkSession

/**
 * Dataset的构建
 * 
 * dataset在构造的时候需要两个条件：
 *  第一导入隐式转换：import spark.implicits._
 *  第二要求封装数据类型为Product的子类，最好就是case class
 */
object _03SparkSQLModeOps {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
            .master("local[*]")
            .appName("_03SparkSQLModeOps")
            .getOrCreate()
        val list = List(
            Student("韩香彧", 17, 167.5),
            Student("石云涛", 88, 147.5),
            Student("刘炳文", 20, 170.5),
            Student("乔钰芹", 16, 167.5)
        )
        import spark.implicits._
        val df = spark.createDataset(list)

        df.show()

        spark.stop()
    }
}
case class Student(name: String, age: Int, height: Double)

