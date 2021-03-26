package com.desheng.bigdata.spark.sql.p1

import com.desheng.bigdata.spark.domain.Person
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConversions

/**
 * SparkSQL中的编程模型主要有：DataFrame和Dataset
 * DataFrame的构建分为了两种方式
 *      基于反射的方式构建
 *      基于动态编程的方式构建
 * Dataset的构建发昂是和dataframe差不多一样
 */
object _02SparkSQLModeOps {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .master("local[*]")
                .appName("_02SparkSQLModeOps")
                .getOrCreate()
        //create dataframe
//        createByBean(spark)
        import scala.collection.JavaConversions._
        val rows = List(
            new Person("韩香彧", 17, 167.5),
            new Person("石云涛", 88, 147.5),
            new Person("刘炳文", 20, 170.5),
            new Person("乔钰芹", 16, 167.5)
        ).map(person => {
            Row(person.getName, person.getAge, person.getHeight)
        })
        //动态编程
        val schema = StructType(
            Array(
                StructField("name", DataTypes.StringType, false),
                StructField("age", DataTypes.IntegerType, false),
                StructField("height", DataTypes.DoubleType, false)
            )
        )
       val df = spark.createDataFrame(rows, schema)

        df.show()

        spark.stop()
    }
    def createByBean(spark: SparkSession): Unit = {
        val list = List(
            new Person("韩香彧", 17, 167.5),
            new Person("石云涛", 88, 147.5),
            new Person("刘炳文", 20, 170.5),
            new Person("乔钰芹", 16, 167.5)
        )
        /*
            scala中可以和java的集合互相转换，需要通过一个工具类JavaConversion
                JavaConversions.seqAsJavaList(list)
            还可以进行隐式调用
                import scala.collection.JavaConversions._
          */
        val javaList = JavaConversions.seqAsJavaList(list)

        val pdf = spark.createDataFrame(javaList, classOf[Person])

        pdf.show()

    }
}

//case class Person(name: String, age: Int, height: Double)