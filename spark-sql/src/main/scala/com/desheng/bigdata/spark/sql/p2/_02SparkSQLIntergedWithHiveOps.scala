package com.desheng.bigdata.spark.sql.p2

import org.apache.spark.sql.SparkSession

/**
 * SparkSQL和Hive的整合
 *  从 hive中加载，完成计算，并将结果落地到hive表中
 *  teacher_basic b
 *      name,age,married,course
 *
 *  teacher_info i
 *      name,height
 *  第一步：在hive中创建这两张表，并完成数据的加载
 *  第二部：进行关联查询，找到所有数据
 *  select
 *    *
 *  from teacher_basic b
 *  inner join teacher_info i on b.name = i.name
 *  第三步：将关联查询结果写入到hive对应表中
 *
 * spark和hive整合式需要注意的地方：
 *      1、为了能够让spark正常的解析hive的仓库为止，需要将hive-site.xml传递给spark，加载到spark的classpath中
 *          一种通过直接将hive-site.xml放到spark的conf目录下面
 *          另外一种就是通过程序的方式放到classpath即可（第二种）
 *      2、在hive-site.xml中最重要的就是一个参数
 *      <property>
     *      <name>hive.metastore.warehouse.dir</name>
     *      <value>/user/hive/warehouse</value>
 *      </property>
 *      如果没有配置这个参数，就会在当前程序的当前目录下面指定hive的warehouse，而真正的数据在hdfs里卖弄，执行的时候会找不到数据：
 *          except: file:///  hdfs://
 *       3、同时得需要解析出hdfs的具体路径，所以也需要将hdfs-site.xml和core-site.xml也打到classpath下面
 *       4、得需要将mysql的驱动包打入classpath中
 */
object _02SparkSQLIntergedWithHiveOps {
    def main(args: Array[String]): Unit = {

        if(args == null || args.length != 2) {
            println(
                """
                  |usage! <basic_path> <info_path>
                  |""".stripMargin)
            System.exit(-1)
        }
        val Array(basic_path, info_path) = args


        val spark = SparkSession.builder()
            .master("local[*]")
            .appName("_02SparkSQLIntergedWithHiveOps")
            .enableHiveSupport()//支持hive的操作
            .getOrCreate()

        println("step 1 create database.")

        spark.sql(
            """
              |create database if not exists test_0817
              |""".stripMargin)

        println("step 2 create table teacher_basic.")
        spark.sql(
            """
              |create table if not exists `test_0817`.`teacher_basic` (
              |   name string,
              |   age int,
              |   married boolean,
              |   course int
              |) row format delimited
              |fields terminated by ','
              |""".stripMargin)

        println("step 2 create table teacher_info.")
        spark.sql(
            """
              |create table if not exists `test_0817`.`teacher_info` (
              |   name string,
              |   height double
              |) row format delimited
              |fields terminated by ','
              |""".stripMargin)

        println("step 3 load data into teacher_basic.")
        spark.sql(
            s"""
              |load data inpath '${basic_path}' into table `test_0817`.`teacher_basic`
              |""".stripMargin)

        println("step 3 load data into teacher_info.")
        spark.sql(
            s"""
               |load data inpath '${info_path}' into table `test_0817`.`teacher_info`
               |""".stripMargin)

        println("step 4 join")
        val joined = spark.sql(
                """
                  |select
                  |  b.name,
                  |  b.age,
                  |  b.married,
                  |  b.course,
                  |  i.height
                  |from `test_0817`.`teacher_basic` b
                  |inner join `test_0817`.`teacher_info` i on b.name = i.name
                  |""".stripMargin)
        println("step 5 save data into hive table")

        joined.write.saveAsTable("`test_0817`.`teacher`")

        println("ok!")
        spark.stop()
    }
}
