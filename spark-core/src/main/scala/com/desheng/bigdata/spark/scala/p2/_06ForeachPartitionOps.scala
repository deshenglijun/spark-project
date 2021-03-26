package com.desheng.bigdata.spark.scala.p2

import java.sql.DriverManager

import com.desheng.bigdata.common.db.ConnectionPool
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *  foreach
 *  foreachPartition
 *      写入数据库的操作
 *
 * grant all PRIVILEGES on test.* to 'mark'@'%' IDENTIFIED by 'sorry';
 * flush PRIVILEGES;
 */
object _06ForeachPartitionOps {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName("_05ActionOps")
            .setMaster("local[*]")
        val sc = new SparkContext(conf)
    /*
    CREATE TABLE wordcounts (
        word VARCHAR ( 10 ),
    `count` INT
    )
     */
        val array = sc.parallelize(Array(
            "hello you",
            "green eye",
            "demon you",
            "heaven you world",
            "hell me",
            "green hand"
        ), 1)

        val pairs = array.flatMap(_.split("\\s+")).map((_, 1))
        val rdd:RDD[(String, Int)] = pairs.aggregateByKey(0)(_+_, _+_)

//        saveInfoMySQL(rdd)
//        saveInfoMySQL2(rdd)
//        saveInfoMySQLByForeachPartition(rdd)
//        saveInfoMySQLByForeachPartitionBatch(rdd)
//        saveInfoMySQLByForeachPartitionBatch2(rdd)
        saveInfoMySQLByConnectionPool(rdd)
        sc.stop()
    }
    def saveInfoMySQLByConnectionPool(rdd: RDD[(String, Int)]): Unit = {
        rdd.foreachPartition(partition => {
            //这是在partition内部，属于该partition的本地
            val connection = ConnectionPool.getConnection()
            val sql =
                """
                  |insert into wordcounts(word, `count`) Values(?, ?)
                  |""".stripMargin
            val ps = connection.prepareStatement(sql)

            var times = 0
            partition.foreach{case (word, count) => {
                times += 1
                ps.setString(1, word)
                ps.setInt(2, count)
                ps.addBatch() //批量操作
                if(times >= 4) {//分批次分批量写入
                    ps.executeBatch()
                    times = 0
                }
            }}
            if(times != 0) {
                ps.executeBatch() //批量操作
                times = 0
            }
            ps.close()
            ConnectionPool.release(connection)
        })
    }
    /*
        在之前的基础之上分批次批量写入数据库
        就是说：如果该分区由10w记录，每个批次不再是全量写入，二十每个批次写u5000条记录，分20此写完
        每4条记录写一次
     */
    def saveInfoMySQLByForeachPartitionBatch2(rdd: RDD[(String, Int)]): Unit = {
        rdd.foreachPartition(partition => {
            //这是在partition内部，属于该partition的本地
            Class.forName("com.mysql.jdbc.Driver")
            val url = "jdbc:mysql://localhost:3306/test"
            val connection = DriverManager.getConnection(url, "mark", "sorry")
            val sql =
                """
                  |insert into wordcounts(word, `count`) Values(?, ?)
                  |""".stripMargin
            val ps = connection.prepareStatement(sql)

            var times = 0
            partition.foreach{case (word, count) => {
                times += 1
                ps.setString(1, word)
                ps.setInt(2, count)
                ps.addBatch() //批量操作
                if(times >= 4) {//分批次分批量写入
                    ps.executeBatch()
                    times = 0
                }
            }}
            if(times != 0) {
                ps.executeBatch() //批量操作
                times = 0
            }
            ps.close()
            connection.close()
        })
    }

    //在之前的基础之上批量写入数据库
    def saveInfoMySQLByForeachPartitionBatch(rdd: RDD[(String, Int)]): Unit = {
        rdd.foreachPartition(partition => {
            //这是在partition内部，属于该partition的本地
            Class.forName("com.mysql.jdbc.Driver")
            val url = "jdbc:mysql://localhost:3306/test"
            val connection = DriverManager.getConnection(url, "mark", "sorry")
            val sql =
                """
                  |insert into wordcounts(word, `count`) Values(?, ?)
                  |""".stripMargin
            val ps = connection.prepareStatement(sql)

            partition.foreach{case (word, count) => {
                ps.setString(1, word)
                ps.setInt(2, count)
                ps.addBatch() //批量操作
            }}
            ps.executeBatch() //批量操作
            ps.close()
            connection.close()
        })
    }

    /*
        第二种效率太低，每一条记录都要加载一次Driver，创建一次连接
        可以采用在每一个分区内加载一次驱动，创建一次连接来完成使用
     */
    def saveInfoMySQLByForeachPartition(rdd: RDD[(String, Int)]): Unit = {
        rdd.foreachPartition(partition => {
            //这是在partition内部，属于该partition的本地
            Class.forName("com.mysql.jdbc.Driver")
            val url = "jdbc:mysql://localhost:3306/test"
            val connection = DriverManager.getConnection(url, "mark", "sorry")
            val sql =
                """
                  |insert into wordcounts(word, `count`) Values(?, ?)
                  |""".stripMargin
            val ps = connection.prepareStatement(sql)

            partition.foreach{case (word, count) => {
                ps.setString(1, word)
                ps.setInt(2, count)
                ps.execute()
            }}
            ps.close()
            connection.close()
        })
    }

    //终于写入数据库，但是极其不友好
    def saveInfoMySQL2(rdd: RDD[(String, Int)]): Unit = {
        rdd.foreach{case (word, count) => {
            Class.forName("com.mysql.jdbc.Driver")
            val url = "jdbc:mysql://localhost:3306/test"
            val connection = DriverManager.getConnection(url, "mark", "sorry")
            val sql =
                """
                  |insert into wordcounts(word, `count`) Values(?, ?)
                  |""".stripMargin
            val ps = connection.prepareStatement(sql)

            ps.setString(1, word)
            ps.setInt(2, count)
            ps.execute()

            ps.close()
            connection.close()
        }}
    }

    /*
        数据库编程,这是由序列化的问题
     */
    def saveInfoMySQL(rdd: RDD[(String, Int)]): Unit = {
        //加载驱动
//        classOf[Driver]
        Class.forName("com.mysql.jdbc.Driver")
        //获得Connection连接
        val url = "jdbc:mysql://localhost:3306/test"
        val connection = DriverManager.getConnection(url, "mark", "sorry")
        //获取执行器statement
        val sql =
            """
              |insert into wordcounts(word, `count`) Values(?, ?)
              |""".stripMargin
        val ps = connection.prepareStatement(sql)
        rdd.foreach{case (word, count) => {
            ps.setString(1, word)
            ps.setInt(2, count)

            ps.execute()
        }}
        ps.close()
        connection.close()
    }
}
