package com.desheng.bigdata.spark.scala.p3.shared

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
 * 自定义累加器：
 *  IN ：add方法的参数类型
 *  OUT：value方法的返回值类型
 */
class MyAccumulator extends AccumulatorV2[String, Map[String, Int]]{

    private var map = mutable.Map[String, Int]()
    /**
     * 初始化值是否为0还是非0
     * 0，返回true
     * 非0，返回false
     */
    override def isZero: Boolean = true

    override def copy(): AccumulatorV2[String, Map[String, Int]] = {
        val newAccu = new MyAccumulator
        newAccu.map = this.map
        newAccu
    }

    //清空累加器
    override def reset(): Unit = this.map.clear()
    //局部累加
    override def add(word: String): Unit = {
//        val countOption = map.get(word)
//        if(countOption.isDefined) {//已经累加过了
//            map.put(word, countOption.get + 1)
//        } else {
//            map.put(word, 1)
//        }
        map.put(word, map.getOrElse(word, 0) + 1)
    }
    //全局累加
    override def merge(other: AccumulatorV2[String, Map[String, Int]]): Unit = {
        for((word, count) <- other.value) {
            map.put(word, map.getOrElse(word, 0) + count)
        }
    }


    override def value: Map[String, Int] = this.map.toMap
}
