package com.desheng.bigdata.spark.sql.p3

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

/**
 * 自定义udaf
 */
class MyAvgUDAF extends UserDefinedAggregateFunction {
    /*
        该udaf输入参数的类型说明
     */
    override def inputSchema: StructType = StructType(List(
        StructField("height", DataTypes.DoubleType, false)
    ))
    /*
        为了计算聚合结果所需要的涉及到的临时变量的类型
        平均数=总数/个数，这里面涉及到了2个临时变量，总数，个数
     */
    override def bufferSchema: StructType =  StructType(List(
        StructField("sum", DataTypes.DoubleType, false),
        StructField("count", DataTypes.IntegerType, false)
    ))
    /*
        该udaf返回值的数据类型
     */
    override def dataType: DataType = DataTypes.DoubleType
    /*
        确定性，相同的输入，其返回值是确定，不会有其他可能，称之为确定性，即返回为true
     */
    override def deterministic: Boolean = true
    //初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer.update(0, 0.0)
        buffer.update(1, 0)
    }

    //局部聚合
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        buffer.update(0, buffer.getDouble(0) + input.getDouble(0))
        buffer.update(1, buffer.getInt(1) + 1)
    }

    //分区间的全局聚合
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        buffer1.update(0, buffer1.getDouble(0) + buffer2.getDouble(0))
        buffer1.update(1, buffer1.getInt(1) + buffer2.getInt(1))
    }

    override def evaluate(buffer: Row): Double = {
        buffer.getDouble(0) / buffer.getInt(1)
    }
}
