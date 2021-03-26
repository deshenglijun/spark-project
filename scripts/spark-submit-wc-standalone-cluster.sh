#!/bin/sh

SPARK_HOME=/home/bigdata/apps/spark
${SPARK_HOME}/bin/spark-submit \
--class com.offcn.bigdata.spark.scala.p1.RemoteScalaWordCountApp \
--master spark://bigdata01:7077 \
--deploy-mode cluster \
--driver-memory 600m \
--driver-cores 1 \
--executor-memory 600m \
--executor-cores 1 \
--total-executor-cores 1 \
hdfs://ns1/jars/spark/core/spark-wc.jar \
hdfs://ns1/data/hello.log
