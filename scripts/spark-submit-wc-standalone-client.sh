#!/bin/sh

SPARK_HOME=/home/bigdata/apps/spark
${SPARK_HOME}/bin/spark-submit \
--class com.offcn.bigdata.spark.scala.p1.RemoteScalaWordCountApp \
--master spark://bigdata01:7077 \
--deploy-mode client \
--driver-memory 600m \
--executor-memory 600m \
--executor-cores 1 \
--total-executor-cores 1 \
/home/bigdata/jars/spark/core/spark-wc.jar \
hdfs://ns1/data/hello.log
