#!/bin/sh

SPARK_HOME=/home/bigdata/apps/spark
${SPARK_HOME}/bin/spark-submit \
--class com.offcn.bigdata.spark.scala.p1.RemoteScalaWordCountApp \
--master local \
--deploy-mode client \
--driver-memory 600m \
/home/bigdata/jars/spark/core/spark-wc.jar \
hdfs://ns1/data/hello.log
