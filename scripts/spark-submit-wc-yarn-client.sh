#!/bin/sh

SPARK_HOME=/home/bigdata/apps/spark
${SPARK_HOME}/bin/spark-submit \
--class com.offcn.bigdata.spark.scala.p1.RemoteScalaWordCountApp \
--master yarn \
--deploy-mode client \
--driver-memory 600m \
--executor-memory 600m \
--executor-cores 1 \
--num-executors 1 \
/home/bigdata/jars/spark/core/spark-wc.jar \
hdfs://ns1/data/hello.log \
hdfs://ns1/out/spark/wc
