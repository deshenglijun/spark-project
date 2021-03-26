package com.desheng.bigdata.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.Arrays;

public class LambdaWordCountApp {
    public static void main(String[] args) {
        Logger.getLogger("org.apache.spark").setLevel(Level.INFO);//日志局部管理
        SparkConf conf = new SparkConf();
        conf.setAppName(LambdaWordCountApp.class.getSimpleName());
        conf.setMaster("local[*]");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> lines = jsc.textFile("E:/data/spark/hello.txt");

        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split("\\s+")).iterator());

        JavaPairRDD<String, Integer> pairs = words.mapToPair(word -> new Tuple2<String, Integer>(word, 1));

        JavaPairRDD<String, Integer> ret = pairs.reduceByKey((v1, v2) -> v1 + v2);

        ret.foreach(kv -> System.out.println(kv._1 + "---" + kv._2));

        jsc.stop();
    }
}
