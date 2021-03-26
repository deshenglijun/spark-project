package com.desheng.bigdata.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * spark的wordcount的入门程序
 * 编程入口为sparkContext
 * 步骤：
 *  1.创建SparkContext
 *      构建SparkContext相关的以来
 *          A master URL must be set in your configuration
 *          An application name must be set in your configuration
 *  2.加载外部数据源
 *  3.基于业务完成各种计算
 *  4.完成数据的落地
 *  5.释放资源
 *
 *  需要说明一点的是：
 *      SparkContext在不同的版本，不同的模块中名字不一样
 *    SparkCore中
 *          java中：叫JavaSparkContext
 *          scala中：SparkContext
 *    SparkSQL:
 *          2.0以前，又SQLContext和HiveContext
 *          2.0以后，将二者进行综合——SparkSession
 *    SparkStreaming:
 *          JavaStreamingContext
 *          StreamingContext
 */
public class WordCountApp {
    public static void main(String[] args) {
        //step 1 创建sparkContext
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName(WordCountApp.class.getSimpleName());

        JavaSparkContext jsc = new JavaSparkContext(conf);

        //2.加载外部数据源
        JavaRDD<String> lines = jsc.textFile("E:/data/spark/hello.txt");

        //step 3.基于业务完成各种计算

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split("\\s+")).iterator();
            }
        });

        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        JavaPairRDD<String, Integer> ret = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        //step 4.完成数据的落地

        ret.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> kv) throws Exception {
                System.out.println(kv._1 + "--->" + kv._2);
            }
        });
        //step 5.释放资源
        jsc.stop();
    }
}
