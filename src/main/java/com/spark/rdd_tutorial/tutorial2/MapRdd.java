package com.spark.rdd_tutorial.tutorial2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;

/**
 * Created by zhaikaishun on 2017/8/20.
 * 貌似 map 在java 中没多大用处
 */
public class MapRdd {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("MapRdd").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        jsc.setLogLevel("WARN");
        JavaRDD<String> lines = jsc.textFile("D:\\git\\spark_tutorial\\src\\main\\resources\\filter_sample.txt");
        JavaRDD<Iterable<String>> mapRDD = lines.map(new Function<String, Iterable<String>>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                String[] split = s.split("\\s+");
                return Arrays.asList(split);
            }
        });
        //循环打印
        mapRDD.foreach(new VoidFunction<Iterable<String>>() {
            @Override
            public void call(Iterable<String> strings) throws Exception {
                System.out.println(strings);
            }
        });
    }
}
