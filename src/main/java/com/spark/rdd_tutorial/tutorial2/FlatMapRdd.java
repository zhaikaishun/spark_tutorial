package com.spark.rdd_tutorial.tutorial2;

import com.spark.rdd.tutorial.util.MyIterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by zhaikaishun on 2017/8/20.
 * 2.0 版本以上的用iterator
 */
public class FlatMapRdd {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("FlatMapRdd").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        jsc.setLogLevel("WARN");
        JavaRDD<String> lines = jsc.textFile("D:\\git\\spark_tutorial\\src\\main\\resources\\filter_sample.txt");
        JavaRDD<String> flatMapRDD = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public MyIterator<String> call(String s) throws Exception {
                String[] split = s.split("\\s+");
                MyIterator myIterator = new MyIterator(Arrays.asList(split));
                return myIterator;
            }
        });
        //循环打印
        flatMapRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }
}
