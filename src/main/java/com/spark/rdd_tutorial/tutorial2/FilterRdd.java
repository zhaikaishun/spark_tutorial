package com.spark.rdd_tutorial.tutorial2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.List;

/**
 * Created by zhaikaishun on 2017/8/20.
 */
public class FilterRdd {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("FilterRdd").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        jsc.setLogLevel("WARN");
        JavaRDD<String> lines = jsc.textFile("D:\\git\\spark_tutorial\\src\\main\\resources\\filter_sample.txt");
        JavaRDD<String> zksRDD = lines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s.contains("zks");
            }
        });
        //打印内容
        List<String> zksCollect = zksRDD.collect();
        for (String str:zksCollect) {
            System.out.println(str);
        }
    }
}
