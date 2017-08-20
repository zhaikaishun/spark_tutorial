package com.spark.rdd_tutorial.tutorial4;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * Created by zhaikaishun on 2017/8/20.
 */
public class MapToPairRdd {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("MapToPairRdd").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("WARN");

        JavaRDD<String> lines = sc.textFile("D:\\git\\spark_tutorial\\src\\main\\resources\\filter_sample.txt");
        //输入的是一个string的字符串，输出的是一个(String, Integer) 的map
        JavaPairRDD<String, Integer> pairRDD = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s.split("\\s+")[0], 1);
            }
        });

        //输出
        pairRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tp) throws Exception {
                System.out.println("key: "+tp._1+" value: "+tp._2);
            }
        });


    }
}
