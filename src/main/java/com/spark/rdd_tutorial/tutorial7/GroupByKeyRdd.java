package com.spark.rdd_tutorial.tutorial7;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by zhaikaishun on 2017/8/20.
 */
public class GroupByKeyRdd {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("GroupByKeyRdd").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("WARN");

        JavaRDD<Tuple2<String,Float>> scoreDetails = sc.parallelize(Arrays.asList(new Tuple2("xiaoming", 75)
                , new Tuple2("xiaoming", 90)
                , new Tuple2("zs1", 95)
                , new Tuple2("lihua", 188)
                , new Tuple2("xiaoming", 188)
                , new Tuple2("zs1", 188)
                , new Tuple2("lihua", 188)),3);
        //将JavaRDD<Tuple2<String,Float>> 类型转换为 JavaPairRDD<String, Float>
        JavaPairRDD<String, Float> scoreMapRDD = JavaPairRDD.fromJavaRDD(scoreDetails);
        JavaRDD<List<Tuple2<String, Float>>> glom = scoreMapRDD.glom();
        glom.foreach(new VoidFunction<List<Tuple2<String, Float>>>() {
            @Override
            public void call(List<Tuple2<String, Float>> tuple2s) throws Exception {
                System.out.println(tuple2s);
                System.out.println("######");
            }
        });
        System.out.println("++++++++++++++");
        scoreMapRDD.groupByKey().glom().foreach(new VoidFunction<List<Tuple2<String, Iterable<Float>>>>() {
            @Override
            public void call(List<Tuple2<String, Iterable<Float>>> tuple2s) throws Exception {
                System.out.println(tuple2s);
                System.out.println("*******");
            }
        });

//        Map<String, Iterable<Float>> resultMap = scoreMapRDD.groupByKey().collectAsMap();
//        for (String key:resultMap.keySet()) {
//            System.out.println("("+key+", "+resultMap.get(key)+")");
//        }


    }
}
