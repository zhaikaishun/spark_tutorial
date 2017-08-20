package com.spark.rdd_tutorial.tutorial7;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Arrays;
import java.util.Map;

/**
 * Created by zhaikaishun on 2017/8/20.
 */
public class CogroupRdd {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("CogroupRdd").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("WARN");

        JavaRDD<Tuple2<String,Float>> scoreDetails1 = sc.parallelize(Arrays.asList(new Tuple2("xiaoming", 75)
                , new Tuple2("xiaoming", 90)
                , new Tuple2("lihua", 95)
                , new Tuple2("lihua", 96)));
        JavaRDD<Tuple2<String,Float>> scoreDetails2 = sc.parallelize(Arrays.asList(new Tuple2("xiaoming", 75)
                , new Tuple2("lihua", 60)
                , new Tuple2("lihua", 62)));
        JavaRDD<Tuple2<String,Float>> scoreDetails3 = sc.parallelize(Arrays.asList(new Tuple2("xiaoming", 75)
                , new Tuple2("xiaoming", 45)
                , new Tuple2("lihua", 24)
                , new Tuple2("lihua", 57)));

        JavaPairRDD<String, Float> scoreMapRDD1 = JavaPairRDD.fromJavaRDD(scoreDetails1);
        JavaPairRDD<String, Float> scoreMapRDD2 = JavaPairRDD.fromJavaRDD(scoreDetails2);
        JavaPairRDD<String, Float> scoreMapRDD3 = JavaPairRDD.fromJavaRDD(scoreDetails2);

        JavaPairRDD<String, Tuple3<Iterable<Float>, Iterable<Float>, Iterable<Float>>> cogroupRDD = (JavaPairRDD<String, Tuple3<Iterable<Float>, Iterable<Float>, Iterable<Float>>>) scoreMapRDD1.cogroup(scoreMapRDD2, scoreMapRDD3);
        Map<String, Tuple3<Iterable<Float>, Iterable<Float>, Iterable<Float>>> tuple3 = cogroupRDD.collectAsMap();
        for (String key:tuple3.keySet()) {
            System.out.println("("+key+", "+tuple3.get(key)+")");
        }

    }
}
