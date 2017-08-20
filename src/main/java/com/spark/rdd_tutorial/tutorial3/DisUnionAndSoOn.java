package com.spark.rdd_tutorial.tutorial3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by zhaikaishun on 2017/8/20.
 */
public class DisUnionAndSoOn {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("tutorial3").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("WARN");

        // Distinct
        JavaRDD<String> RDD1 = sc.parallelize(Arrays.asList("aa", "aa", "bb", "cc", "dd"));
        JavaRDD<String> distinctRDD = RDD1.distinct();
        List<String> collect = distinctRDD.collect();
        System.out.println("*************** Distinct ***********");
        for (String str:collect) {
            System.out.println(str);
        }

        //union
        JavaRDD<String> RDD2 = sc.parallelize(Arrays.asList("aa","dd","ff"));
        JavaRDD<String> unionRDD = RDD1.union(RDD2);
        collect = unionRDD.collect();
        System.out.println("\n"+"*************** Union ***********");
        for (String str:collect) {
            System.out.println(str);
        }

        // intersection
        JavaRDD<String> intersectionRDD = RDD1.intersection(RDD2);
        collect = intersectionRDD.collect();
        System.out.println("\n"+"*************** intersection ***********");
        for (String str:collect) {
            System.out.println(str);
        }
        //subtract
        JavaRDD<String> subtractRDD = RDD1.subtract(RDD2);
        collect = subtractRDD.collect();
        System.out.println("\n"+"*************** Subtract ***********");
        for (String str:collect) {
            System.out.println(str);
        }
        //cartesian
        JavaPairRDD<String, String> cartesian = RDD1.cartesian(RDD2);
        List<Tuple2<String, String>> collect1 = cartesian.collect();
        System.out.println("\n"+"*************** Cartesian ***********");
        for (Tuple2<String, String> tp:collect1) {
            System.out.println("("+tp._1+" "+tp._2+")");
        }

    }
}
