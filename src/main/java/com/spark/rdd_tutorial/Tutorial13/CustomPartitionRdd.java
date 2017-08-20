package com.spark.rdd_tutorial.Tutorial13;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 自定义分区
 * Created by zhaikaishun on 2017/8/20.
 */
public class CustomPartitionRdd {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("CustomPartitionRdd").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("WARN");
        JavaRDD<Tuple2<Integer, Integer>> tupRdd = sc.parallelize(Arrays.asList(new Tuple2<Integer, Integer>(1, 1), new Tuple2<Integer, Integer>(1, 2)
                , new Tuple2<Integer, Integer>(2, 3), new Tuple2<Integer, Integer>(2, 4)
                , new Tuple2<Integer, Integer>(3, 5), new Tuple2<Integer, Integer>(3, 6)
                , new Tuple2<Integer, Integer>(4, 7), new Tuple2<Integer, Integer>(4, 8)
                , new Tuple2<Integer, Integer>(5, 9), new Tuple2<Integer, Integer>(5, 10)
        ), 3);
        JavaPairRDD<Integer, Integer> pairRDD = JavaPairRDD.fromJavaRDD(tupRdd);

        System.out.println("============CustomPartition==================");
        JavaPairRDD<Integer, Integer> customPart = pairRDD.partitionBy(new JavaCustomPart(3));
        HashPartitionerRdd.printPartRDD(customPart);

    }
}
