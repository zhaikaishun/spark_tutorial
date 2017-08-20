package com.spark.rdd_tutorial.tutorial12;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by zhaikaishun on 2017/8/20.
 */
public class MapPartitionsRdd {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("MapPartitionsRdd").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("WARN");

        JavaRDD<Integer> rdd = sc.parallelize(
                Arrays.asList(1,2,3,4,5,6,7,8,9,10));
/*==========================把每一个元素平方 =======================================*/
        JavaRDD<Integer> mapPartitionRDD = rdd.mapPartitions(new FlatMapFunction<Iterator<Integer>, Integer>() {
            @Override
            public Iterator<Integer> call(Iterator<Integer> it) throws Exception {
                ArrayList<Integer> results = new ArrayList<>();
                while (it.hasNext()) {
                    int i = it.next();
                    results.add(i*i);
                }
                return results.iterator();
            }
        });
        System.out.println("把每一个元素平方");
        mapPartitionRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

        /* ================把每一个数字i变成一个map(i,i*i)的形式======================================== */

        JavaRDD<Tuple2<Integer, Integer>> tuple2JavaRDD = rdd.mapPartitions(new FlatMapFunction<Iterator<Integer>, Tuple2<Integer, Integer>>() {
            @Override
            public Iterator<Tuple2<Integer, Integer>> call(Iterator<Integer> it) throws Exception {
                ArrayList<Tuple2<Integer, Integer>> tuple2s = new ArrayList<>();
                while (it.hasNext()) {
                    Integer next = it.next();
                    tuple2s.add(new Tuple2<Integer, Integer>(next, next * next));
                }
                return tuple2s.iterator();
            }
        });
        System.out.println("把每一个数字i变成一个map(i,i*i)的形式");
        tuple2JavaRDD.foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
            @Override
            public void call(Tuple2<Integer, Integer> tp2) throws Exception {
                System.out.println(tp2);
            }
        });
    }
}
