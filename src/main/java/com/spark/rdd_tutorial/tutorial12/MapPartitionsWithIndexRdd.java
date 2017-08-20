package com.spark.rdd_tutorial.tutorial12;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by zhaikaishun on 2017/8/20.
 */
public class MapPartitionsWithIndexRdd {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("MapPartitionsWithIndexRdd").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("WARN");
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3);
        JavaRDD<Tuple2<Integer, Integer>> tuple2JavaRDD = rdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<Tuple2<Integer, Integer>>>() {
            @Override
            public Iterator<Tuple2<Integer, Integer>> call(Integer partIndex, Iterator<Integer> it) throws Exception {
                ArrayList<Tuple2<Integer, Integer>> tuple2s = new ArrayList<>();
                while (it.hasNext()) {
                    int next = it.next();
                    tuple2s.add(new Tuple2<>(partIndex, next));
                }
                return tuple2s.iterator();
            }
        }, false);

        tuple2JavaRDD.foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
            @Override
            public void call(Tuple2<Integer, Integer> tp2) throws Exception {
                System.out.println(tp2);
            }
        });
        /*mapPartitionsWithIndex 统计键值对中的各个分区的元素 */
        JavaRDD<Tuple2<Integer, Integer>> rdd1 = sc.parallelize(Arrays.asList(new Tuple2<Integer, Integer>(1, 1), new Tuple2<Integer, Integer>(1, 2)
                , new Tuple2<Integer, Integer>(2, 3), new Tuple2<Integer, Integer>(2, 4)
                , new Tuple2<Integer, Integer>(3, 5), new Tuple2<Integer, Integer>(3, 6)
                , new Tuple2<Integer, Integer>(4, 7), new Tuple2<Integer, Integer>(4, 8)
                , new Tuple2<Integer, Integer>(5, 9), new Tuple2<Integer, Integer>(5, 10)
        ), 3);
        JavaPairRDD<Integer, Integer> pairRDD = JavaPairRDD.fromJavaRDD(rdd1);

        JavaRDD<Tuple2<Integer, Tuple2<Integer, Integer>>> mapPartitionIndexRDD = pairRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<Integer, Integer>>, Iterator<Tuple2<Integer, Tuple2<Integer, Integer>>>>() {
            @Override
            public Iterator<Tuple2<Integer, Tuple2<Integer, Integer>>> call(Integer partIndex, Iterator<Tuple2<Integer, Integer>> tuple2Iterator) {
                ArrayList<Tuple2<Integer, Tuple2<Integer, Integer>>> tuple2s = new ArrayList<>();

                while (tuple2Iterator.hasNext()) {
                    Tuple2<Integer, Integer> next = tuple2Iterator.next();
                    tuple2s.add(new Tuple2<Integer, Tuple2<Integer, Integer>>(partIndex, next));
                }
                return tuple2s.iterator();
            }
        }, false);
        System.out.println("mapPartitionsWithIndex 统计键值对中的各个分区的元素");
        mapPartitionIndexRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Integer, Integer>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<Integer, Integer>> integerTuple2Tuple2) throws Exception {
                System.out.println(integerTuple2Tuple2);
            }
        });

        /*补充：打印各个分区的操作，可以使用 glom 的方法*/
        System.out.println("打印各个分区的操作，可以使用 glom 的方法");
        JavaRDD<List<Tuple2<Integer, Integer>>> glom = pairRDD.glom();
        glom.foreach(new VoidFunction<List<Tuple2<Integer, Integer>>>() {
            @Override
            public void call(List<Tuple2<Integer, Integer>> tuple2s) throws Exception {
                System.out.println(tuple2s);
            }
        });

    }
}
