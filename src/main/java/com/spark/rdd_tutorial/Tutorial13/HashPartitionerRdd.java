package com.spark.rdd_tutorial.Tutorial13;

import org.apache.spark.HashPartitioner;
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

/**
 * Created by zhaikaishun on 2017/8/20.
 */
public class HashPartitionerRdd {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("HashPartitionerRdd").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("WARN");
        JavaRDD<Tuple2<Integer, Integer>> tupRdd = sc.parallelize(Arrays.asList(new Tuple2<Integer, Integer>(1, 1), new Tuple2<Integer, Integer>(1, 2)
                , new Tuple2<Integer, Integer>(2, 3), new Tuple2<Integer, Integer>(2, 4)
                , new Tuple2<Integer, Integer>(3, 5), new Tuple2<Integer, Integer>(3, 6)
                , new Tuple2<Integer, Integer>(4, 7), new Tuple2<Integer, Integer>(4, 8)
                , new Tuple2<Integer, Integer>(5, 9), new Tuple2<Integer, Integer>(5, 10)
        ), 3);
        JavaPairRDD<Integer, Integer> pairRDD = JavaPairRDD.fromJavaRDD(tupRdd);
        JavaPairRDD<Integer, Integer> partitioned = pairRDD.partitionBy(new HashPartitioner(3));
        System.out.println("============HashPartitioner==================");
        printPartRDD(partitioned);


    }


    /**
     * 用来打印 JavaPairRDD<Integer, Integer> 类型的 rdd 的每个分区下的各个元素，这里顺便复习mapPartitionsWithIndex
     * 其实最好使用glom，就不用那么麻烦了，但是我这里没有使用
     * @param pairRDD
     */
    public static void printPartRDD(JavaPairRDD<Integer, Integer> pairRDD) {
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

        mapPartitionIndexRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Integer, Integer>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<Integer, Integer>> integerTuple2Tuple2) throws Exception {
                System.out.println(integerTuple2Tuple2);
            }
        });
    }
}
