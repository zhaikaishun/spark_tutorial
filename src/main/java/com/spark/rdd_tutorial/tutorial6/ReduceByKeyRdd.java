package com.spark.rdd_tutorial.tutorial6;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by zhaikaishun on 2017/8/20.
 */
public class ReduceByKeyRdd {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("ReduceByKey").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("WARN");
        JavaRDD<String> lines = sc.textFile("D:\\git\\spark_tutorial\\src\\main\\resources\\filter_sample.txt");

        JavaPairRDD<String, Integer> wordPairRDD = lines.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(String s) throws Exception {
                ArrayList<Tuple2<String, Integer>> tpLists = new ArrayList<Tuple2<String, Integer>>();
                String[] split = s.split("\\s+");
                for (int i = 0; i <split.length ; i++) {
                    Tuple2 tp = new Tuple2<String,Integer>(split[i], 1);
                    tpLists.add(tp);
                }
                return tpLists.iterator();
            }
        });

        JavaPairRDD<String, Integer> wordCountRDD = wordPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        });
        Map<String, Integer> collectAsMap = wordCountRDD.collectAsMap();
        for (String key:collectAsMap.keySet()) {
            System.out.println("("+key+","+collectAsMap.get(key)+")");
        }
    }
}
