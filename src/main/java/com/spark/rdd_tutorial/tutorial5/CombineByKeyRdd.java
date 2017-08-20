package com.spark.rdd_tutorial.tutorial5;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Map;

/**
 * Created by zhaikaishun on 2017/8/20.
 */
public class CombineByKeyRdd {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("CombineByKeyRdd").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("WARN");
        ArrayList<ScoreDetail> scoreDetails = new ArrayList<>();
        scoreDetails.add(new ScoreDetail("xiaoming", "Math", 98));
        scoreDetails.add(new ScoreDetail("xiaoming", "English", 88));
        scoreDetails.add(new ScoreDetail("wangwu", "Math", 75));
        scoreDetails.add(new ScoreDetail("wangwu", "Englist", 78));
        scoreDetails.add(new ScoreDetail("lihua", "Math", 90));
        scoreDetails.add(new ScoreDetail("lihua", "English", 80));
        scoreDetails.add(new ScoreDetail("zhangsan", "Math", 91));
        scoreDetails.add(new ScoreDetail("zhangsan", "English", 80));

        JavaRDD<ScoreDetail> scoreDetailsRDD = sc.parallelize(scoreDetails);

        JavaPairRDD<String, ScoreDetail> pairRDD = scoreDetailsRDD.mapToPair(new PairFunction<ScoreDetail, String, ScoreDetail>() {
            @Override
            public Tuple2<String, ScoreDetail> call(ScoreDetail scoreDetail) throws Exception {

                return new Tuple2<>(scoreDetail.studentName, scoreDetail);
            }
        });
//        new Function<ScoreDetail, Float,Integer>();

        Function<ScoreDetail, Tuple2<Float, Integer>> createCombine = new Function<ScoreDetail, Tuple2<Float, Integer>>() {
            @Override
            public Tuple2<Float, Integer> call(ScoreDetail scoreDetail) throws Exception {
                return new Tuple2<>(scoreDetail.score, 1);
            }
        };

        // Function2传入两个值，返回一个值
        Function2<Tuple2<Float, Integer>, ScoreDetail, Tuple2<Float, Integer>> mergeValue = new Function2<Tuple2<Float, Integer>, ScoreDetail, Tuple2<Float, Integer>>() {
            @Override
            public Tuple2<Float, Integer> call(Tuple2<Float, Integer> tp, ScoreDetail scoreDetail) throws Exception {
                return new Tuple2<>(tp._1 + scoreDetail.score, tp._2 + 1);
            }
        };
        Function2<Tuple2<Float, Integer>, Tuple2<Float, Integer>, Tuple2<Float, Integer>> mergeCombiners = new Function2<Tuple2<Float, Integer>, Tuple2<Float, Integer>, Tuple2<Float, Integer>>() {
            @Override
            public Tuple2<Float, Integer> call(Tuple2<Float, Integer> tp1, Tuple2<Float, Integer> tp2) throws Exception {
                return new Tuple2<>(tp1._1 + tp2._1, tp1._2 + tp2._2);
            }
        };
        JavaPairRDD<String, Tuple2<Float,Integer>> combineByRDD  = pairRDD.combineByKey(createCombine,mergeValue,mergeCombiners);

        //打印平均数
        Map<String, Tuple2<Float, Integer>> stringTuple2Map = combineByRDD.collectAsMap();
        for ( String et:stringTuple2Map.keySet()) {
            System.out.println(et+" "+stringTuple2Map.get(et)._1/stringTuple2Map.get(et)._2);
        }
    }
}
