package com.spark.rdd_tutorial.tutorial8;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;
import java.util.Arrays;
import java.util.Map;

public class JoinRDD {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("ReduceByKey").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("WARN");

        JavaRDD<Tuple2<Integer,Integer>> rddPre = sc.parallelize(Arrays.asList(new Tuple2(1,2)
                , new Tuple2(3,4)
                , new Tuple2(3,6)));
        JavaRDD<Tuple2<Integer,Integer>> otherPre = sc.parallelize(Arrays.asList(new Tuple2(3,10),new Tuple2(4,8)));

        //JavaRDD转换成JavaPairRDD
        JavaPairRDD<Integer, Integer> rdd = JavaPairRDD.fromJavaRDD(rddPre);
        JavaPairRDD<Integer, Integer> other = JavaPairRDD.fromJavaRDD(otherPre);
        //subtractByKey
        JavaPairRDD<Integer, Integer> subRDD = rdd.subtractByKey(other);

        //join
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinRDD =  rdd.join(other);
        //fullOutJoin
        JavaPairRDD<Integer, Tuple2<Optional<Integer>, Optional<Integer>>> fullOutJoinRDD = rdd.fullOuterJoin(other);
        //leftOuterJoin
        JavaPairRDD<Integer, Tuple2<Integer, Optional<Integer>>> leftOutJoinRDD = rdd.leftOuterJoin(other);

        //rightOutJoin
        JavaPairRDD<Integer, Tuple2<Optional<Integer>, Integer>> rightOutJoinRDD = rdd.rightOuterJoin(other);
        //输出看效果
        Map<Integer, Integer> subMap = subRDD.collectAsMap();
        System.out.println("-------------subRDD-------------");
        for (Integer key : subMap.keySet()) {
            System.out.println("subRDD: "+key+", "+subMap.get(key));
        }
        Map<Integer, Tuple2<Integer, Integer>> joinMap = joinRDD.collectAsMap();
        System.out.println("-------------joinRDD-------------");
        for (Integer key : joinMap.keySet()) {
            System.out.println("join: "+key+", Tuple("+joinMap.get(key)._1+","+joinMap.get(key)._2+")");
        }
        Map<Integer, Tuple2<Optional<Integer>, Optional<Integer>>> fullOutJoinMap = fullOutJoinRDD.collectAsMap();
        System.out.println("-------------fullOutJoinRDD-------------");
        for (Integer key : fullOutJoinMap.keySet()) {
            System.out.println("fullOutJoinRDD: "+key+", Tuple("+fullOutJoinMap.get(key)._1+","+fullOutJoinMap.get(key)._2+")");
        }

        Map<Integer, Tuple2<Integer, Optional<Integer>>> leftOutJoinMap = leftOutJoinRDD.collectAsMap();
        System.out.println("-------------leftOutJoinRDD-------------");
        for (Integer key : leftOutJoinMap.keySet()) {
            System.out.println("leftOutJoinRDD: "+key+", Tuple("+leftOutJoinMap.get(key)._1+","+leftOutJoinMap.get(key)._2+")");
        }

        Map<Integer, Tuple2<Optional<Integer>, Integer>> rightOutJoinMap = rightOutJoinRDD.collectAsMap();
        System.out.println("-------------rightOutJoinRDD-------------");
        for (Integer key : rightOutJoinMap.keySet()) {
            System.out.println("rightOutJoinRDD: "+key+", Tuple("+rightOutJoinMap.get(key)._1+","+rightOutJoinMap.get(key)._2+")");
        }

    }
}
