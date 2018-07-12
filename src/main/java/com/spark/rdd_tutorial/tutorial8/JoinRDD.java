package com.spark.rdd_tutorial.tutorial8;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.VoidFunction;
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
        System.out.println("-------------joinRDD-------------");
        joinRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Integer, Integer>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<Integer, Integer>> tptp) throws Exception {
                System.out.println("key: "+tptp._1+", value: "+tptp._2._1+","+tptp._2._2);
            }
        });

        System.out.println("-------------fullOutJoinRDD-------------");
        JavaPairRDD<Integer, Tuple2<Optional<Integer>, Optional<Integer>>> fullOutJoinRDD = rdd.fullOuterJoin(other);
        fullOutJoinRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Optional<Integer>, Optional<Integer>>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<Optional<Integer>, Optional<Integer>>> tptp2) throws Exception {
                System.out.println("key: "+ tptp2._1+" value: "+tptp2._2._1+", "+tptp2._2._2);
            }
        });

        System.out.println("-------------leftOutJoinRDD-------------");
        JavaPairRDD<Integer, Tuple2<Integer, Optional<Integer>>> leftOutJoinRDD = rdd.leftOuterJoin(other);
        leftOutJoinRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Integer, Optional<Integer>>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<Integer, Optional<Integer>>> tptp2) throws Exception {
                System.out.println("key: "+tptp2._1+" value:  "+tptp2._2._1+", "+tptp2._2._2);
            }
        });

        System.out.println("-------------rightOutJoinRDD-------------");
        JavaPairRDD<Integer, Tuple2<Optional<Integer>, Integer>> rightOutJoinRDD = rdd.rightOuterJoin(other);
        rightOutJoinRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Optional<Integer>, Integer>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<Optional<Integer>, Integer>> tptp2) throws Exception {
                System.out.println("key: "+tptp2._1+"value:  "+tptp2._2._1+", "+tptp2._2._2);
            }
        });

    }
}
