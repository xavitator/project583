package A;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Spark {

    private JavaSparkContext sc;
    JavaRDD<String> input;

    public Spark(String file) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("inf583A");
        this.sc = new JavaSparkContext(conf);
        this.input = sc.textFile(file);
    }

    public void largestInteger(){
        JavaRDD<Integer> integers = input.map(s -> Integer.parseInt(s));
        Integer biggestInt = integers.reduce((a,b) -> Integer.max(a,b));
        System.out.println("Largest integer : " + biggestInt);
    }

    public void averageInteger(){
        /**
         *  We use Long instead of Integer for the first argument
         *  to be sure that the accumulator doesn't explode the integer limit
         */
        JavaPairRDD<Long, Integer> integers = input.mapToPair(s -> {
           Long l = Long.parseLong(s);
           return new Tuple2<>(l, 1);
        });
        Tuple2<Long, Integer> reduceAll = integers.reduce((a,b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2));
        double res = reduceAll._1 / (double) reduceAll._2;
        System.out.println("Average : " + res);
    }

    public void uniqueIntegers(){
        JavaPairRDD<String, Integer> integers = input.mapToPair(s -> new Tuple2<> (s, 1));
        JavaPairRDD<String, Integer> uniqueInt = integers.reduceByKey((a,b) -> a);
        JavaRDD<String> res = uniqueInt.map(el -> el._1);
        System.out.println("Unique integers :");
        for (String el :
                res.collect()) {
            System.out.print(el + ", ");
        }
        System.out.println();
    }

    public void countUniqueIntegers(){
        JavaPairRDD<String, Integer> integers = input.mapToPair(s -> new Tuple2<> (s, 1));
        JavaPairRDD<String, Integer> countInt = integers.reduceByKey((a,b) -> 1);
        JavaRDD<Integer> countUnique = countInt.map(a -> a._2);
        int res = countUnique.reduce((a,b) -> a+b);
        System.out.println("Count unique : "+res);
    }
}
