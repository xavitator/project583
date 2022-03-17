import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Int;
import scala.Tuple2;

public class SparkA {

    private JavaSparkContext sc;
    JavaRDD<String> input;

    public SparkA(String file) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("inf583A");
        this.sc = new JavaSparkContext(conf);
        this.input = sc.textFile(file);
    }

    public int largestInteger(){
        JavaRDD<Integer> integers = input.map(s -> Integer.parseInt(s));
        Integer biggestInt = integers.reduce((a,b) -> Integer.max(a,b));
        return biggestInt;
    }

    public double averageInteger(){
        /**
         *  We use Long instead of Integer for the first argument
         *  to be sure that the accumulator doesn't explode the integer limit
         */
        JavaPairRDD<Long, Integer> integers = input.mapToPair(s -> {
           Long l = Long.parseLong(s);
           return new Tuple2<>(l, 1);
        });
        JavaPairRDD<Long, Integer> reduceByIntegers = integers.reduceByKey((a, b) -> a + b);
        Tuple2<Long, Integer> agregation = reduceByIntegers.reduce((a,b) -> new Tuple2<Long, Integer>(a._1 + b._1 * b._2, a._2 + b._2) );
        return agregation._1 / (double) agregation._2;
    }

    public JavaRDD<String> uniqueIntegers(){
        JavaPairRDD<String, Integer> integers = input.mapToPair(s -> new Tuple2<> (s, 1));
        JavaPairRDD<String, Integer> uniqueInt = integers.reduceByKey((a,b) -> a);
        JavaRDD<String> res = uniqueInt.map(el -> el._1);
        return res;
    }

    public JavaPairRDD<String, Integer> countIntegers(){
        JavaPairRDD<String, Integer> integers = input.mapToPair(s -> new Tuple2<> (s, 1));
        JavaPairRDD<String, Integer> countInt = integers.reduceByKey((a,b) -> a + b);
        return countInt;
    }

    public static void main(String[] args )
    {
        SparkA partA = new SparkA("integers/integers.txt");
        int exo1 = partA.largestInteger();
        double exo2 = partA.averageInteger();
        JavaRDD<String> exo3 = partA.uniqueIntegers();
        JavaPairRDD<String, Integer> exo4 = partA.countIntegers();
        System.out.println(exo1);
        System.out.println(exo2);
        System.out.println(exo3);
        System.out.println(exo4);
    }
}
