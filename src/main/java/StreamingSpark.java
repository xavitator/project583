import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.*;
import java.util.*;
import java.util.regex.Pattern;

import scala.Int;
import scala.Tuple2;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;

public class StreamingSpark{

    public JavaStreamingContext jssc;
    private SparkConf conf;
    private String file;
    private JavaDStream<String> stream;

    public StreamingSpark(String file) throws IOException {
        this.conf = new SparkConf().setMaster("local[*]").setAppName("inf583A");
        jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        this.file = file;
        JavaSparkContext jsc = new JavaSparkContext (jssc.ssc().sc());
        FileReader fr = new FileReader (this.file);
        BufferedReader br = new BufferedReader(fr);
        String line;
        int count = 0;
        ArrayList<String> batch = new ArrayList <>();
        Queue<JavaRDD<String>> rdds = new LinkedList <>();
        while ((line = br.readLine()) != null ) {
            if (count == 100)
            {
                JavaRDD<String> rdd = jsc.parallelize(batch);
                rdds.add(rdd);
                batch = new ArrayList<>();
                count = 0;
            }
            batch.add(line) ;
            count +=1;
        }
        JavaRDD<String> rdd = jsc.parallelize(batch) ;
        rdds.add(rdd) ;
        this.stream = jssc.queueStream(rdds, true);
    }

    public void largestInteger() throws IOException, InterruptedException {
        JavaDStream<Integer> integers = stream.window(Durations.seconds(10)).map(s -> Integer.parseInt(s));
        JavaDStream<Integer> biggestInt = integers.reduce((a,b) -> Integer.max(a,b));
        biggestInt.foreachRDD(x -> x.collect().stream().forEach(a -> System.out.println("Stream biggest integer :"+a)));
    }

    public void averageInteger() throws IOException, InterruptedException {
        /**
         *  We use Long instead of Integer for the first argument
         *  to be sure that the accumulator doesn't explode the integer limit
         */
        // with the windows function, we observe the computation of the algorithm
        JavaPairDStream<Long, Integer> integers = stream.window(Durations.seconds(10)).mapToPair(s -> {
            Long l = Long.parseLong(s);
            return new Tuple2<>(l, 1);
        });
        JavaDStream< Tuple2<Long, Integer> > reduceAll = integers.reduce((a,b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2));
        JavaDStream<Double> average = reduceAll.map(a -> (a._2 != 0) ? a._1 / (double) a._2 : 0);
        average.foreachRDD(x -> x.collect().stream().forEach(a -> System.out.println("Stream average integer :"+a)));
    }

    private static int getFirstSetBitPos(int i)
    {
        int max = 9;
        int mask = (1 << max) - 1;
        int n = i & mask;
        if(n == 0) return max;
        return (int)((Math.log10(n & -n)) / Math.log10(2));
    }

    public void countIntegers(){
//        JavaDStream<Integer> hashed = stream.window(Durations.seconds(10)).map(a -> ~ (Integer.parseInt(a) >> 1)); // uncomment if you want an other approximation
        JavaDStream<Integer> hashed = stream.window(Durations.seconds(10)).map(a -> Integer.parseInt(a)); // uncomment if you want an other approximation
        JavaPairDStream<Integer, Integer> integers = hashed.mapToPair(s -> new Tuple2<>(getFirstSetBitPos(s), 1));
        JavaPairDStream<Integer, Integer> groupByIndices = integers.reduceByKey((a,b) -> a+b);

        JavaDStream< Tuple2<Integer, Integer > > indices = groupByIndices.map(a -> new Tuple2<>(a._1, 1 << a._1));
        JavaDStream<Integer> tab_ind = indices.map(a -> a._2);
        JavaDStream<Integer> tab_finish = tab_ind.reduce((a,b) -> a | b);
        JavaDStream< Double > supposition = tab_finish.map(a ->  Math.pow(2, getFirstSetBitPos(a + 1)) / 0.77351);
        supposition.foreachRDD(x -> x.collect().stream().forEach(a -> System.out.println("Stream unique counts :"+a)));
    }
}
