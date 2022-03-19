import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.IOException;

public class Main {

    public static void sparkPartA(){
        Spark partA = new Spark("integers/integers.txt");
        System.out.println("Part A using Spark :");
        partA.largestInteger();
        partA.averageInteger();
        partA.uniqueIntegers();
        partA.countUniqueIntegers();
    }

    public static void streamingSparkPartA() throws IOException, InterruptedException {
        StreamingSpark partA = new StreamingSpark("integers/integers.txt");
        System.out.println("Part A using StreamingSpark :");
        partA.largestInteger();
        partA.averageInteger();
        partA.countIntegers();
        partA.jssc.start();
        partA.jssc.awaitTerminationOrTimeout(10000);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
        sparkPartA();
        streamingSparkPartA();
    }
}
