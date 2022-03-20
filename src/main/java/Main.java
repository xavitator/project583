import A.Spark;
import A.StreamingSpark;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;

public class Main {

    public static void sparkPartA(){
        Spark partA = new Spark("integers/integers.txt");
        System.out.println("Part A using A.Spark :");
        partA.largestInteger();
        partA.averageInteger();
        partA.uniqueIntegers();
        partA.countUniqueIntegers();
    }

    public static void streamingSparkPartA() throws IOException, InterruptedException {
        StreamingSpark partA = new StreamingSpark("integers/integers.txt");
        System.out.println("Part A using A.StreamingSpark :");
        partA.largestInteger();
        partA.averageInteger();
        partA.countIntegers();
        partA.jssc.start();
        partA.jssc.awaitTerminationOrTimeout(10000);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
//        sparkPartA();
        streamingSparkPartA();
    }
}
