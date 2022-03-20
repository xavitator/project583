package MatrixMultiplication;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TwoMapreduce
{
    public static void main( String[] args )
    {
    	//Initiate the input file and output folder
    	String inputFile = "edgelist.txt";
    	String outputFolder = "output";

    	// Create a Java Spark Context
    	SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("Spark");
    	JavaSparkContext sc = new JavaSparkContext(conf);

		// Create Vector map
		JavaRDD<String> input_idlabels = sc.textFile("idslabels.txt");
		Map<Integer, Double> vector = input_idlabels.mapToPair( s -> new Tuple2<Integer,Double>( Integer.parseInt(s.split(" ")[0]), 1.0/64375)).collectAsMap();
		
    	JavaRDD<String> input = sc.textFile(inputFile);

		// MAP
		JavaPairRDD<Integer,List<String>> mapped_matrix_1 = input.mapToPair(s -> {
			List<String> line = new ArrayList<String>(Arrays.asList(s.split(" ")));
			Integer entry =  Integer.parseInt(line.get(0));
			line.remove(0);
			return new Tuple2<Integer, List<String>>(entry, line);
		});
		// Flatten
		JavaPairRDD<Integer,String> mapped_matrix_2 = mapped_matrix_1.flatMapValues(value ->
		value.iterator());
		
		//Parse the strings to double
		JavaPairRDD<Integer, Double> mapped_matrix = mapped_matrix_2.mapValues(Integer::parseInt).mapValues(vector::get);
				
		//REDUCE
		JavaPairRDD<Integer,Double> reduce_output = mapped_matrix.reduceByKey((a,b) -> a+b);

		reduce_output.foreach(data -> System.out.println(data._1 + " " + data ._2));
    }

}