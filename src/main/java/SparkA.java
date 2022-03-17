import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkA {

    private JavaSparkContext sc;
    JavaRDD<String> input;

    public SparkA(String file) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("inf583A");
        this.sc = new JavaSparkContext(conf);
        this.input = sc.textFile(file);
    }

    public int largestInteger(String savedFile){
        JavaPairRDD<Integer, Integer> integers = input.mapToPair(
                s -> {
                    Integer l = Integer.parseInt(s);
                    return new Tuple2<>(0, l);
                }
        );
        JavaPairRDD<Integer, Integer> biggestInt = integers.reduceByKey((a,b) -> Integer.max(a,b));
        userPairFinal.saveAsTextFile("exercise3");
    }

    public static void main(String[] args )
    {
        String inputFile = "ratings.csv";
        // Create a Java Spark Context
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load our input data.
        JavaRDD<String> input = sc.textFile(inputFile);

        //Exercise 1: Compute the number of movies rated for each user by using the file
        // For each line output pair (user, 1).
        JavaPairRDD<String, Integer> users = input.mapToPair(s ->
        { String[] line = s.split(",");
            return new Tuple2<>(line[0], 1);});

        // For simplicity, we assume that each user will rate a movie just once
        JavaPairRDD<String, Integer> moviesPerUser = users.reduceByKey((a,b) -> a+b);


        // Save the word count back out to an output folder.
        moviesPerUser.saveAsTextFile("exercise1");


        //Exercise 2: Compute the average rating for each movie
        // We output movie, (rating, 1)
        JavaPairRDD<String, Tuple2<Double, Integer>> ratings = input.mapToPair(s ->
        { String[] line = s.split(",");
            return new Tuple2<>(line[1], new Tuple2<>(Double.parseDouble(line[2]),1));});


        // We sum the ratings and the values of 1
        JavaPairRDD<String, Tuple2<Double, Integer>> sumRatings = ratings.reduceByKey((a,b)->
                new Tuple2<>(a._1 + b._1, a._2 + b._2));

        //We compute the average using the sum of ratings and the count of ratings
        JavaPairRDD<String, Double> averageRatings = sumRatings.mapValues(a -> new Double(a._1/a._2));

        averageRatings.saveAsTextFile("exercise2");



        // Exercise 3: Find the pairs of users that rated at least ten common movies
        // We output the pair (movie, user)
        JavaPairRDD<String, String> movies = input.mapToPair(s ->
        { String[] line = s.split(",");
            return new Tuple2<>(line[1], line[0]);});
        // We have per each movie all the users. We can output as keys pairs of users and as value the movie

        // We get all the pairs movie (user1, user2) -> we filter the pairs where user1<user2 to get only unique pairs
        JavaPairRDD<String, Tuple2<String, String>> moviesUserPair = movies.join(movies).filter(
                a -> {if (a._2._1.compareTo(a._2._2) == 1) return true; return false;});

        JavaPairRDD<Tuple2<String, String>, Integer> userPairMovies = moviesUserPair.mapToPair( a -> new Tuple2<>(a._2, 1));


        // pay attention to filter -> it will keep the elements for which the function is true
        JavaPairRDD<Tuple2<String, String>, Integer> userPairFinal = userPairMovies.reduceByKey((a,b) -> a+b).filter(a -> a._2>9);

        userPairFinal.saveAsTextFile("exercise3");

    }
}
