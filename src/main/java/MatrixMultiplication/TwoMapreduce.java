package MatrixMultiplication;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.io.IOException;

import java.util.StringTokenizer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;


import scala.Tuple2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TwoMapreduce{

	public static class MatrixMapperVector2_1 extends Mapper<Object, Text, Text, Text>

	{

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException

		{
			Configuration conf = context.getConfiguration();
			String line = value.toString();
			Text outputKey = new Text();
			Text outputValue = new Text();
			outputKey.set(line.split("\t")[0]);
			outputValue.set("N," + "0" + "," + line.split("\t")[1]);
			context.write(outputKey, outputValue);
		}
	}

	public static class MatrixMapperMatrix2_1 extends Mapper<Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			Configuration conf = context.getConfiguration();
			String[] nodes = value.toString().split(" ");
			Text outputKey = new Text();
			Text outputValue = new Text();
			for (int i = 1; i < nodes.length; i++) {
				outputKey.set(nodes[i]);
				outputValue.set("M," + nodes[0] + "," + "1");
				context.write(outputKey, outputValue);
			}
		}
	}

	public static class MatrixReducer2_1 extends Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			String[] value;
			HashMap<Integer, Float> hashA = new HashMap<Integer, Float>();
			HashMap<Integer, Float> hashB = new HashMap<Integer, Float>();
			for (Text val : values)
			{
				value = val.toString().split(",");
				if (value[0].equals("M"))
				{
					hashA.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
				}
				else
				{
					hashB.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
				}
			}
			float a_ij;
			float b_jk;

			for (Entry<Integer, Float> b : hashB.entrySet()) {
				float result = 0;
				context.write(new Text(key.toString() + "," + Integer.toString(b.getKey())),
						new Text(Float.toString(result)));
			}

			for (Entry<Integer, Float> a : hashA.entrySet()) {
				for (Entry<Integer, Float> b : hashB.entrySet()) {
					a_ij = a.getValue();
					b_jk = b.getValue();
					float result = a_ij * b_jk;
					context.write(new Text(Integer.toString(a.getKey()) + "," + Integer.toString(b.getKey())),
							new Text(Float.toString(result)));
				}
			}
		}
	}

	public static class MatrixMapper2_2 extends Mapper<Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			Configuration conf = context.getConfiguration();
			context.write(new Text(value.toString().split("\t")[0]), new Text(value.toString().split("\t")[1]));
		}
	}

	public static class MatrixReducer2_2 extends Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			String value;
			float result = (float) 0.0;
			for (Text val : values)
			{
				value = val.toString();
				result += Float.parseFloat(value);
			}
			context.write(new Text(key.toString().split(",")[0]), new Text(Float.toString(result)));
		}
	}

	static void matrixMultiplicationUsingTwoMapReduce(String input, String inputR, String outputFolder)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		// M is an m-by-n matrix; N is an n-by-p matrix.
		conf.set("m", "64375");
		conf.set("n", "64375");
		conf.set("p", "1");

		Job job1 = Job.getInstance(conf, "MatrixMultiplication");
		job1.setJarByClass(TwoMapreduce.class);
		job1.setReducerClass(MatrixReducer2_1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		MultipleInputs.addInputPath(job1, new Path(inputR), TextInputFormat.class, MatrixMapperVector2_1.class);

		MultipleInputs.addInputPath(job1, new Path(input), TextInputFormat.class, MatrixMapperMatrix2_1.class);

		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path("outputIntermediate")))
			fs.delete(new Path("outputIntermediate"), true);

		TextOutputFormat.setOutputPath(job1, new Path("outputIntermediate"));
		job1.waitForCompletion(true);

		conf = new Configuration();

		job1 = Job.getInstance(conf, "MatrixMultiplication-part2");
		job1.setJarByClass(TwoMapreduce.class);
		job1.setMapperClass(MatrixMapper2_2.class);
		job1.setCombinerClass(MatrixReducer2_2.class);
		job1.setReducerClass(MatrixReducer2_2.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path("outputIntermediate"));
		fs = FileSystem.get(conf);
		if (fs.exists(new Path(outputFolder)))
			fs.delete(new Path(outputFolder), true);
		FileOutputFormat.setOutputPath(job1, new Path(outputFolder));
		job1.waitForCompletion(true);

	}
	
	public static void main(String args[]) throws ClassNotFoundException, IOException, InterruptedException {
		matrixMultiplicationUsingTwoMapReduce("graph","graph","output");
	}

}