package MatrixMultiplication;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class OneMapreduce {
	public static class MatrixMapperVector extends Mapper<Object, Text, Text, Text>

	{

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException

		{

			Configuration conf = context.getConfiguration();
			int m = Integer.parseInt(conf.get("m"));
			int p = Integer.parseInt(conf.get("p"));
			String line = value.toString();
			String[] indicesAndValue = line.split("\t");
			Text outputKey = new Text();
			Text outputValue = new Text();
			for (int i = 0; i < m; i++)
			{
				outputKey.set(i + "," + "0");
				outputValue.set("N," + indicesAndValue[0] + "," + indicesAndValue[1]);
				context.write(outputKey, outputValue);
			}
		}
	}

	public static class MatrixMapperMatrix extends Mapper<Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			Configuration conf = context.getConfiguration();
			int m = Integer.parseInt(conf.get("m"));
			int p = Integer.parseInt(conf.get("p"));
			String line = value.toString();
			String[] indicesAndValue = line.split(" ");
			Text outputKey = new Text();
			Text outputValue = new Text();
			for (int i = 1; i < indicesAndValue.length; i++) {
				for (int k = 0; k < p; k++) {
					outputKey.set(indicesAndValue[0] + "," + k);
					outputValue.set("M," + indicesAndValue[i] + "," + "1");
					context.write(outputKey, outputValue);
				}
			}
		}
	}
	
	public static class MatrixReducer extends Reducer<Text, Text, Text, Text>
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
			int n = Integer.parseInt(context.getConfiguration().get("n"));
			float result = 0.0f;
			float a_ij;
			float b_jk;
			for (int j = 0; j < n; j++)
			{
				a_ij = hashA.containsKey(j) ? hashA.get(j) : 0.0f;
				b_jk = hashB.containsKey(j) ? hashB.get(j) : 0.0f;
				result += a_ij * b_jk;
			}
			if (result != 0.0f)
			{
				context.write(new Text(""), new Text(key.toString() + "\t" + Float.toString(result)));
			}
		}
	}

	static void matrixMultiplicationUsingOneMapReduce(String input, String inputR, String outputFolder)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		// M is an m-by-n matrix; N is an n-by-p matrix.
		conf.set("m", "64375");
		conf.set("n", "64375");
		conf.set("p", "1");
		Job job1 = Job.getInstance(conf, "MatrixMultiplication");
		job1.setJarByClass(PartB_3_Using_One_MapReduce.class);
		job1.setReducerClass(MatrixReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		MultipleInputs.addInputPath(job1, new Path("graph/edgelist.txt"), TextInputFormat.class, MatrixMapperVector.class);
		MultipleInputs.addInputPath(job1, new Path("graph/vector.txt"), TextInputFormat.class, MatrixMapperMatrix.class);

		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(outputFolder)))
			fs.delete(new Path(outputFolder), true);

		TextOutputFormat.setOutputPath(job1, new Path(outputFolder));
		job1.waitForCompletion(true);
	}
public static void main(String args[]) throws ClassNotFoundException, IOException, InterruptedException {
	matrixMultiplicationUsingOneMapReduce("graph","inputR","output1");
}
}