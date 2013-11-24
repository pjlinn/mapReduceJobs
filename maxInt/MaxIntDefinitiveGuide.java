/*
	Max int MapReduce from the Hadoop: The Definitive Guide example

	Uses new API, runs on MRv1

	To run:

	To compile:

*/
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

public class MaxIntDefinitiveGuide {

	public static class Map 
	extends Mapper<LongWritable, Text, Text, IntWritable> {

		private static final String singleKey = "a";

		@Override
		public void map(LongWritable key, Text value, Context context) 
		throws IOException, InterruptedException {

			String line = value.toString();
			String num = line.substring(0);

			int max;

			max = Integer.parseInt(num);

			context.write(new Text(singleKey), new IntWritable(max));

		}
	}

	public static class Reduce 
	extends Reducer<Text,IntWritable,Text,IntWritable> {


		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
		throws IOException, InterruptedException {

			int maxValue = Integer.MIN_VALUE;

			for (IntWritable value : values) {
				maxValue = Math.max(maxValue, value.get());
			}
			context.write(key, new IntWritable(maxValue));
		}
	}

	public static void main(String[] args) 
	throws Exception {

		Job job = new Job();
		job.setJarByClass(MaxIntDefinitiveGuide.class);
		job.setJobName("Max: Definitive Guide Ex.");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}