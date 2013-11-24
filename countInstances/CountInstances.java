/*
	MapReduce job to count the number of instances of each unique number
	in a list of numbers. This is basically the same thing as a wordcount
	except it uses the newer version of mapReduce not sure if it's the
	newest or not.

	Currently doesn't work

	Hadoop 2.2
	[pjlinn@peterRoomDesktop wordcountv1.0]$ javac -cp \
	"/home/pjlinn/Documents/dataMiningPrograms/hadoop-2.2.0/share/hadoop/map\
	reduce/*:/home/pjlinn/Documents/dataMiningPrograms/hadoop-2.2.0/share/had\
	oop/common/*" WordCount.java
*/

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapred.*;

public class CountInstances {

	public static class Map 
	extends Mapper<LongWritable,Text, Text,IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text num = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {

			String line = value.toString();

			StringTokenizer tokenizer = new StringTokenizer(line);

			num.set(tokenizer.nextToken());

			context.write(num, one);
		}
	}

	public static class Reduce 
	extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException {

			int sum = 0;

			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) 
	throws Exception {
		
		if (args.length != 2) {
			System.out.println("Usage: CountInstances <input path> <output path>");
			System.exit(-1);
		}

		Job job = new Job();
		job.setJarByClass(CountInstances.class);
		job.setJobName("Count Instances");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}