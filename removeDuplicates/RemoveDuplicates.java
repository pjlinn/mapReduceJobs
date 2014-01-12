/*
	MapReduce job that will return the same set of integers
	passed, but with duplicates removed.

	To Compile:
		javac -cp ~/Documents/dataMiningPrograms\
		/hadoop-1.2.1/hadoop-core-1.2.1.jar -d removeDuplicates/ RemoveDuplicates.java	

	To Jar:	
		jar -cvf removeDuplicates.jar -C removeDuplicates/ .

	To Run:
		bin/hadoop jar ~/Documents/gitHub/mapReduceJobs/removeDuplicates\
		/removeDuplicates.jar RemoveDuplicates /input/nums.txt /output
*/

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import java.util.StringTokenizer;

public class RemoveDuplicates {
	static public class Map 
	extends Mapper<LongWritable,Text,LongWritable,LongWritable> {

		private final LongWritable one = new LongWritable(1);
		private LongWritable num = new LongWritable();

		@Override
		public void map(LongWritable key, Text value, Context context) 
		throws IOException, InterruptedException {

			String line = value.toString();

			StringTokenizer tokenizer = new StringTokenizer(line);

			num.set(Long.parseLong(tokenizer.nextToken()));

			context.write(num, one);
		}
	}

	static public class Reduce
	extends Reducer<LongWritable, LongWritable, LongWritable, Text> {

		@Override
		public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
		throws IOException, InterruptedException {

			LongWritable result = new LongWritable();

			Text empty = new Text("");

			for (LongWritable value : values) {
				result.set(key.get());
			}
			context.write(result, empty);
		}
	}

	public static void main(String[] args) throws Exception {
		
		Job job = new Job();
		job.setJarByClass(RemoveDuplicates.class);
		job.setJobName("RemoveDuplicates");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(Map.class);
		// job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}