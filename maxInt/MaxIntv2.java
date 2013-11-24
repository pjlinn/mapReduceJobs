/*
	The first version of this program didn't really work. It returned 
	every unique int, since I'm getting the max from each group of ints
	which isn't really doing anything.

	I think I need to adjust the map phase, so I created this version
	to play around with.

	Finds the largest integer from a file of integers.

	Runs on MRv1


	-----
	to compile: 
	[pjlinn@peterRoomDesktop maxInt]$ javac -cp ~/Documents/dataMiningPrograms\
	/hadoop-1.2.1/hadoop-core-1.2.1.jar -d maxIntv2/ MaxIntv2.java

	to jar:
	jar -cvf MaxIntv2.jar -C maxIntv2/ .

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

// import org.apache.hadoop.mapred.*;

public class MaxIntv2 {
	// Class Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
	public static class Map 
	extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private IntWritable num = new IntWritable();

		@Override
		public void map(LongWritable key, Text value, Context context) 
		throws IOException, InterruptedException {

			String line = value.toString();

			StringTokenizer tokenizer = new StringTokenizer(line);

			num.set(Integer.parseInt(tokenizer.nextToken()));

			context.write(one, num);
		}
	}

	public static class Reduce
	extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		@Override
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException {

			IntWritable max = new IntWritable(0);

			for (IntWritable value : values ) {
				max = (value.get() > max.get()) ? value : max;
			}
			context.write(key, max);

		}
	}

	public static void main(String[] args) throws Exception {
		
		Job job = new Job();
		job.setJarByClass(MaxIntv2.class);
		job.setJobName("Max Int v2");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true) ? 0  : 1);
	}
}