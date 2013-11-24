/*
	Finds the largest integer from a file of integers.

	Runs on MRv1


	-----
	to compile: 
	[pjlinn@peterRoomDesktop maxInt]$ javac -cp ~/Documents/dataMiningPrograms\
	/hadoop-1.2.1/hadoop-core-1.2.1.jar -d maxInt/ MaxInt.java

	to jar:
	jar -cvf MaxInt.jar -C maxInt/ .

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

public class MaxInt {
	// Class Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
	public static class Map 
	extends Mapper<LongWritable, Text, Text, IntWritable> {

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

			IntWritable max = new IntWritable(0);

			for (IntWritable value : values ) {
				max = (value.get() > max.get()) ? value : max;
			}
			context.write(key, max);

		}
	}

	public static void main(String[] args) throws Exception {
		
		Job job = new Job();
		job.setJarByClass(MaxInt.class);
		job.setJobName("Max Int");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true) ? 0  : 1);
	}
}