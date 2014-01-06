/*
	Finds the largest integer from a file of integers.

	Runs on MRv1 since it is the new API


	-----
	to compile: 
	[pjlinn@peterRoomDesktop maxInt]$ javac -cp ~/Documents/dataMiningPrograms\
	/hadoop-1.2.1/hadoop-core-1.2.1.jar -d maxInt/ MaxInt.java

	to jar:
	jar -cvf MaxInt.jar -C maxInt/ .

	to run:
	bin/hadoop jar ~/Documents/gitHub/mapReduceJobs/maxInt/MaxInt.jar \
	MaxInt /input/nums.txt /output/maxInt

	--------
	It actually returns each unique digit, not the max, so it doesn't work
	- It was because I was using the (key, value) pair, this is what I originally
	had:
		=> context.write(num, one); 
	changed it to:
		=> context.write(one, num);
	- the first way would create a map task for every individual number, where I 
	really just want 1 map task for all the numbers.
	- Had to change a lot of stuff with that though like the key ins and key outs

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
	extends Mapper<LongWritable, Text, IntWritable, LongWritable> {

		private final static IntWritable one = new IntWritable(1);
		private LongWritable num = new LongWritable();

		@Override
		public void map(LongWritable key, Text value, Context context) 
		throws IOException, InterruptedException {

			String line = value.toString();

			StringTokenizer tokenizer = new StringTokenizer(line);

			num.set(Long.parseLong(tokenizer.nextToken()));

			context.write(one, num);
		}
	}

	public static class Reduce
	extends Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {

		LongWritable max = new LongWritable(0);

		@Override
		public void reduce(IntWritable key, Iterable<LongWritable> values, Context context)
		throws IOException, InterruptedException {

			for (LongWritable value : values ) {
				// max = (value.get() > max.get()) ? value : max; -> Doesn't work
				if (value.get() > max.get()) {
					max.set(value.get());
				}
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
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(LongWritable.class);

		System.exit(job.waitForCompletion(true) ? 0  : 1);
	}
}