/*
	Finds the average of a set of numbers

	-----------------

	To compile:
		javac -cp ~/Documents/dataMiningPrograms\
		/hadoop-1.2.1/hadoop-core-1.2.1.jar -d average/ Average.java

	To jar:
		jar -cvf average.jar -C average/ .

	To Run:
		bin/hadoop jar ~/Documents/gitHub/mapReduceJobs/average/average.jar \
		Average /input/nums.txt /output/
*/


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.util.StringTokenizer;

public class Average {
	// Class Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
	static public class Map 
	extends Mapper<LongWritable,Text,DoubleWritable,DoubleWritable> {

	// private final static Text singleKey = new Text("Average");
	private final DoubleWritable one = new DoubleWritable(1);
	private DoubleWritable num = new DoubleWritable();

		@Override
		public void map(LongWritable key, Text value, Context context)	
		throws IOException, InterruptedException {

			String line = value.toString();

			StringTokenizer tokenizer = new StringTokenizer(line);

			num.set(Double.parseDouble(tokenizer.nextToken()));

			context.write(one, num);

		}

	}

	static public class Reduce 
	extends Reducer<DoubleWritable,DoubleWritable,DoubleWritable,DoubleWritable> {

		@Override
		public void reduce(DoubleWritable key, Iterable<DoubleWritable> values, Context context)
		throws IOException, InterruptedException {

			double sum = 0;
			double counter = 0;
			double avg;

			// Text avg = new Text("Average");
			DoubleWritable iSum = new DoubleWritable();
			DoubleWritable dKey = new DoubleWritable(key.get());

			for (DoubleWritable value : values) {
				sum += value.get();
				counter += dKey.get();
			}

			avg = sum / counter; // Rounds off decimal - fix
			iSum.set(counter);
			context.write(iSum, new DoubleWritable(avg));
		}
	}

	public static void main(String[] args) throws Exception {
		
		Job job = new Job();
		job.setJarByClass(Average.class);
		job.setJobName("Average");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(Map.class);
		// job.setCombinerClass(Reduce.class); Doesn't work
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(DoubleWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}