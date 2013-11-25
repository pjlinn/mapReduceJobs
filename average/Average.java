/*
	Finds the average of a set of numbers
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

public class Average {
	// Class Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
	static public class Map 
	extends Mapper<LongWritable,Text,Text,IntWritable> {

	private final static Text singleKey = new Text("Average");
	private IntWritable num = new IntWritable();

		@Override
		public void map(LongWritable key, Text value, Context context)	
		throws IOException, InterruptedException {

			String line = value.toString();

			StringTokenizer tokenizer = new StringTokenizer(line);

			num.set(Integer.parseInt(tokenizer.nextToken()));

			context.write(singleKey, num);

		}

	}

	static public class Reduce 
	extends Reducer<Text,IntWritable,Text,IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException {

			int sum = 0;
			int counter = 0;

			for (IntWritable value : values) {
				sum += value.get();
				counter++;
			}

			sum = sum / counter; // Rounds off decimal - fix
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		
		Job job = new Job();
		job.setJarByClass(Average.class);
		job.setJobName("Average");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}