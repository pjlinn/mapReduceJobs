/*
	Wordcount example from the Hadoop MapReduce tutorial:
	http://hadoop.apache.org/docs/r1.2.1/mapred_tutorial.html

	Usage:

	$ mkdir wordcount_classes 
	$ javac -classpath ${HADOOP_HOME}/hadoop-${HADOOP_VERSION}-core.jar 
		-d wordcount_classes WordCount.java 
	$ jar -cvf /usr/joe/wordcount.jar -C wordcount_classes/ .

	Assuming that:

	/usr/joe/wordcount/input - input directory in HDFS
	/usr/joe/wordcount/output - output directory in HDFS
	Sample text-files as input:

	$ bin/hadoop dfs -ls /usr/joe/wordcount/input/ 
	/usr/joe/wordcount/input/file01 
	/usr/joe/wordcount/input/file02 

	$ bin/hadoop dfs -cat /usr/joe/wordcount/input/file01 
	Hello World Bye World 

	$ bin/hadoop dfs -cat /usr/joe/wordcount/input/file02 
	Hello Hadoop Goodbye Hadoop

	Run the application:

	$ bin/hadoop jar /usr/joe/wordcount.jar org.myorg.WordCount 
		/usr/joe/wordcount/input /usr/joe/wordcount/output

	Output:

	$ bin/hadoop dfs -cat /usr/joe/wordcount/output/part-00000 
	Bye 1 
	Goodbye 1 
	Hadoop 2 
	Hello 2 
	World 2 	



	Hadoop 2.2
	[pjlinn@peterRoomDesktop wordcountv1.0]$ javac -cp \
	"/home/pjlinn/Documents/dataMiningPrograms/hadoop-2.2.0/share/hadoop/map\
	reduce/*:/home/pjlinn/Documents/dataMiningPrograms/hadoop-2.2.0/share/had\
	oop/common/*" WordCount.java

	[pjlinn@peterRoomDesktop hadoop-2.2.0]$ ./bin/hadoop jar ~/Documents/git\
	Hub/mapReduceJobs/wordcountv1.0/wordcount.jar WordCount /testing/input/ \
	/testing/output
*/

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class WordCount {

	public static class Map extends MapReduceBase implements 
	Mapper<LongWritable, Text, Text, IntWritable> {
		
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text,
			IntWritable> output, Reporter reporter) throws IOException {

			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

			while (tokenizer.hasMoreTokens()){
				word.set(tokenizer.nextToken());
				output.collect(word, one);
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
	Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {

				int sum = 0;

				while(values.hasNext()) {
					sum += values.next().get();
				}
				output.collect(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws IOException{
		
		JobConf conf = new JobConf(WordCount.class);
		conf.setJobName("wordcount");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}