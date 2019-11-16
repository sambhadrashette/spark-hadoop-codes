package com.sachin.hadoop;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class YearlyAvgTempDriver extends Configured implements Tool {
	public static void main(String[] args) {
		try {
			GenericOptionsParser parser = new GenericOptionsParser(args);
			Configuration conf = parser.getConfiguration();
			YearlyAvgTempDriver driver = new YearlyAvgTempDriver();
			int ret = ToolRunner.run(conf, driver, args);
			System.exit(ret);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 2) {
			System.out.println("Invalid arguments. Expected 2 args: Input Path, Output Path");
			return 1;
		}
		// create MR job
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "YearlyAvgTemp");
		// set jar in which mapper & reducer class is present
		job.setJarByClass(YearlyAvgTempDriver.class);
		// set mapper class and mapper output key-value type
		job.setMapperClass(TempMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		// set reducer class and reducer output key-value type
		job.setReducerClass(TempReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		// set input & output formats
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		// set input & output hdfs directory for data
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// submit job & wait for completion
		boolean success = job.waitForCompletion(true);
		// exit application
		int ret = success ? 0 : 1;
		return ret;
	}
}
