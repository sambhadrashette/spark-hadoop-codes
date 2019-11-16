package com.sachin.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class JobwiseAvgSalDriver {
	public static void main(String[] args) throws Exception {
		// create MR job
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "JobwiseAvgSal");
		// set jar in which mapper & reducer class is present
		job.setJarByClass(JobwiseAvgSalDriver.class);
		// set mapper class and mapper output key-value type
		job.setMapperClass(EmpMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		// set reducer class and reducer output key-value type
		job.setReducerClass(EmpReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		// set input & output formats
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		// set input & output hdfs directory for data
		FileInputFormat.setInputPaths(job, new Path("/user/spark/emp/input"));
		FileOutputFormat.setOutputPath(job, new Path("/user/spark/emp/output"));
		// submit job & wait for completion
		boolean success = job.waitForCompletion(true);
		// exit application
		int ret = success ? 0 : 1;
		System.exit(ret);
	}
}