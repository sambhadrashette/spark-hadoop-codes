package com.sachin.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EmpMapper extends Mapper<LongWritable,Text, IntWritable,DoubleWritable> {
      @Override
      public void map(LongWritable key, Text value, Mapper<LongWritable,Text, IntWritable,DoubleWritable>.Context ctx) throws IOException, InterruptedException {
          String line = value.toString();
          String[] parts = line.split(",");
          IntWritable deptno = new IntWritable(Integer.parseInt(parts[7]));
          DoubleWritable sal = new DoubleWritable( Double.parseDouble(parts[5]) );
          ctx.write(deptno, sal);
      }
  }