package com.sachin.hadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TempMapper extends Mapper<LongWritable,Text, IntWritable, DoubleWritable> {
      @Override
      public void map(LongWritable key, Text value, Mapper<LongWritable,Text, IntWritable,DoubleWritable>.Context ctx) throws IOException, InterruptedException {
          String line = value.toString();
          
          List<String> criteria = Arrays.asList("0", "1", "4", "5", "9");
          String quality = line.substring(92, 93);
          if(criteria.contains(quality)) {
        	  IntWritable year =new IntWritable(Integer.parseInt(line.substring(15, 19)));
        	  DoubleWritable temp = new DoubleWritable(Double.parseDouble(line.substring(87, 92)));
              ctx.write(year, temp);
          }
      }
  }