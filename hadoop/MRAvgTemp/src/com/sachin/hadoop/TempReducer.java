package com.sachin.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TempReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
      public void reduce(IntWritable key, Iterable<DoubleWritable> values, Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable>.Context ctx) throws IOException, InterruptedException {
          int cnt = 0;
          double sum = 0.0;
          for(DoubleWritable temp: values) {
              sum = sum + temp.get();
              cnt++;
          }
          DoubleWritable avg_temp = new DoubleWritable(sum / cnt);
          ctx.write(key, avg_temp);
      }
  }