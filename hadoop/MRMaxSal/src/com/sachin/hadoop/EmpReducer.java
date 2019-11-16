package com.sachin.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class EmpReducer extends Reducer<IntWritable,DoubleWritable, IntWritable,DoubleWritable> {
      public void reduce(IntWritable key, Iterable<DoubleWritable> values, Reducer<IntWritable,DoubleWritable, IntWritable,DoubleWritable>.Context ctx) throws IOException, InterruptedException {
    	  double max = Double.MIN_VALUE;
          for (DoubleWritable sal : values) {
              if(sal.get() > max)
                  max = sal.get();
          }
          DoubleWritable max_sal = new DoubleWritable(max);
          ctx.write(key, max_sal);
      }
  }