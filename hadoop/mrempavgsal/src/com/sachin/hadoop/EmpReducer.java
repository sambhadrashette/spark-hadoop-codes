package com.sachin.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EmpReducer extends Reducer<Text,DoubleWritable, Text,DoubleWritable> {
      public void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text,DoubleWritable, Text,DoubleWritable>.Context ctx) throws IOException, InterruptedException {
          int cnt = 0;
          double sum = 0.0;
          for(DoubleWritable sal: values) {
              sum = sum + sal.get();
              cnt++;
          }
          DoubleWritable avg_sal = new DoubleWritable(sum / cnt);
          ctx.write(key, avg_sal);
      }
  }