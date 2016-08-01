package com.clases.api.samples;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerIdentity extends
    Reducer<Text, NullWritable, Text, NullWritable> {

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
   * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
   */
  @Override
  protected void reduce(Text key, Iterable<NullWritable> values, Context context)
      throws IOException, InterruptedException {

    for (NullWritable null_writable : values) {
      context.write(key, null_writable);
    }
  }

}
