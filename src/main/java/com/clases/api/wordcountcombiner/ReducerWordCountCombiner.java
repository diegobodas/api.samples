package com.clases.api.wordcountcombiner;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerWordCountCombiner extends
    Reducer<Text, IntWritable, Text, LongWritable> {

  private LongWritable countAllWords = new LongWritable();

  @Override
  protected void reduce(Text WordKey, Iterable<IntWritable> theCount,
      Context context)
      throws IOException, InterruptedException {
    // Now, grouped values
    long count = 0;
    for (IntWritable partialCount : theCount) {
      count += (long) partialCount.get();
    }
    countAllWords.set(count);
    context.write(WordKey, countAllWords);
  }

}
