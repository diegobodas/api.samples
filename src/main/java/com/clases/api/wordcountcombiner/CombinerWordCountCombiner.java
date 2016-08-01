package com.clases.api.wordcountcombiner;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CombinerWordCountCombiner extends
    Reducer<Text, IntWritable, Text, IntWritable> {

  private IntWritable countAllWords = new IntWritable();

  @Override
  protected void reduce(Text WordKey, Iterable<IntWritable> ListOfOnes,
      Context context)
      throws IOException, InterruptedException {

    int count1s = 0;
    for (@SuppressWarnings("unused")
    IntWritable one : ListOfOnes) {
      count1s++;
    }
    countAllWords.set(count1s);
    context.write(WordKey, countAllWords);
  }

}
