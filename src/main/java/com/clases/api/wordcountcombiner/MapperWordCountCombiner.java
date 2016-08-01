package com.clases.api.wordcountcombiner;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperWordCountCombiner extends
    Mapper<LongWritable, Text, Text, IntWritable> {

  private static IntWritable IntWritable_1 = new IntWritable(1);
  private Text WordAsText = new Text();
  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
   * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
   */
  @Override
  protected void map(LongWritable key, Text textLine, Context context)
      throws IOException, InterruptedException {
    // TODO Auto-generated method stub
    for (String word : textLine.toString().split("\\b+")) {
      word = word.toLowerCase();
      if (word.matches("[a-záéíóúñç]+")) {
        WordAsText.set(word);
        context.write(WordAsText, IntWritable_1);
      }
    }

  }

}
