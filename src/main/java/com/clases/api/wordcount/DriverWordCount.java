package com.clases.api.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DriverWordCount extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    // Get Configuration (thanks to Configured)
    Configuration conf = getConf();
    // Job configuration
    Job job = Job.getInstance(conf);
    // Our class
    job.setJarByClass(DriverWordCount.class);
    // The Mapper
    job.setMapperClass(MapperWordCount.class);

    // The Reducer
    job.setReducerClass(ReducerWordCount.class);
    // Sin efecto con un hilo local
    // job.setNumReduceTasks(2);
    // Generación especulativa
    job.setMapSpeculativeExecution(false);

    // The input file
    job.setInputFormatClass(TextInputFormat.class);
    // We suppose that the input path is introduced in command line (first
    // place)
    FileInputFormat.addInputPath(job, new Path(args[0]));
    // The output --- Check the Mapper
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    // Output File
    job.setOutputFormatClass(TextOutputFormat.class);
    // We suppose that the input path is introduced in command line (2nd place)
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration(true);
    ToolRunner.run(conf, new DriverWordCount(), args);

  }

}
