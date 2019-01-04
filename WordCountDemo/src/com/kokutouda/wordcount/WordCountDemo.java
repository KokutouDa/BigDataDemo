package com.kokutouda.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Random;
import java.util.StringTokenizer;


public class WordCountDemo {

    public static class StringTokenizerMapper extends Mapper<Object, Text, Text, LongWritable> {
        private final LongWritable one = new LongWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //break down sentences
            StringTokenizer str = new StringTokenizer(value.toString());
            while (str.hasMoreTokens()) {
                word.set(str.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class CountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable result = new LongWritable();

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            //计算集合，相同的加上云，写完后写入
            long num = 0;

            for (LongWritable val : values) {
                num += val.get();
            }
            result.set(num);
            context.write(key, result);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path inDir = new Path(args[0]);
        Path outDir = new Path(args[1]);

        Job wordCountJob = Job.getInstance(conf);
        wordCountJob.setJobName("word count");
        wordCountJob.setJarByClass(WordCountDemo.class);
        FileInputFormat.setInputPaths(wordCountJob, inDir);
        FileOutputFormat.setOutputPath(wordCountJob, outDir);
        wordCountJob.setOutputKeyClass(Text.class);
        wordCountJob.setOutputValueClass(LongWritable.class);
        wordCountJob.setMapperClass(StringTokenizerMapper.class);
        wordCountJob.setCombinerClass(CountReducer.class);
        wordCountJob.setReducerClass(CountReducer.class);
         wordCountJob.waitForCompletion(true);

    }

}
