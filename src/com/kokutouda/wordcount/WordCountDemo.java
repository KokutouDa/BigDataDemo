package com.kokutouda.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;


public class WordCountDemo {

    public static class StringTokenizerMapper extends Mapper<Object, Text, Text, LongWritable> {
        private final LongWritable one = new LongWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //把句子分解成单词
            StringTokenizer str = new StringTokenizer(value.toString());
            //再遍历单词，以 key,value 的形式存储
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
            long num = 0;

            //遍历读取Map的输出键值，计算词频
            for (LongWritable val : values) {
                num += val.get();
            }
            result.set(num);
            context.write(key, result);
        }
    }


    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("usage: WordCount <inDir> <outDir>");
        } else {
            Configuration conf = new Configuration();
            Path inDir = new Path(args[0]);
            Path outDir = new Path(args[1]);
            //创建任务
            Job wordCountJob = Job.getInstance(conf);
            wordCountJob.setJobName("word count");
            wordCountJob.setJarByClass(WordCountDemo.class);
            //设置文件输入和输出的路径
            FileInputFormat.setInputPaths(wordCountJob, inDir);
            FileOutputFormat.setOutputPath(wordCountJob, outDir);
            //设置输出的键值的类型
            wordCountJob.setOutputKeyClass(Text.class);
            wordCountJob.setOutputValueClass(LongWritable.class);

            wordCountJob.setMapperClass(StringTokenizerMapper.class);
            wordCountJob.setCombinerClass(CountReducer.class);
            wordCountJob.setReducerClass(CountReducer.class);
            //执行任务
            wordCountJob.waitForCompletion(true);
        }
    }
}
