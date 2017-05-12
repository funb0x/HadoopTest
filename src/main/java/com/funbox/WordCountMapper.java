package com.funbox;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    IntWritable one = new IntWritable(1);
    Text word = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.out.println("WordCountMapper setup");
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while(itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("WordCountMapper cleanup");
        super.cleanup(context);
    }
}
