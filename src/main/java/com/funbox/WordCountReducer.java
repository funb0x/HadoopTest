package com.funbox;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable result = new IntWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.out.println("WordCountReducer setup");
        super.setup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<IntWritable> iterator = values.iterator();
        int sum = 0;

        while(iterator.hasNext()) {
            sum += iterator.next().get();
        }

        result.set(sum);
        context.write(key, result);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("WordCountReducer cleanup");
        super.cleanup(context);
    }
}
