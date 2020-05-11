package com.jiang.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordcountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    int count = 0;
    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        count = 0;
        //sum
        for (IntWritable value : values) {
            count += value.get();
        }
        //write
        v.set(count);
        context.write(key, v);
    }
}
