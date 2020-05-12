package com.jiang.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//create mapper
public class WordcountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //get line data
        String line = value.toString();

        //split data
        String[] words = line.split(" ");

        //data
        for (String word : words) {
            k.set(word);
            context.write(k, v);
        }
    }
}
