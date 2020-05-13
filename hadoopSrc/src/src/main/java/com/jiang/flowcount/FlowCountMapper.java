package com.jiang.flowcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text,FlowBeanSort,Text> {

    Text v = new Text();
    FlowBeanSort k = new FlowBeanSort();

    @Override
    protected void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException {
        //get line
        String line = value.toString();
        //split line
        String[] fields = line.split(" ");
        //set
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long dowFlow = Long.parseLong(fields[fields.length - 2]);
        k.set(upFlow, dowFlow);
        v.set(fields[1]);
        //write
        context.write(k,v);
    }

}
