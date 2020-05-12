package com.jiang.flowcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text,Text,FlowBean> {

    Text k = new Text();
    FlowBean v = new FlowBean();

    @Override
    protected void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException {
        //get line
        String line = value.toString();
        //split line
        String[] fields = line.split(" ");
        //set
        k.set(fields[1]);
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long dowFlow = Long.parseLong(fields[fields.length - 2]);
        v.set(upFlow, dowFlow);
        //write
        context.write(k,v);
    }

}
