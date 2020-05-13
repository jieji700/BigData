package com.jiang.reducerjoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {
    String name;
    TableBean tb = new TableBean();
    Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        name = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(" ");
        if (name.startsWith("order")) {
            tb.setOrder_id(fields[0]);
            tb.setP_id(fields[1]);
            tb.setAmount(Integer.parseInt(fields[2]));
            tb.setPname("");
            tb.setFlag("order");
            k.set(fields[1]);
        } else {
            tb.setOrder_id("");
            tb.setP_id(fields[0]);
            tb.setAmount(0);
            tb.setPname(fields[1]);
            tb.setFlag("pd");
            k.set(fields[0]);
        }
        context.write(k, tb);
    }
}
