package com.jiang.outputFormatCus;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FilterReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    Text k = new Text();

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        String line = key.toString();
        line += "\r\n";
        k.set(line);
        for (NullWritable value : values) {
            context.write(k, NullWritable.get());
        }
    }
}
