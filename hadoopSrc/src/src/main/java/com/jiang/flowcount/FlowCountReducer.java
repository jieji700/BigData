package com.jiang.flowcount;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<FlowBeanSort,Text, Text,FlowBeanSort> {

    FlowBeanSort v = new FlowBeanSort();

    @Override
    protected void reduce(FlowBeanSort key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text value:values){
            context.write(value,key);
        }
    }
}
