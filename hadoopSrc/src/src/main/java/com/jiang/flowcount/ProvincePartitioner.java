package com.jiang.flowcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<FlowBeanSort,Text> {
    public int getPartition(FlowBeanSort flowBean, Text text, int i) {
        //key phoneNo
        //get phoneno
        String prePhoneNo = text.toString().substring(0, 3);
        int partition = 4;
        if ("135".equals(prePhoneNo)) {
            partition = 0;
        } else if ("136".equals(prePhoneNo)) {
            partition = 1;
        } else if ("137".equals(prePhoneNo)) {
            partition = 2;
        } else if ("138".equals(prePhoneNo)) {
            partition = 3;
        }

        return partition;
    }
}
