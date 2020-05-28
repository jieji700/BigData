package com.jiang.hbaseMr2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class demo2Driver extends Configuration implements Tool {
    private Configuration conf = null;

    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf());

        job.setJarByClass(demo2Driver.class);
        job.setMapperClass(demo2Mapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Put.class);

        TableMapReduceUtil.initTableReducerJob("master_bak2", demo2Reducer.class, job);
        FileInputFormat.setInputPaths(job, args[0]);

        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public Configuration getConf() {
        return this.conf;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        int i = ToolRunner.run(conf, new demo2Driver(), args);
        System.exit(i);
    }
}
