package com.jiang.reducerjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TableDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //get job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //set jar location
        job.setJarByClass(TableDriver.class);
        //link to map&reduce
        job.setMapperClass(TableMapper.class);
        job.setReducerClass(TableReducer.class);

        //set mapper output datatype
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);

        //set final data k,v
        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

        //set input&output path
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //commit
//        job.submit();
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
