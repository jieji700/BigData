package com.jiang.order;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OrderDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"F:\\hadoop\\hadoop\\hadoopSrc\\test\\input\\products.txt",
                "F:\\hadoop\\hadoop\\hadoopSrc\\test\\output"};
        //get job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //set jar location
        job.setJarByClass(OrderDriver.class);
        //link to map&reduce
        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);

        //set mapper output datatype
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        //set final data k,v
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);
        job.setGroupingComparatorClass(OrderSortGroupingComparator.class);

        //set input&output path
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //commit
//        job.submit();
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
