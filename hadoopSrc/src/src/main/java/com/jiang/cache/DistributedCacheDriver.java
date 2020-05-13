package com.jiang.cache;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class DistributedCacheDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        //get job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //set jar location
        job.setJarByClass(DistributedCacheDriver.class);
        //link to map&reduce
        job.setMapperClass(DistributedCacheMapper.class);

        //set final data k,v
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //set input&output path
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.addCacheFile(new URI("file:///F:/hadoop/hadoop/hadoopSrc/test/input/JOIN/products.txt"));
        job.setNumReduceTasks(0);

        //commit
//        job.submit();
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
