package com.jiang.cache;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class DistributedCacheMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    HashMap<String,String> hashmap = new HashMap<String,String>();
    Text k = new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath().toString();

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));
        String line;
        while (StringUtils.isNotEmpty(line =bufferedReader.readLine())){
            String[] fields = line.split(" ");
            hashmap.put(fields[0],fields[1]);
        }
        IOUtils.closeStream(bufferedReader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fileds = line.split(" ");
        String pid = fileds[1];
        String pname = hashmap.get(pid);
        line = line + '\t'+ pname;
        k.set(line);
        context.write(k,NullWritable.get());
    }
}
