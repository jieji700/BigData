package com.jiang.outputFormatCus;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class FilerRecordWriter extends RecordWriter<Text, NullWritable> {

    FSDataOutputStream fsTargetStream;
    FSDataOutputStream fsOtherStream;

    public FilerRecordWriter(TaskAttemptContext job) {
        try {
            FileSystem fileSystem = FileSystem.get(job.getConfiguration());
            fsTargetStream = fileSystem.create(new Path("F:/target.log"));
            fsOtherStream = fileSystem.create(new Path("F:/other.log"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        if (text.toString().contains("baidu")) {
            fsTargetStream.write(text.toString().getBytes());
        } else{
            fsOtherStream.write(text.toString().getBytes());
        }
    }

    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(fsTargetStream);
        IOUtils.closeStream(fsOtherStream);
    }
}
