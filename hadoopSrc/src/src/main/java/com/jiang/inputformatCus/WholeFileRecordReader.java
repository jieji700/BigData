package com.jiang.inputformatCus;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WholeFileRecordReader extends RecordReader<Text, BytesWritable> {
    private FileSplit fileSplit;
    private Configuration conf;
    private Text k = new Text();
    private BytesWritable v = new BytesWritable();
    private boolean isProcess = true;

    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //init
        this.fileSplit = (FileSplit) inputSplit;
        conf = taskAttemptContext.getConfiguration();
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        //main
        if (isProcess) {
            byte[] buf = new byte[(int) (fileSplit.getLength())];

            //get fs
            Path path = this.fileSplit.getPath();
            FileSystem fileSystem = path.getFileSystem(conf);

            //inputStream
            FSDataInputStream fsDataInputStream = fileSystem.open(path);

            //copy
            IOUtils.readFully(fsDataInputStream, buf, 0, buf.length);

            //set value
            v.set(buf, 0, buf.length);

            //set key
            k.set(path.toString());

            IOUtils.closeStream(fsDataInputStream);

            this.isProcess = false;

            return true;
        }
        return false;
    }

    public Text getCurrentKey() throws IOException, InterruptedException {
        return k;
    }

    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return v;
    }

    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    public void close() throws IOException {
    }
}
