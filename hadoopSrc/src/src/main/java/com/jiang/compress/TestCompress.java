package com.jiang.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

public class TestCompress {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        //Compress
        compress("f:/hello.txt", "org.apache.hadoop.io.compress.BZip2Codec");
        decompress("f:/hello.txt.bz2");
    }

    private static void compress(String fileName, String type) throws ClassNotFoundException, IOException {

        FileInputStream fis = new FileInputStream(new File(fileName));

        Class codecClass = Class.forName(type);
        CompressionCodec codec = (CompressionCodec)
                ReflectionUtils.newInstance(codecClass, new Configuration());

        FileOutputStream fos = new FileOutputStream(new File(fileName + codec.getDefaultExtension()));
        CompressionOutputStream cos = codec.createOutputStream(fos);

        IOUtils.copyBytes(fis, cos, 1024 * 1024 * 5, false);

        IOUtils.closeStream(cos);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
    }

    private static void decompress(String filename) throws IOException {
        CompressionCodecFactory factory = new
                CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path(filename));
        if (codec == null) {
            System.out.println("cannot find codec for file" +
                    filename);
            return;
        }
        CompressionInputStream cis = codec.createInputStream(new
                FileInputStream(new File(filename)));

        FileOutputStream fos = new FileOutputStream(new File(filename
                + ".decoded"));

        IOUtils.copyBytes(cis, fos, 1024 * 1024 * 5, false);

        IOUtils.closeStream(cis);
        IOUtils.closeStream(fos);
    }
}
