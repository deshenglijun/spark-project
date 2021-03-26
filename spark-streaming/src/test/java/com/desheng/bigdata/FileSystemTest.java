package com.desheng.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class FileSystemTest {
    public static void main(String[] args) throws Exception {
        FileSystem fs = FileSystem.get(new URI("hdfs://ns1/"), new Configuration());

        FSDataOutputStream fos = fs.create(new Path("/data/spark/monitor/kannixingbbux.txt"));
        fos.writeUTF("hei hei");
        fs.close();
    }
}
