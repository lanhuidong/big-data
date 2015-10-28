package com.nexusy.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * @author lanhuidong
 * @since 2015-10-28
 */
public class ListStatus {

    public static void main(String[] args) throws IOException {
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path[] paths = new Path[args.length];
        for (int i = 0; i < args.length; i++) {
            paths[i] = new Path(args[i]);
        }
        FileStatus[] status = fs.listStatus(paths);
        Path[] listedPath = FileUtil.stat2Paths(status);
        for (Path path : listedPath) {
            System.out.println(path);
        }
    }
}
