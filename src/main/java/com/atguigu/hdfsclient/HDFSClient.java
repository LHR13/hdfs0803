package com.atguigu.hdfsclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class HDFSClient {
    private FileSystem fileSystem;

    @Before
    public void create() throws IOException, InterruptedException {
        fileSystem = FileSystem.get(URI.create("hdfs://localhost:9000"), new Configuration());
    }

    @Test
    public void append() throws IOException {
        FSDataOutputStream append = fileSystem.append(new Path("/mytest1/1.txt"));
        FileInputStream inputStream = new FileInputStream("/usr/local/hadoop/style.txt");
        IOUtils.copyBytes(inputStream, append, 1024);

    }

    @Test
    public void listFiles() throws IOException {
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(new Path("/"), true);

        while (locatedFileStatusRemoteIterator.hasNext()) {
            LocatedFileStatus status = locatedFileStatusRemoteIterator.next();

            System.out.println("======================");
            System.out.println(status.getPath());

            System.out.println("块信息：");
            BlockLocation[] blockLocations = status.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                System.out.print("块在：");
                for (String host : hosts) {
                    System.out.print(host + " ");
                    System.out.println();
                }

            }
        }
    }

    @After
    public void close() throws IOException {
        fileSystem.close();
    }
}
