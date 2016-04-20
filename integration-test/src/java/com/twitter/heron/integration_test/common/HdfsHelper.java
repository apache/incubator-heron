package com.twitter.heron.integration_test.common;

import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * A helper class abstracting away the interaction with HDFS
 * Note: Adding these helper method directly in the spout code results
 * in a ClassNotFoundException for hadoop classes, while deploying a topology, even though
 * those classes are not being accessed during deploy.
 */
public class HdfsHelper {

  /**
   * Provide an InputStreamReader to read from a hdfs file
   *
   * @param path is the hdfs file location
   */
  public static InputStreamReader getHdfsStreamReader(String path) throws IOException {
    Path pt = new Path(path);
    Configuration conf = new Configuration();
    FileSystem fileSystem = FileSystem.get(conf);
    return new InputStreamReader(fileSystem.open(pt), "UTF-8");
  }
}