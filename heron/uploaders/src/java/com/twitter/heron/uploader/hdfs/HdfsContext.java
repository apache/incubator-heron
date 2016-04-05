package com.twitter.heron.uploader.hdfs;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;

public class HdfsContext extends Context {
  public static final String HADOOP_CONFIG_DIRECTORY = "heron.uploader.hdfs.config.directory";
  public static final String HDFS_TOPOLOGIES_DIRECTORY_URI = "heron.uploader.hdfs.topologies.directory.uri";

  public static String hadoopConfigDirectory(Config config) {
    return config.getStringValue(HADOOP_CONFIG_DIRECTORY);
  }

  public static String hdfsTopologiesDirectoryURI(Config config) {
    return config.getStringValue(HDFS_TOPOLOGIES_DIRECTORY_URI);
  }
}
