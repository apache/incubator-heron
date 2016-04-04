package com.twitter.heron.uploader.hdfs;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;

public class HdfsContext extends Context {
  public static final String HADOOP_CONFIG_DIRECTORY = "heron.uploader.hdfs.config.directory";
  public static final String HDFS_TOPOLOGIES_DIRECTORY = "heron.uploader.hdfs.topologies.directory";

  public static String getHadoopConfigDirectory(Config config) {
    return config.getStringValue(HADOOP_CONFIG_DIRECTORY);
  }

  public static String getHdfsTopologiesDirectory(Config config) {
    return config.getStringValue(HDFS_TOPOLOGIES_DIRECTORY);
  }
}
