package com.twitter.heron.spi.common;

public class Constants {

  // name of the resource file that holds the config keys
  public static final String KEYS_YAML = "heron/config/src/yaml/keys.yaml";

  // name of the resource file that holds the default values for config keys
  public static final String DEFAULTS_YAML = "heron/config/src/yaml/defaults.yaml";

  public static final long GB = 1024L * 1024 * 1024;
  public static final long MB = 1024L * 1024;

  public static final String TUNNEL = "tunnel";
  public static final String ZKHOST = "zkhost";
  public static final String ZKPORT = "zkport";
  public static final String ZKROOT = "zkroot";

  public static final String ZK_CONNECTION_STRING = "zk.connection.string";
  public static final String ZK_CONNECTION_TIMEOUT_MS = "zk.connection.timeout.ms";

  public static final String LOCALFS_STATE_MANAGER_CLASS = 
      "com.twitter.heron.statemgr.localfs.LocalFileSystemStateManager";

  public static final String ZK_STATE_MANAGER_CLASS = 
      "com.twitter.heron.statemgr.zookeeper.curator.CuratorStateManager";

}
