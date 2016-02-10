package com.twitter.heron.spi.common;

public class Constants {
  public static final long GB = 1024L * 1024 * 1024;
  public static final long MB = 1024L * 1024;

  public static final String TUNNEL = "tunnel";
  public static final String ZKHOST = "zkhost";
  public static final String ZKPORT = "zkport";
  public static final String ZKROOT = "zkroot";
  public static final String ZK_CONNECTION_STRING = "zk.connection.string";
  public static final String ZK_CONNECTION_TIMEOUT_MS = "zk.connection.timeout.ms";

  public static final String HERON_CORE_RELEASE_URI = "heron.core.release.uri";
  public static final String TOPOLOGY_PKG_URI = "heron.topology.pkg.uri";

  public static final String HERON_RELEASE_PACKAGE = "heron.release.package";
  public static final String HERON_RELEASE_PACKAGE_ROLE = "heron.release.package.role";
  public static final String HERON_RELEASE_PACKAGE_NAME = "heron.release.package.name";
  public static final String HERON_RELEASE_PACKAGE_VERSION = "heron.release.package.version";
  public static final String HERON_UPLOADER_VERSION = "heron.uploader.version";

  public static final String HERON_UPLOADER_FILE_SYSTEM_PATH = "heron.uploader.file.system.path";

  public static final String DC = "dc";
  public static final String ROLE = "role";
  public static final String ENVIRON = "environ";
  public static final String UPLOADER_CLASS = "uploader.class";
  public static final String LAUNCHER_CLASS = "launcher.class";
  public static final String SCHEDULER_CLASS = "scheduler.class";
  public static final String RUNTIME_MANAGER_CLASS = "runtime.manager.class";
  public static final String PACKING_ALGORITHM_CLASS = "packing.algorithm.class";
  public static final String STATE_MANAGER_CLASS = "state.manager.class";

  public static final String TOPOLOGY_DEFINITION_FILE = "topology.definition.file";

  public static final String HERON_DIR = "heron.dir";
  public static final String HERON_CONFIG_PATH = "heron.config.path";
  public static final String HERON_CONFIG_LOADER = "heron.config.loader";
  public static final String HERON_AURORA_BIND_PREFIX = "heron.aurora.bind.";
  public static final String HERON_VERBOSE = "heron.verbose";
  public static final String CONFIG_PROPERTY = "config.property";
}
