package com.twitter.heron.scheduler.api;

public class Constants {
  public static final long GB = 1024L * 1024 * 1024;
  public static final long MB = 1024L * 1024;

  public static final String ZK_CONNECTION_TIMEOUT_MS = "zk.connection.timeout.ms";
  public static final String ZK_CONNECTION_STRING = "zk.connection.string";

  public static final String HERON_CORE_RELEASE_URI = "heron.core.release.uri";
  public static final String TOPOLOGY_PKG_URI = "heron.topology.pkg.uri";

  public static final String HERON_RELEASE_USER_NAME = "heron.release.pkgrole";
  public static final String HERON_RELEASE_TAG = "heron.release.pkgname";
  public static final String HERON_RELEASE_VERSION = "heron.release.pkgversion";
  public static final String HERON_UPLOADER_VERSION = "release.packer.version";

  public static final String DC = "dc";
  public static final String ENVIRON = "environ";
  public static final String ROLE = "role";
  public static final String UPLOADER_CLASS = "uploader.class";
  public static final String LAUNCHER_CLASS = "launcher.class";
  public static final String SCHEDULER_CLASS = "scheduler.class";
  public static final String RUNTIME_MANAGER_CLASS = "runtime.manager.class";
  public static final String PACKING_ALGORITHM_CLASS = "packing.algorithm.class";
  public static final String STATE_MANAGER_CLASS = "state.manager.class";
  public static final String HERON_VERBOSE = "heron.verbose";
  public static final String DEFAULT_RELEASE_PACKAGE = "heron-core-release";
  public static final String VERSIONS_FILENAME_PREFIX = "versions.filename";

  public static final String TOPOLOGY_DEFINITION_FILE = "topology.definition.file";
}
