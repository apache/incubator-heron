package com.twitter.heron.spi.common;

public class Keys {
  public static final String HERON_CORE_RELEASE_URI = "heron.core.release.uri";
  public static final String TOPOLOGY_PKG_URI = "heron.topology.pkg.uri";

  public class Config { 
    // Keys provided by user config
    public static final String VERBOSE = "heron.config.verbose";
    public static final String CLUSTER = "heron.config.cluster";
    public static final String ROLE = "heron.config.role";
    public static final String ENVIRON = "heron.config.environ";
    public static final String CONFIG_PATH = "heron.config.path";
    public static final String TOPOLOGY_NAME = "heron.config.topology.name";

    // Keys for user provided classes
    public static final String UPLOADER_CLASS = "heron.uploader.class";
    public static final String LAUNCHER_CLASS = "heron.launcher.class";
    public static final String SCHEDULER_CLASS = "heron.scheduler.class";
    public static final String RUNTIME_MANAGER_CLASS = "heron.runtime.manager.class";
    public static final String PACKING_ALGORITHM_CLASS = "heron.packing.algorithm.class";
    public static final String STATE_MANAGER_CLASS = "heron.state.manager.class";
  };

  public class Runtime {
    public static final String HERON_CORE_RELEASE_URI = "heron.core.release.uri";
    public static final String TOPOLOGY_PKG_URI = "heron.topology.pkg.uri";

    public static final String HERON_RELEASE_PACKAGE = "heron.release.package";
    public static final String HERON_RELEASE_PACKAGE_ROLE = "heron.release.package.role";
    public static final String HERON_RELEASE_PACKAGE_NAME = "heron.release.package.name";
    public static final String HERON_RELEASE_PACKAGE_VERSION = "heron.release.package.version";
    public static final String HERON_UPLOADER_VERSION = "heron.uploader.version";
 }
  
  public static final String TOPOLOGY_DEFINITION_FILE = "topology.definition.file";

  public static final String HERON_DIR = "heron.dir";
  public static final String HERON_CONFIG_LOADER = "heron.config.loader";
  public static final String HERON_AURORA_BIND_PREFIX = "heron.aurora.bind.";
  public static final String HERON_VERBOSE = "heron.verbose";
  public static final String CONFIG_PROPERTY = "config.property";
}
