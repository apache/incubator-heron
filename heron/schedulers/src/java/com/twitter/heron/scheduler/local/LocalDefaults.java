package com.twitter.heron.scheduler.local;

public class LocalDefaults {
  public static final String CORE_PACKAGE_URI = "${HERON_DIST}/heron-core.tar.gz";
  public static final String WORKING_DIRECTORY = "${HOME}/.heron/topologies/${CLUSTER}/${ROLE}/${TOPOLOGY}";
}
