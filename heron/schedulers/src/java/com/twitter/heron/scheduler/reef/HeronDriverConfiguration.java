package com.twitter.heron.scheduler.reef;

import com.twitter.heron.scheduler.reef.HeronConfigurationOptions.*;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.RequiredParameter;

/**
 * {@link HeronDriverConfiguration} constructs optional and required configuration needed by Heron Master Driver
 */
public class HeronDriverConfiguration extends ConfigurationModuleBuilder {
  public static final RequiredParameter<String> TOPOLOGY_NAME = new RequiredParameter<>();
  public static final RequiredParameter<String> TOPOLOGY_JAR = new RequiredParameter<>();
  public static final RequiredParameter<String> CLUSTER = new RequiredParameter<>();
  public static final RequiredParameter<String> ROLE = new RequiredParameter<>();
  public static final RequiredParameter<String> ENV = new RequiredParameter<>();
  public static final RequiredParameter<Integer> HTTP_PORT = new RequiredParameter<>();

  public static final ConfigurationModule CONF = new HeronDriverConfiguration().merge(DriverConfiguration.CONF)
          .bindNamedParameter(TopologyName.class, TOPOLOGY_NAME)
          .bindNamedParameter(TopologyJar.class, TOPOLOGY_JAR)
          .bindNamedParameter(Cluster.class, CLUSTER)
          .bindNamedParameter(Environ.class, ENV)
          .bindNamedParameter(Role.class, ROLE)
          .bindNamedParameter(HttpPort.class, HTTP_PORT)
          .build();
}
