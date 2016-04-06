package com.twitter.heron.scheduler.reef;

import com.twitter.heron.scheduler.reef.HeronConfigurationOptions.*;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.RequiredParameter;

public class HeronTaskConfiguration extends ConfigurationModuleBuilder {
  public static final RequiredParameter<String> TOPOLOGY_NAME = new RequiredParameter<>();
  public static final RequiredParameter<String> TOPOLOGY_JAR = new RequiredParameter<>();
  public static final RequiredParameter<String> CLUSTER = new RequiredParameter<>();
  public static final RequiredParameter<String> ROLE = new RequiredParameter<>();
  public static final RequiredParameter<String> ENV = new RequiredParameter<>();
  public static final RequiredParameter<String> PACKED_PLAN = new RequiredParameter<>();
  public static final RequiredParameter<String> CONTAINER_ID = new RequiredParameter<>();

  public static final ConfigurationModule CONF = new HeronTaskConfiguration().merge(TaskConfiguration.CONF)
          .bindNamedParameter(TopologyName.class, TOPOLOGY_NAME)
          .bindNamedParameter(TopologyJar.class, TOPOLOGY_JAR)
          .bindNamedParameter(Cluster.class, CLUSTER)
          .bindNamedParameter(Environ.class, ENV)
          .bindNamedParameter(Role.class, ROLE)
          .bindNamedParameter(PackedPlan.class, PACKED_PLAN)
          .bindNamedParameter(ContainerId.class, CONTAINER_ID)
          .build();
}
