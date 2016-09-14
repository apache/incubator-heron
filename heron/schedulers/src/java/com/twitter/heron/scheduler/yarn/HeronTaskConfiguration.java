// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.scheduler.yarn;

import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalParameter;
import org.apache.reef.tang.formats.RequiredParameter;

import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.Cluster;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.ComponentRamMap;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.Environ;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.HeronCorePackageName;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.HeronExecutorId;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.Role;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.TopologyJar;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.TopologyName;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.TopologyPackageName;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.VerboseLogMode;

public class HeronTaskConfiguration extends ConfigurationModuleBuilder {
  public static final RequiredParameter<String> TOPOLOGY_NAME = new RequiredParameter<>();
  public static final RequiredParameter<String> TOPOLOGY_JAR = new RequiredParameter<>();
  public static final RequiredParameter<String> TOPOLOGY_PACKAGE_NAME = new RequiredParameter<>();
  public static final RequiredParameter<String> HERON_CORE_PACKAGE_NAME = new RequiredParameter<>();
  public static final RequiredParameter<String> CLUSTER = new RequiredParameter<>();
  public static final RequiredParameter<String> ROLE = new RequiredParameter<>();
  public static final RequiredParameter<String> ENV = new RequiredParameter<>();
  public static final RequiredParameter<String> COMPONENT_RAM_MAP = new RequiredParameter<>();
  public static final RequiredParameter<Integer> CONTAINER_ID = new RequiredParameter<>();
  public static final OptionalParameter<Boolean> VERBOSE = new OptionalParameter<>();

  public static final ConfigurationModule CONF = new HeronTaskConfiguration()
      .merge(TaskConfiguration.CONF)
      .bindNamedParameter(TopologyName.class, TOPOLOGY_NAME)
      .bindNamedParameter(TopologyJar.class, TOPOLOGY_JAR)
      .bindNamedParameter(TopologyPackageName.class, TOPOLOGY_PACKAGE_NAME)
      .bindNamedParameter(HeronCorePackageName.class, HERON_CORE_PACKAGE_NAME)
      .bindNamedParameter(Cluster.class, CLUSTER)
      .bindNamedParameter(Environ.class, ENV)
      .bindNamedParameter(Role.class, ROLE)
      .bindNamedParameter(ComponentRamMap.class, COMPONENT_RAM_MAP)
      .bindNamedParameter(HeronExecutorId.class, CONTAINER_ID)
      .bindNamedParameter(VerboseLogMode.class, VERBOSE)
      .build();
}
