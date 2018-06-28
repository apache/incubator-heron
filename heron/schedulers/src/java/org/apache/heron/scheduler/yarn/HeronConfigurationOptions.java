/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.scheduler.yarn;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

public class HeronConfigurationOptions {
  @NamedParameter(doc = "Heron-REEF cluster configuration param", default_value = "yarn")
  public class Cluster implements Name<String> {
  }

  @NamedParameter(doc = "Heron-REEF environment configuration parameter", default_value = "default")
  public class Environ implements Name<String> {
  }

  @NamedParameter(doc = "Heron-REEF Role configuration parameter", default_value = "heron")
  public class Role implements Name<String> {
  }

  @NamedParameter(doc = "Topology Jar name")
  public class TopologyJar implements Name<String> {
  }

  @NamedParameter(doc = "Name of topology package file")
  public class TopologyPackageName implements Name<String> {
  }

  @NamedParameter(doc = "Name of heron core package file")
  public class HeronCorePackageName implements Name<String> {
  }

  @NamedParameter(doc = "Component RAM Distribution")
  public class ComponentRamMap implements Name<String> {
  }

  @NamedParameter(doc = "Heron topology Name")
  public class TopologyName implements Name<String> {
  }

  @NamedParameter(doc = "Heron-REEF http port configuration", default_value = "0")
  public class HttpPort implements Name<Integer> {
  }

  @NamedParameter(doc = "Heron Executors Id, 0 = TM, 1 <= worker", default_value = "0")
  public class HeronExecutorId implements Name<Integer> {
  }

  @NamedParameter(doc = "verbose logs", default_value = "false")
  public class VerboseLogMode implements Name<Boolean> {
  }
}
