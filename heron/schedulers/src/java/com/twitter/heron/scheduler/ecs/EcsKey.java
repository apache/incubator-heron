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

package com.twitter.heron.scheduler.ecs;

import com.twitter.heron.spi.common.Key;

public enum EcsKey {
  // config key for specifying the destination topology binary file
  ECS_CLUSTER_BINARY("heron.ecs.topology.binary.file", "heron-examples.jar"),
  ECS_COMPOSE_TEMPLATE("heron.ecs.compose.template.file",
                        "${HOME}/.heron/conf/ecs/ecs_compose_template.yaml"),
  ECS_AMI_INSTANCE("heron.ecs.ami.instance", "http://169.254.169.254/latest/meta-data/local-ipv4"),
  ECS_COMPOSE_UPCMD("heron.ecs.compose.up", "ecs-cli compose --project-name "),
  ECS_COMPOSE_STOP("heron.ecs.compose.up", "aws ecs stop-task --cluster default --task "),
  //ECS_COMPOSE_LIST("heron.ecs.compose.up", "ecs-cli ps")
  ECS_COMPOSE_LIST("heron.ecs.compose.up", "aws ecs list-tasks --family "),
  ECS_LIST_BY("heron.ecs.list.by", "families"),
  ECS_TASK_TAG("heron.ecs.task.tag", "taskArns"),
  ECS_GET_FAMILY("heron.ecs.family.name",
                  "aws ecs list-task-definition-families --family-prefix ecscompose-"),
  WORKING_DIRECTORY("heron.scheduler.ecs.working.directory",
      "${HOME}/.herondata/topologies/${CLUSTER}/${ROLE}/${TOPOLOGY}");


  private final String value;
  private final Key.Type type;
  private final Object defaultValue;

  EcsKey(String value, String defaultValue) {
    this.value = value;
    this.type = Key.Type.STRING;
    this.defaultValue = defaultValue;
  }

  public String value() {
    return value;
  }

  public Object getDefault() {
    return defaultValue;
  }

  public String getDefaultString() {
    if (type != Key.Type.STRING) {
      throw new IllegalAccessError(String.format(
          "Config Key %s is type %s, getDefaultString() not supported", this.name(), this.type));
    }
    return (String) this.defaultValue;
  }

}
