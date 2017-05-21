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

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.TokenSub;

public class EcsContext extends Context {

  public static String ecsClusterBinary(Config config) {
    String ecsClusterBinary = config.getStringValue(
        EcsKey.ECS_CLUSTER_BINARY.value(), EcsKey.ECS_CLUSTER_BINARY.getDefaultString());
    return TokenSub.substitute(config, ecsClusterBinary);
  }
  public static String workingDirectory(Config config) {
    String workingDirectory = config.getStringValue(
        EcsKey.WORKING_DIRECTORY.value(), EcsKey.WORKING_DIRECTORY.getDefaultString());
    return TokenSub.substitute(config, workingDirectory);
  }

  public static String ecsComposeTemplate(Config config) {
    String composeTemplate = config.getStringValue(
        EcsKey.ECS_COMPOSE_TEMPLATE.value(), EcsKey.ECS_COMPOSE_TEMPLATE.getDefaultString());
    return TokenSub.substitute(config, composeTemplate);
  }

  public static String AmiInstanceUrl(Config config) {
    String amiInstanceUrl = config.getStringValue(
        EcsKey.ECS_AMI_INSTANCE.value(), EcsKey.ECS_AMI_INSTANCE.getDefaultString());
    return TokenSub.substitute(config, amiInstanceUrl);
  }
  public static String composeupCmd(Config config) {
    String amiInstanceUrl = config.getStringValue(
        EcsKey.ECS_COMPOSE_UPCMD.value(), EcsKey.ECS_COMPOSE_UPCMD.getDefaultString());
    return TokenSub.substitute(config, amiInstanceUrl);
  }
  public static String composeStopCmd(Config config) {
    String amiInstanceUrl = config.getStringValue(
        EcsKey.ECS_COMPOSE_STOP.value(), EcsKey.ECS_COMPOSE_STOP.getDefaultString());
    return TokenSub.substitute(config, amiInstanceUrl);
  }
  public static String composeListCmd(Config config) {
    String amiInstanceUrl = config.getStringValue(
        EcsKey.ECS_COMPOSE_LIST.value(), EcsKey.ECS_COMPOSE_LIST.getDefaultString());
    return TokenSub.substitute(config, amiInstanceUrl);
  }
  public static String composeListby(Config config) {
    String amiInstanceUrl = config.getStringValue(
        EcsKey.ECS_LIST_BY.value(), EcsKey.ECS_LIST_BY.getDefaultString());
    return TokenSub.substitute(config, amiInstanceUrl);
  }
  public static String composeFamilyName(Config config) {
    String amiInstanceUrl = config.getStringValue(
        EcsKey.ECS_GET_FAMILY.value(), EcsKey.ECS_GET_FAMILY.getDefaultString());
    return TokenSub.substitute(config, amiInstanceUrl);
  }
  public static String composeTaskTag(Config config) {
    String amiInstanceUrl = config.getStringValue(
        EcsKey.ECS_TASK_TAG.value(), EcsKey.ECS_TASK_TAG.getDefaultString());
    return TokenSub.substitute(config, amiInstanceUrl);
  }
}

