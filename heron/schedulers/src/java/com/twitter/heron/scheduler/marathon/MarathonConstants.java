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

package com.twitter.heron.scheduler.marathon;

public final class MarathonConstants {
  private MarathonConstants() {

  }

  public static final String ID = "id";
  public static final String COMMAND = "cmd";
  public static final String CPU = "cpus";
  public static final String MEMORY = "mem";
  public static final String DISK = "disk";
  public static final String PORT_DEFINITIONS = "portDefinitions";
  public static final String INSTANCES = "instances";
  public static final String LABELS = "labels";
  public static final String FETCH = "fetch";
  public static final String USER = "user";
  public static final String APPS = "apps";
  public static final String ENVIRONMENT = "environment";
  public static final String URI = "uri";
  public static final String EXECUTABLE = "executable";
  public static final String EXTRACT = "extract";
  public static final String CACHE = "cache";
  public static final String PORT = "port";
  public static final String PROTOCOL = "protocol";
  public static final String PORT_NAME = "name";
  public static final String TCP = "tcp";

  public static final String[] PORT_NAMES = new String[]{
      "master", "tmaster-controller", "tmaster-stats", "shell", "metricsmgr", "scheduler"};

  public static final String MASTER_PORT = "$PORT0";
  public static final String TMASTER_CONTROLLER_PORT = "$PORT1";
  public static final String TMASTER_STATS_PORT = "$PORT2";
  public static final String SHELL_PORT = "$PORT3";
  public static final String METRICSMGR_PORT = "$PORT4";
  public static final String SCHEDULER_PORT = "$PORT5";

  public static final String[] PORT_LIST = new String[]{
      MASTER_PORT, TMASTER_CONTROLLER_PORT, TMASTER_STATS_PORT,
      SHELL_PORT, METRICSMGR_PORT, SCHEDULER_PORT};

  public static final String JOB_LINK = "/ui/#/group/%2F";
}
