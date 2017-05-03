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


/**
 * Created by ananth on 4/29/17.
 */
public class EcsContext extends Context {
  public static final String PART1 = "version: '2'\n"
      + "services:\n"
      + "  container_number:\n"
      + "    image: ananthgs/onlyheronandubuntu\n";
  public static final String CMD = "    command: [\"sh\", \"-c\", \"mkdir /s3; cd /s3 ;"
      + "aws s3 cp s3://herondockercal/TOPOLOGY_NAME/topology.tar.gz /s3 ;"
      + "aws s3 cp s3://herondockercal/heron-core-testbuild-ubuntu14.04.tar.gz /s3 ;cd /s3;"
      + " tar -zxvf topology.tar.gz; tar -zxvf heron-core-testbuild-ubuntu14.04.tar.gz;"
      + "heron_executor ;\"] \n";
  public static final String ECSNETWORK = "    networks:\n"
      + "      - heron\n"
      + "    ports:\n"
      + "    - \"5000:5000\"\n"
      + "    - \"5001:5001\"\n"
      + "    - \"5002:5002\"\n"
      + "    - \"5003:5003\"\n"
      + "    - \"5004:5004\"\n"
      + "    - \"5005:5005\"\n"
      + "    - \"5006:5006\"\n"
      + "    - \"5007:5007\"\n"
      + "    - \"5008:5008\"\n"
      + "    - \"5009:5009\"\n"
      + "    volumes:\n"
      + "      - \"herondata:/root/.herondata\"\n"
      + "networks:\n"
      + "  heron:\n"
      + "    driver: bridge\n"
      + "volumes:\n"
      + "  herondata:\n"
      + "    driver: local";
  public static final String DESTINATION_JVM = "/usr/lib/jvm/java-8-oracle";
  public static final String COMPOSE_WORKING_DIR = "/tmp/";

  public static String workingDirectory(Config config) {
    String workingDirectory = config.getStringValue(
        EcsKey.WORKING_DIRECTORY.value(), EcsKey.WORKING_DIRECTORY.getDefaultString());
    return TokenSub.substitute(config, workingDirectory);
  }
}

