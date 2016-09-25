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
package com.twitter.heron.uploader.docker;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;

final class DockerContext extends Context {

  public static final String HERON_UPLOADER_DOCKER_BASE = "heron.uploader.docker.base";
  public static final String HERON_UPLOADER_DOCKER_REPOSITORY = "heron.uploader.docker.registry";
  public static final String HERON_UPLOADER_DOCKER_PUSH = "heron.uploader.docker.push";

  public static String baseImage(Config config) {
    return config.getStringValue(HERON_UPLOADER_DOCKER_BASE);
  }

  public static String dockerRegistry(Config config) {
    return config.getStringValue(HERON_UPLOADER_DOCKER_REPOSITORY);
  }

  public static boolean push(Config config) {
    return config.getBooleanValue(HERON_UPLOADER_DOCKER_PUSH, false);
  }

}
