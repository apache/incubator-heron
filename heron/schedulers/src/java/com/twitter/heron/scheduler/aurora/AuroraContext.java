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

package com.twitter.heron.scheduler.aurora;

import java.io.File;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;

public final class AuroraContext extends Context {
  public static final String JOB_LINK_TEMPLATE = "heron.scheduler.job.link.template";
  public static final String JOB_TEMPLATE = "heron.scheduler.job.template";

  private AuroraContext() {
  }

  public static String getJobLinkTemplate(Config config) {
    return config.getStringValue(JOB_LINK_TEMPLATE);
  }

  public static String getHeronAuroraPath(Config config) {
    return config.getStringValue(JOB_TEMPLATE,
        new File(Context.heronConf(config), "heron.aurora").getPath());
  }
}
