//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package com.twitter.heron.scheduler.nomad;

import java.io.File;
import java.util.logging.Logger;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;

public class NomadContext extends Context {
  private static final Logger LOG = Logger.getLogger(NomadContext.class.getName());

  public static final String HERON_NOMAD_SCHEDULER_URI = "heron.nomad.scheduler.uri";

  public static final String HERON_NOMAD_CORE_FREQ_MAPPING = "heron.nomad.core.freq.mapping";

  public static String workingDirectory(Config config) {
    return config.getStringValue(
        NomadKey.WORKING_DIRECTORY.value(), NomadKey.WORKING_DIRECTORY.getDefaultString());
  }

  public static String getHeronNomadPath(Config config) {
    String filePath = config.getStringValue(NomadKey.JOB_TEMPLATE.value(),
        Context.heronConf(config) + "/" + NomadKey.JOB_TEMPLATE.getDefaultString());
    return new File(filePath).getPath();
  }

  public static String getSchedulerURI(Config config) {
    return config.getStringValue(HERON_NOMAD_SCHEDULER_URI);
  }

  public static int getCoreFreqMapping(Config config) {
    return config.getIntegerValue(HERON_NOMAD_CORE_FREQ_MAPPING, 1000);
  }
}
