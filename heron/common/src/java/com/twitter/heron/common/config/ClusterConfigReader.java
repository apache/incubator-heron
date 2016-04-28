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

package com.twitter.heron.common.config;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Loads config file in Yaml properties file format based
 * on cluster and config path
 */
public final class ClusterConfigReader extends ConfigReader {
  private static final Logger LOG = Logger.getLogger(ClusterConfigReader.class.getName());

  public static Map<String, Object> load(String cluster, String configPath, String fileName) {
    Map<String, Object> config = new HashMap<>();

    // Read the defaults file, first
    String file1 = Paths.get(configPath, fileName).toString();
    LOG.info("Cluster " + cluster + " config file " + file1 + "\n");
    Map<String, Object> props1 = loadFile(file1);

    if (props1 == null) {
      LOG.info("props1 is null \n");
    }

    if (props1.isEmpty()) {
      LOG.info("Config file " + file1 + " is empty");
    }

    // Read the cluster specific file and override
    String file2 = Paths.get(configPath, cluster, fileName).toString();
    Map<String, Object> props2 = loadFile(file2);
    if (props2.isEmpty()) {
      LOG.info("Config file " + file2 + " is empty");
    }

    // If both files have errors, return false
    if (props1.isEmpty() && props2.isEmpty()) {
      LOG.info("Files " + file1 + " and " + file2 + " are not present");
      return config;
    }

    // Save the properties and return successfully
    config.putAll(props1);
    config.putAll(props2);
    return config;
  }
}
