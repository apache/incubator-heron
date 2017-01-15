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

package com.twitter.heron.spi.common;

import java.io.InputStream;
import java.util.Map;
import java.util.logging.Logger;

import com.twitter.heron.common.config.ConfigReader;

public final class Resource {
  private static final Logger LOG = Logger.getLogger(Resource.class.getName());

  private Resource() {
  }

  /*
   * Loads the YAML file specified as a resource
   *
   * @param className, the name of the class
   * @param resName, the name of the resource
   *
   * @return Map, a map of key value pairs
   */
  public static Map<String, Object> load(String className, String resName)
      throws ClassNotFoundException {

    // get the class loader for current class
    ClassLoader cLoader = Class.forName(className).getClassLoader();

    // open the resource file and parse the yaml
    InputStream fileStream = cLoader.getResourceAsStream(resName);
    Map<String, Object> kvPairs = ConfigReader.loadStream(fileStream);

    // if nothing there exit, since this config is mandatory
    if (kvPairs.isEmpty()) {
      LOG.severe("Config keys cannot be empty ");
      throw new RuntimeException("Config keys file cannot be empty");
    }

    return kvPairs;
  }
}
