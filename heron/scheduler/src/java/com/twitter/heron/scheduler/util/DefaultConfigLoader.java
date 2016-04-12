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

package com.twitter.heron.scheduler.util;

import java.util.logging.Logger;

/**
 * Loads config file in Java properties file format.
 */
public class DefaultConfigLoader extends AbstractPropertiesConfigLoader {
  private static final Logger LOG = Logger.getLogger(DefaultConfigLoader.class.getName());

  public boolean load(String configFile, String configOverride) {
    PropertiesFileConfigLoader baseLoader = new PropertiesFileConfigLoader();
    String propertyOverride = preparePropertyOverride(configOverride);
    if (baseLoader.load(configFile, propertyOverride)) {
      properties.putAll(baseLoader.getProperties());
      return true;
    } else {
      return false;
    }
  }
}
