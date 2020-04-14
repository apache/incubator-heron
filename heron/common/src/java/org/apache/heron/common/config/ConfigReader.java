/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.common.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

/**
 * Loads config file in Yaml file format.
 */
public final class ConfigReader {
  private static final Logger LOG = Logger.getLogger(ConfigReader.class.getName());

  private ConfigReader() {
  }

  /**
   * Load properties from the given YAML file
   *
   * @param fileName the name of YAML file to read
   *
   * @return Map, contains the key value pairs of config
   */
  @SuppressWarnings("unchecked") // when we cast yaml.load(fin)
  public static Map<String, Object> loadFile(String fileName) {
    Map<String, Object> props = new HashMap<>();
    if (fileName == null) {
      LOG.warning("Config file name cannot be null");
      return props;
    } else if (fileName.isEmpty()) {
      LOG.warning("Config file name is empty");
      return props;
    } else {

      // check if the file exists and also it is a regular file
      Path path = Paths.get(fileName);

      if (!Files.exists(path)) {
        LOG.fine("Config file " + fileName + " does not exist");
        return props;
      }

      if (!Files.isRegularFile(path)) {
        LOG.warning("Config file " + fileName + " might be a directory.");
        return props;
      }

      LOG.log(Level.FINE, "Reading config file {0}", fileName);

      Map<String, Object> propsYaml = null;
      try {
        FileInputStream fin = new FileInputStream(new File(fileName));
        try {
          Yaml yaml = new Yaml(new SafeConstructor());
          propsYaml = (Map<String, Object>) yaml.load(fin);
          LOG.log(Level.FINE, "Successfully read config file {0}", fileName);
        } finally {
          fin.close();
        }
      } catch (IOException e) {
        LOG.log(Level.SEVERE, "Failed to load config file: " + fileName, e);
      }

      return propsYaml != null ? propsYaml : props;
    }
  }

  /**
   * Load config from the given YAML stream
   *
   * @param inputStream the name of YAML stream to read
   *
   * @return Map, contains the key value pairs of config
   */
  @SuppressWarnings("unchecked") // yaml.load API returns raw Map
  public static Map<String, Object> loadStream(InputStream inputStream) {
    LOG.fine("Reading config stream");

    Yaml yaml = new Yaml(new SafeConstructor());
    Map<Object, Object> propsYaml = (Map<Object, Object>) yaml.load(inputStream);
    LOG.fine("Successfully read config");

    Map<String, Object> typedMap = new HashMap<>();
    for (Object key: propsYaml.keySet()) {
      typedMap.put(key.toString(), propsYaml.get(key));
    }

    return typedMap;
  }
}
