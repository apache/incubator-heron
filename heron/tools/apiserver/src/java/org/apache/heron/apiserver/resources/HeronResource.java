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

package org.apache.heron.apiserver.resources;

import javax.servlet.ServletContext;
import javax.ws.rs.core.Context;

import org.apache.heron.spi.common.Config;

public class HeronResource {

  public static final String ATTRIBUTE_CLUSTER = "cluster";
  public static final String ATTRIBUTE_CONFIGURATION = "configuration";
  public static final String ATTRIBUTE_CONFIGURATION_DIRECTORY = "configuration_directory";
  public static final String ATTRIBUTE_CONFIGURATION_OVERRIDE_PATH = "configuration_override";
  public static final String ATTRIBUTE_PORT = "port";
  public static final String ATTRIBUTE_DOWNLOAD_HOSTNAME = "download_hostname";
  public static final String ATTRIBUTE_HERON_CORE_PACKAGE_PATH = "heron_core_package_path";

  @Context
  protected ServletContext servletContext;

  private Config baseConfiguration;
  private String configurationDirectory;
  private String configurationOverridePath;
  private String cluster;
  private String port;
  private String downloadHostname;
  private String heronCorePackagePath;

  Config getBaseConfiguration() {
    if (baseConfiguration == null) {
      baseConfiguration = (Config) servletContext.getAttribute(ATTRIBUTE_CONFIGURATION);
    }
    return baseConfiguration;
  }

  String getConfigurationDirectory() {
    if (configurationDirectory == null) {
      configurationDirectory =
          (String) servletContext.getAttribute(ATTRIBUTE_CONFIGURATION_DIRECTORY);
    }
    return configurationDirectory;
  }

  String getConfigurationOverridePath() {
    if (configurationOverridePath == null) {
      configurationOverridePath =
          (String) servletContext.getAttribute(ATTRIBUTE_CONFIGURATION_OVERRIDE_PATH);
    }

    return configurationOverridePath;
  }

  String getCluster() {
    if (cluster == null) {
      cluster = (String) servletContext.getAttribute(ATTRIBUTE_CLUSTER);
    }

    return cluster;
  }

  String getPort() {
    if (port == null) {
      port = (String) servletContext.getAttribute(ATTRIBUTE_PORT);
    }

    return port;
  }

  String getDownloadHostName() {
    if (downloadHostname == null) {
      downloadHostname = (String) servletContext.getAttribute(ATTRIBUTE_DOWNLOAD_HOSTNAME);
    }

    return downloadHostname;
  }

  String getHeronCorePackagePath() {
    if (heronCorePackagePath == null) {
      heronCorePackagePath
          = (String) servletContext.getAttribute(ATTRIBUTE_HERON_CORE_PACKAGE_PATH);
    }

    return heronCorePackagePath;
  }

}
