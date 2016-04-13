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

package com.twitter.heron.uploader.packer;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.twitter.heron.spi.common.ShellUtils;

public class PackerUtils {
  private static final Logger LOG = Logger.getLogger(PackerUtils.class.getName());

  private static final ObjectMapper mapper = new ObjectMapper();

  public static String getTopologyPackageName(String topologyName, String releaseTag) {
    return String.format("heron-topology-%s_%s", topologyName, releaseTag);
  }

  /**
   * Fetch packer URI for a particular package.
   * Throws exception if either packer fetch fails or uri is missing from packer info.
   */
  public static String getPackageURI(String cluster, String role, String packageName,
                                     Boolean isVerbose, String version) {

    LOG.info("Getting packer json info for : " + packageName);
    String packerUploadCmd = String.format(
        "packer get_version --cluster %s %s %s %s --json", cluster, role, packageName, version);

    StringBuilder jsonStrBuilder = new StringBuilder();
    if (0 != ShellUtils.runProcess(isVerbose, packerUploadCmd, jsonStrBuilder, null)) {
      throw new RuntimeException("Fetching info from packer failed");
    } else {
      String jsonStr = jsonStrBuilder.toString();

      return getURIFromPackerResponse(jsonStr);
    }
  }

  public static String getURIFromPackerResponse(String responseInJSON) {
    // Convert the JSON String to JAVA Map
    Map<String, Object> jsonPackerInfo = null;
    try {
      jsonPackerInfo = mapper.readValue(responseInJSON, Map.class);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to parse the String into map: " + responseInJSON, e);
    }

    String packerURI = (String) jsonPackerInfo.get("uri");
    if (packerURI == null) {
      throw new RuntimeException("No URI found in packer info:  " + responseInJSON);
    }
    return packerURI;
  }

  /**
   * Extract package name from given Packer URI
   * @param uriString in the form "packer://{role}/{pkgName}/{version}"
   * @return package name extract from uri
   */
  public static String getReleasePkgName(String uriString) {
    if (!isPackerURI(uriString)) {
      throw new IllegalArgumentException("URI string is not in packer format");
    }

    String[] pkgInfos = uriString.split("/");
    if (pkgInfos.length < 4) {
      throw new IllegalArgumentException("Packer URI string missing components");
    }

    return pkgInfos[3];
  }

  public static String getTopologyURIString(String role, String pkgName, String version) {
      return String.format("packer://%s/%s/%s", role, pkgName, version);
  }

  private static boolean isPackerURI(String uriString) {
    URI url;
    try {
      url = new URI(uriString);
    } catch (Exception e) {
      return false;
    }
    return "packer".equals(url.getScheme());
  }
}
