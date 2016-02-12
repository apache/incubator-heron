package com.twitter.heron.uploaders.packer;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.twitter.heron.spi.utils.ShellUtility;

public class PackerUtility {
  private static final Logger LOG = Logger.getLogger(PackerUtility.class.getName());

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
    if (0 != ShellUtility.runProcess(isVerbose, packerUploadCmd, jsonStrBuilder, null)) {
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
}
