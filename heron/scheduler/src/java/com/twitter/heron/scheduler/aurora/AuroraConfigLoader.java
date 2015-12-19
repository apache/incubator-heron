package com.twitter.heron.scheduler.aurora;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.twitter.heron.scheduler.api.Constants;
import com.twitter.heron.scheduler.twitter.PackerUploader;
import com.twitter.heron.scheduler.util.DefaultConfigLoader;

public class AuroraConfigLoader extends DefaultConfigLoader {
  private static final Logger LOG = Logger.getLogger(AuroraConfigLoader.class.getName());
  private static final ObjectMapper mapper = new ObjectMapper();

  public boolean translatePackageVersion() {
    // TODO(nbhagat) : Don't hard-code any default values. Read from config.
    String cluster = properties.getProperty(Constants.DC);
    String releasePkgName = properties.getProperty(
        Constants.HERON_RELEASE_TAG, Constants.DEFAULT_RELEASE_PACKAGE);
    String releasePkgVersion = properties.getProperty(
        Constants.HERON_RELEASE_VERSION, "live");
    String heronDir = properties.getProperty(AuroraLauncher.HERON_DIR);

    if (properties.containsKey(PackerUploader.HERON_PACKER_PKGVERSION)) {
      return true;
    }
    if (!releasePkgName.equals(Constants.DEFAULT_RELEASE_PACKAGE)) {
      properties.setProperty(PackerUploader.HERON_PACKER_PKGVERSION, releasePkgVersion);
      return true;
    }
    String versionsFile = properties.getProperty(
        Constants.VERSIONS_FILENAME_PREFIX + "." + cluster, "versions.conf");
    // Try getting version map.
    try {
      String content = new String(Files.readAllBytes(Paths.get(heronDir, versionsFile)));

      // Convert the JSON String to JAVA Map
      Map<String, Object> versionsMap = null;
      try {
        versionsMap = mapper.readValue(content, Map.class);
      } catch (IOException e) {
        LOG.log(Level.SEVERE, "Failed to parse the String into map: " + content, e);
      }

      if (releasePkgVersion == null
          || releasePkgVersion.isEmpty()
          || "live".equals(releasePkgVersion)) {
        Integer maxVersion = 1;
        String maxVersionStr = "live";
        for (Object versionStr : versionsMap.keySet()) {
          int version = Integer.parseInt(versionsMap.get(versionStr).toString());
          if (maxVersion < version) {
            maxVersion = version;
            maxVersionStr = versionStr.toString();
          }
        }
        properties.setProperty(PackerUploader.HERON_PACKER_PKGVERSION, maxVersion.toString());
        properties.setProperty(Constants.HERON_RELEASE_VERSION, maxVersionStr);
      } else {
        if (!versionsMap.containsKey(releasePkgVersion)) {
          LOG.severe("Requested version of heron-core release doesn't exist");
          return false;
        }
        properties.setProperty(PackerUploader.HERON_PACKER_PKGVERSION,
            versionsMap.get(releasePkgVersion).toString());
      }
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to find / parse versions file: " + versionsFile, e);
      LOG.log(Level.SEVERE, "Continuing with live label");
      properties.setProperty(PackerUploader.HERON_PACKER_PKGVERSION, "live");
    }
    return true;
  }

  @Override
  public boolean applyConfigOverride(String configOverride) {
    // Follow the style dc/role/environ
    String[] parts = configOverride.trim().split(" ", 2);
    if (parts.length == 0) {
      LOG.severe("dc/role/environ is required.");
      return false;
    }
    String clusterInfo = parts[0];
    String[] clusterParts = clusterInfo.split("/");
    if (clusterParts.length != 3) {
      LOG.severe("Cluster parts must be dc/role/environ (without spaces)");
      return false;
    }

    properties.setProperty(Constants.DC, clusterParts[0]);
    properties.setProperty(Constants.ROLE, clusterParts[1]);
    properties.setProperty(Constants.ENVIRON, clusterParts[2]);
    if (parts.length == 2 && !parts[1].isEmpty()) {
      if (!super.applyConfigOverride(parts[1])) {
        return false;
      }
    }

    // Deal with the rest of config
    return translatePackageVersion();
  }
}