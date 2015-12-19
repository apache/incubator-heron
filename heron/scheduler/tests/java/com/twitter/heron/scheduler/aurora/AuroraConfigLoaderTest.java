package com.twitter.heron.scheduler.aurora;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.heron.scheduler.api.Constants;
import com.twitter.heron.scheduler.twitter.PackerUploader;

public class AuroraConfigLoaderTest {
  private static final Logger LOG = Logger.getLogger(AuroraConfigLoaderTest.class.getName());
  private static final ObjectMapper mapper = new ObjectMapper();

  private void addConfig(StringBuilder builder, String key, String value) {
    builder.append(String.format(" %s=\"%s\"", key, value));
  }

  @Test
  public void testAuroraOverrides() throws Exception {
    String override = "dc/role/environ";
    AuroraConfigLoader configLoader = AuroraConfigLoader.class.newInstance();
    configLoader.properties = new Properties();
    // Disables version check
    configLoader.properties.setProperty(Constants.HERON_RELEASE_TAG, "test");
    configLoader.applyConfigOverride(override);
    Assert.assertEquals("dc", configLoader.properties.getProperty(Constants.DC));
    Assert.assertEquals("role", configLoader.properties.getProperty(Constants.ROLE));
    Assert.assertEquals("environ", configLoader.properties.getProperty(Constants.ENVIRON));
  }

  @Test
  public void testAuroraOverridesWithDefaultOverrides() throws Exception {
    String override = "dc/role/environ key1=value1 key2=value2";
    AuroraConfigLoader configLoader = AuroraConfigLoader.class.newInstance();
    configLoader.properties = new Properties();
    configLoader.properties.setProperty(Constants.HERON_RELEASE_TAG, "test");
    configLoader.applyConfigOverride(override);
    Assert.assertEquals("dc", configLoader.properties.getProperty(Constants.DC));
    Assert.assertEquals("role", configLoader.properties.getProperty(Constants.ROLE));
    Assert.assertEquals("environ", configLoader.properties.getProperty(Constants.ENVIRON));
    Assert.assertEquals("value1", configLoader.properties.getProperty("key1"));
    Assert.assertEquals("value2", configLoader.properties.getProperty("key2"));
  }

  @Test
  public void testAuroraRespectRespectHeronVersion() throws Exception {
    StringBuilder override = new StringBuilder("dc/role/environ");

    // Add required heron package defaults
    addConfig(override, Constants.HERON_RELEASE_TAG, "testPackage");
    addConfig(override, Constants.HERON_RELEASE_USER_NAME, "test");
    addConfig(override, Constants.HERON_RELEASE_VERSION, "live");

    AuroraConfigLoader configLoader = AuroraConfigLoader.class.newInstance();
    configLoader.properties = new Properties();
    configLoader.applyConfigOverride(override.toString());
    // Verify translated package
    Assert.assertEquals("live",
        configLoader.properties.getProperty(Constants.HERON_RELEASE_VERSION));
  }

  @Test
  public void testLoadingVersionsFile() throws Exception {
    String myVersion = "myVerison";
    String packerVersion = "1";
    StringBuilder override = new StringBuilder("dc/role/environ");
    addConfig(
        override, Constants.HERON_RELEASE_TAG, Constants.DEFAULT_RELEASE_PACKAGE);
    addConfig(override, Constants.HERON_RELEASE_USER_NAME, "heron");
    addConfig(override, Constants.HERON_RELEASE_VERSION, myVersion);
    // Create versions file
    final File temp = File.createTempFile("temp", Long.toString(System.nanoTime()));
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(temp)));
    Map<String, Object> versionMap = new HashMap<>();
    versionMap.put(myVersion, packerVersion);

    // Convert a JAVA map to JSON String
    String versionMapInJSON = "";
    try {
      versionMapInJSON = mapper.writeValueAsString(versionMap);
    } catch (JsonProcessingException e) {
      LOG.log(Level.SEVERE, "Could not convert map to JSONString: " + versionMap.toString(), e);
    }

    writer.write(versionMapInJSON);
    writer.flush();
    writer.close();
    temp.deleteOnExit();
    addConfig(override, Constants.VERSIONS_FILENAME_PREFIX + ".dc", temp.getName());
    addConfig(override, AuroraLauncher.HERON_DIR, temp.getParent());

    // Verify versions file gets loaded.
    AuroraConfigLoader configLoader = AuroraConfigLoader.class.newInstance();
    configLoader.properties = new Properties();
    configLoader.applyConfigOverride(override.toString());
    // Verify translated package
    Assert.assertEquals(packerVersion,
        configLoader.properties.getProperty(PackerUploader.HERON_PACKER_PKGVERSION));
  }

  @Test
  public void testRespectPackerVersionOverride() throws Exception {
    String myVersion = "myVerison";
    String packerVersion = "1";
    String forcedVersion = "2";
    StringBuilder override = new StringBuilder("dc/role/environ");
    addConfig(
        override, Constants.HERON_RELEASE_TAG, Constants.DEFAULT_RELEASE_PACKAGE);
    addConfig(override, Constants.HERON_RELEASE_USER_NAME, "heron");
    addConfig(override, Constants.HERON_RELEASE_VERSION, myVersion);
    addConfig(override, PackerUploader.HERON_PACKER_PKGVERSION, forcedVersion);

    // Create versions file
    final File temp = File.createTempFile("temp", Long.toString(System.nanoTime()));
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(temp)));

    Map<String, Object> versionMap = new HashMap<>();
    versionMap.put(myVersion, packerVersion);

    // Convert a JAVA map to JSON String
    String versionMapInJSON = "";
    try {
      versionMapInJSON = mapper.writeValueAsString(versionMap);
    } catch (JsonProcessingException e) {
      LOG.log(Level.SEVERE, "Could not convert map to JSONString: " + versionMap.toString(), e);
    }

    writer.write(versionMapInJSON);
    writer.flush();
    writer.close();
    temp.deleteOnExit();
    addConfig(override, Constants.VERSIONS_FILENAME_PREFIX + ".dc", temp.getName());
    addConfig(override, AuroraLauncher.HERON_DIR, temp.getParent());

    // Verify versions file gets loaded.
    AuroraConfigLoader configLoader = AuroraConfigLoader.class.newInstance();
    configLoader.properties = new Properties();
    configLoader.applyConfigOverride(override.toString());
    // Verify translated package
    Assert.assertEquals(forcedVersion,
        configLoader.properties.getProperty(PackerUploader.HERON_PACKER_PKGVERSION));
  }
}