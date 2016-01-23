package com.twitter.heron.scheduler.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.heron.scheduler.api.Constants;

public class DefaultConfigLoaderTest {
  @Test
  public void testParsingEmptyOverride() {
    String override = "";
    DefaultConfigLoader configLoader = new DefaultConfigLoader();
    configLoader.applyConfigOverride(override);
    Assert.assertEquals(0, configLoader.properties.entrySet().size());
  }
  @Test
  public void testParsingSimpleOverride() {
    String override = "key1= value1  key2 =value2    key3   =    \"value3\"";
    DefaultConfigLoader configLoader = new DefaultConfigLoader();
    configLoader.applyConfigOverride(override);
    Assert.assertEquals("value1", configLoader.properties.getProperty("key1"));
    Assert.assertEquals("value2", configLoader.properties.getProperty("key2"));
    Assert.assertEquals("value3", configLoader.properties.getProperty("key3"));
    Assert.assertEquals(3, configLoader.properties.entrySet().size());
  }

  @Test
  public void testParsingOverrideWithSeparators() {
    String override = "key1=\"subkey1 = value1\"  key2=\"subkey2:value2\" ";
    DefaultConfigLoader configLoader = new DefaultConfigLoader();
    configLoader.applyConfigOverride(override);
    Assert.assertEquals("subkey1 = value1", configLoader.properties.getProperty("key1"));
    Assert.assertEquals("subkey2:value2", configLoader.properties.getProperty("key2"));
    Assert.assertEquals(2, configLoader.properties.entrySet().size());
  }

  @Test
  public void testParsingOverrideWithQuotes() {
    String override = "key1=\"foo:\\\"subkey1 = value1\\\"\"  ";
    DefaultConfigLoader configLoader = new DefaultConfigLoader();
    configLoader.applyConfigOverride(override);
    Assert.assertEquals("foo:\"subkey1 = value1\"", configLoader.properties.getProperty("key1"));
    Assert.assertEquals(1, configLoader.properties.entrySet().size());
  }

  @Test
  public void testParsingOverrideWithEscape() {
    String override = "key1=foo\\:\"subkey1 = value1\"  key2=subkey2\\=value2";
    DefaultConfigLoader configLoader = new DefaultConfigLoader();
    configLoader.applyConfigOverride(override);
    Assert.assertEquals("foo:\"subkey1 = value1\"", configLoader.properties.getProperty("key1"));
    Assert.assertEquals("subkey2=value2", configLoader.properties.getProperty("key2"));
    Assert.assertEquals(2, configLoader.properties.entrySet().size());
  }

  @Test
  public void testLoadingConfigFromFile() throws Exception {
    final File temp = File.createTempFile("temp", Long.toString(System.nanoTime()));
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(temp)));
    int numLines = 10;
    for (int i = 0; i < numLines; ++i) {
      writer.write(String.format("key%d=value%d", i, i));
      writer.newLine();
    }
    writer.flush();
    writer.close();
    temp.deleteOnExit();
    DefaultConfigLoader configLoader = DefaultConfigLoader.class.newInstance();
    Assert.assertTrue(configLoader.load(temp.getAbsolutePath(), ""));
    for (int i = 0; i < numLines; ++i) {
      Assert.assertEquals("value" + i, configLoader.properties.getProperty("key" + i));
    }
    // Verify defaults are present
    Assert.assertTrue(configLoader.properties.containsKey(Constants.HERON_VERBOSE));
  }
}