package com.twitter.heron.uploader.localfs;

import java.util.HashMap;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.commons.io.FileUtils;

import junit.framework.Assert;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;

import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context; 

public class LocalFileSystemUploaderTest {

  private Config config;
  private String fileSystemDirectory;
  private String testTopologyDirectory;

  @Before
  public void before() throws Exception {

    // form the file system directory using bazel environ files
    fileSystemDirectory = Paths.get(System.getenv("JAVA_RUNFILES"), "topologies").toString();

    // form the test topology directory
    testTopologyDirectory = Paths.get(System.getenv("JAVA_RUNFILES"), 
        LocalFileSystemConstants.TEST_DATA_PATH).toString();

    // Create the minimum config for tests
    config = Config.newBuilder()
        .put(Keys.cluster(), "cluster")
        .put(Keys.role(), "role")
        .put(Keys.topologyName(), "topology")
        .put(Keys.topologyPackageType(), "tar")
        .put(LocalFileSystemKeys.fileSystemDirectory(), fileSystemDirectory)
        .build();
  }

  @After
  public void after() throws Exception {
    FileUtils.deleteDirectory(new File(fileSystemDirectory));
  }

  @Test
  public void testUploader() throws Exception {

    // identify the location of the test topology tar file
    String topologyPackage = Paths.get(testTopologyDirectory, "some-topology.tar").toString();

    Config newconfig = Config.newBuilder()
        .putAll(config).put(Keys.topologyPackageFile(), topologyPackage).build();

    // create the uploader and load the package
    LocalFileSystemUploader uploader = new LocalFileSystemUploader(); 
    uploader.initialize(newconfig);
    Assert.assertTrue(uploader.uploadPackage());

    // verify if the file exists
    String destFile = uploader.getTopologyFile();
    Assert.assertTrue(new File(destFile).isFile());
  }

  @Test
  public void testSourceNotExists() throws Exception {

    // identify the location of the test topology tar file
    String topologyPackage = Paths.get(testTopologyDirectory, "doesnot-exist-topology.tar").toString();

    Config newconfig = Config.newBuilder()
        .putAll(config).put(Keys.topologyPackageFile(), topologyPackage).build();

    // create the uploader and load the package
    LocalFileSystemUploader uploader = new LocalFileSystemUploader(); 
    uploader.initialize(newconfig);

    // Assert that the file does not exist
    Assert.assertFalse(uploader.uploadPackage());
  }

  @Test
  public void testUndo() throws Exception {

    // identify the location of the test topology tar file
    String topologyPackage = Paths.get(testTopologyDirectory, "some-topology.tar").toString();

    Config newconfig = Config.newBuilder()
        .putAll(config).put(Keys.topologyPackageFile(), topologyPackage).build();

    // create the uploader and load the package
    LocalFileSystemUploader uploader = new LocalFileSystemUploader(); 
    uploader.initialize(newconfig);
    Assert.assertTrue(uploader.uploadPackage());

    // verify if the file exists
    String destFile = uploader.getTopologyFile();
    Assert.assertTrue(new File(destFile).isFile());

    // now undo the file
    uploader.undo();
    Assert.assertFalse(new File(destFile).isFile());
  }
}
