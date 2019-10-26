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

package org.apache.heron.uploader.localfs;

import java.io.File;
import java.nio.file.Paths;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.commons.io.FileUtils;
import org.apache.heron.common.basics.PackageType;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Key;
import org.apache.heron.spi.uploader.UploaderException;

public class LocalFileSystemUploaderTest {

  private static final String TOPOLOGY_PACKAGE_FILE_NAME = "some-topology.tar";

  private Config config;
  private String fileSystemDirectory;
  private String testTopologyDirectory;

  @Before
  public void before() throws Exception {
    // form the file system directory using bazel environ files
    fileSystemDirectory = Paths.get(System.getenv("JAVA_RUNFILES"), "topologies").toString();

    // form the test topology directory
    testTopologyDirectory = Paths.get(System.getenv("JAVA_RUNFILES"),
        LocalFileSystemConstantsTest.TEST_DATA_PATH).toString();

    // Create the minimum config for tests
    config = Config.newBuilder()
        .put(Key.CLUSTER, "cluster")
        .put(Key.ROLE, "role")
        .put(Key.TOPOLOGY_NAME, "topology")
        .put(Key.TOPOLOGY_PACKAGE_TYPE, PackageType.TAR)
        .put(LocalFileSystemKey.FILE_SYSTEM_DIRECTORY.value(), fileSystemDirectory)
        .build();
  }

  @After
  public void after() throws Exception {
    FileUtils.deleteDirectory(new File(fileSystemDirectory));
  }

  @Test
  public void testUploader() {
    // identify the location of the test topology tar file
    String topologyPackage = Paths.get(testTopologyDirectory,
        TOPOLOGY_PACKAGE_FILE_NAME).toString();

    Config newConfig = Config.newBuilder()
        .putAll(config).put(Key.TOPOLOGY_PACKAGE_FILE, topologyPackage).build();

    // create the uploader and load the package
    LocalFileSystemUploader uploader = new LocalFileSystemUploader();
    uploader.initialize(newConfig);
    Assert.assertNotNull(uploader.uploadPackage());

    // verify if the file exists
    String destFile = uploader.getTopologyFile();
    Assert.assertTrue(new File(destFile).isFile());
  }

  @Test(expected = UploaderException.class)
  public void testSourceNotExists() {
    // identify the location of the test topology tar file
    String topologyPackage = Paths.get(
        testTopologyDirectory, "doesnot-exist-topology.tar").toString();

    Config newConfig = Config.newBuilder()
        .putAll(config).put(Key.TOPOLOGY_PACKAGE_FILE, topologyPackage).build();

    // create the uploader and load the package
    LocalFileSystemUploader uploader = new LocalFileSystemUploader();
    uploader.initialize(newConfig);
    uploader.uploadPackage();
  }

  @Test
  public void testUndo() {
    // identify the location of the test topology tar file
    String topologyPackage = Paths.get(testTopologyDirectory,
        TOPOLOGY_PACKAGE_FILE_NAME).toString();

    Config newConfig = Config.newBuilder()
        .putAll(config).put(Key.TOPOLOGY_PACKAGE_FILE, topologyPackage).build();

    // create the uploader and load the package
    LocalFileSystemUploader uploader = new LocalFileSystemUploader();
    uploader.initialize(newConfig);
    Assert.assertNotNull(uploader.uploadPackage());

    // verify if the file exists
    String destFile = uploader.getTopologyFile();
    Assert.assertTrue(new File(destFile).isFile());

    // now undo the file
    Assert.assertTrue(uploader.undo());
    Assert.assertFalse(new File(destFile).isFile());
  }

  @Test
  public void testUseDefaultFileSystemDirectoryWhenNotSet() {
    // identify the location of the test topology tar file
    String topologyPackage = Paths.get(testTopologyDirectory,
        TOPOLOGY_PACKAGE_FILE_NAME).toString();

    // set file system directory as null
    Config newConfig = Config.newBuilder()
        .putAll(config)
        .put(Key.TOPOLOGY_PACKAGE_FILE, topologyPackage)
        .put(LocalFileSystemKey.FILE_SYSTEM_DIRECTORY.value(), null)
        .build();

    // create the uploader
    LocalFileSystemUploader uploader = new LocalFileSystemUploader();
    uploader.initialize(newConfig);

    // get default file system directory
    String defaultFileSystemDirectory = LocalFileSystemKey.FILE_SYSTEM_DIRECTORY.getDefaultString();

    String destDirectory = uploader.getTopologyDirectory();
    String destFile = uploader.getTopologyFile();

    // verify usage of default file system directory for destination directory and file
    Assert.assertEquals(destDirectory, defaultFileSystemDirectory);
    Assert.assertTrue(destFile.contains(defaultFileSystemDirectory));
  }

  @Test
  public void testUploadPackageWhenTopologyFileAlreadyExists() {
    // identify the location of the test topology tar file
    String topologyPackage = Paths.get(testTopologyDirectory,
        TOPOLOGY_PACKAGE_FILE_NAME).toString();

    Config newConfig = Config.newBuilder()
        .putAll(config).put(Key.TOPOLOGY_PACKAGE_FILE, topologyPackage).build();

    // create the uploader and load the package
    LocalFileSystemUploader uploader = new LocalFileSystemUploader();
    uploader.initialize(newConfig);
    Assert.assertNotNull(uploader.uploadPackage());

    // verify if the file exists
    String destFile = uploader.getTopologyFile();
    Assert.assertTrue(new File(destFile).isFile());

    // load same package again by overriding existing one
    Assert.assertNotNull(uploader.uploadPackage());
    String destFile2 = uploader.getTopologyFile();
    Assert.assertTrue(new File(destFile2).isFile());

    // verify that existing file is overridden
    Assert.assertEquals(destFile, destFile2);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUploadPackageWhenFileSystemDirectoryIsInvalid() {
    // identify the location of the test topology tar file
    String topologyPackage = Paths.get(testTopologyDirectory,
        TOPOLOGY_PACKAGE_FILE_NAME).toString();

    String invalidFileSystemDirectory = "invalid%path";

    // set invalid file system directory
    Config newConfig = Config.newBuilder()
        .putAll(config)
        .put(Key.TOPOLOGY_PACKAGE_FILE, topologyPackage)
        .put(LocalFileSystemKey.FILE_SYSTEM_DIRECTORY.value(), invalidFileSystemDirectory).build();

    // create the uploader and load the package
    LocalFileSystemUploader uploader = new LocalFileSystemUploader();
    uploader.initialize(newConfig);
    uploader.uploadPackage();
  }

  @Test
  public void testGetUri() {
    LocalFileSystemUploader uploader = new LocalFileSystemUploader();
    Assert.assertEquals(uploader.getUri("testFileName").toString(), "file://testFileName");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetUriWhenDestFileNameIsInvalid() {
    LocalFileSystemUploader uploader = new LocalFileSystemUploader();
    uploader.getUri("invalid_%_DestFilePath");
  }

}
