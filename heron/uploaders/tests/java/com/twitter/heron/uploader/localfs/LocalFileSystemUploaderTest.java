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

package com.twitter.heron.uploader.localfs;

import java.io.File;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.twitter.heron.common.basics.PackageType;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.uploader.UploaderException;

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
        LocalFileSystemConstantsTest.TEST_DATA_PATH).toString();

    // Create the minimum config for tests
    config = Config.newBuilder()
        .put(Keys.cluster(), "cluster")
        .put(Keys.role(), "role")
        .put(Keys.topologyName(), "topology")
        .put(Keys.topologyPackageType(), PackageType.TAR)
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
    Assert.assertNotNull(uploader.uploadPackage());

    // verify if the file exists
    String destFile = uploader.getTopologyFile();
    Assert.assertTrue(new File(destFile).isFile());
  }

  @Test(expected = UploaderException.class)
  public void testSourceNotExists() throws Exception {

    // identify the location of the test topology tar file
    String topologyPackage = Paths.get(
        testTopologyDirectory, "doesnot-exist-topology.tar").toString();

    Config newconfig = Config.newBuilder()
        .putAll(config).put(Keys.topologyPackageFile(), topologyPackage).build();

    // create the uploader and load the package
    LocalFileSystemUploader uploader = new LocalFileSystemUploader();
    uploader.initialize(newconfig);
    uploader.uploadPackage();
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
    Assert.assertNotNull(uploader.uploadPackage());

    // verify if the file exists
    String destFile = uploader.getTopologyFile();
    Assert.assertTrue(new File(destFile).isFile());

    // now undo the file
    uploader.undo();
    Assert.assertFalse(new File(destFile).isFile());
  }
}
