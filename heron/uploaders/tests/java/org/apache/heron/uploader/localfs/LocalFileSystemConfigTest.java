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

import java.nio.file.Paths;

import org.junit.Assert;
import org.junit.Test;

import org.apache.heron.common.basics.PackageType;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Key;
import org.apache.heron.spi.common.TokenSub;

public class LocalFileSystemConfigTest {

  private Config getDefaultConfig() {
    return Config.newBuilder()
        .put(Key.CLUSTER, "cluster")
        .put(Key.ROLE, "role")
        .put(Key.TOPOLOGY_NAME, "topology")
        .put(Key.TOPOLOGY_PACKAGE_TYPE, PackageType.TAR)
        .put(Key.TOPOLOGY_PACKAGE_FILE, "/tmp/something.tar.gz")
        .put(LocalFileSystemKey.FILE_SYSTEM_DIRECTORY.value(),
            LocalFileSystemKey.FILE_SYSTEM_DIRECTORY.getDefaultString())
        .build();
  }

  @Test
  public void testDefaultConfig() throws Exception {
    Config config = Config.toLocalMode(getDefaultConfig());

    Assert.assertEquals(
        LocalFileSystemContext.getFileSystemDirectory(config),
        TokenSub.substitute(config, LocalFileSystemKey.FILE_SYSTEM_DIRECTORY.getDefaultString())
    );
  }

  @Test
  public void testOverrideConfig() throws Exception {
    String overrideDirectory = "/users/twitter";

    Config config = Config.toLocalMode(
        Config.newBuilder()
            .putAll(getDefaultConfig())
            .put(LocalFileSystemKey.FILE_SYSTEM_DIRECTORY.value(), overrideDirectory)
            .build());

    Assert.assertEquals(
        LocalFileSystemContext.getFileSystemDirectory(config),
        overrideDirectory
    );
  }

  @Test
  public void testTopologyDirectory() throws Exception {
    Config config = Config.toLocalMode(getDefaultConfig());
    LocalFileSystemUploader uploader = new LocalFileSystemUploader();
    uploader.initialize(config);

    Assert.assertEquals(
        uploader.getTopologyDirectory(),
        TokenSub.substitute(config, LocalFileSystemKey.FILE_SYSTEM_DIRECTORY.getDefaultString())
    );
  }

  @Test
  public void testTopologyFile() throws Exception {
    Config config = Config.toLocalMode(getDefaultConfig());
    LocalFileSystemUploader uploader = new LocalFileSystemUploader();
    uploader.initialize(config);

    Assert.assertEquals(
        Paths.get(uploader.getTopologyFile()).getParent().toString(),
        LocalFileSystemContext.getFileSystemDirectory(config)
    );
  }

  @Test
  public void testOverrideTopologyDirectory() throws Exception {
    LocalFileSystemUploader uploader = new LocalFileSystemUploader();
    String overrideDirectory = "/users/twitter";

    Config config = Config.toLocalMode(
        Config.newBuilder()
            .putAll(getDefaultConfig())
            .put(LocalFileSystemKey.FILE_SYSTEM_DIRECTORY.value(), overrideDirectory)
            .build());

    uploader.initialize(config);

    Assert.assertEquals(
        uploader.getTopologyDirectory(),
        overrideDirectory
    );
  }

  @Test
  public void testOverrideTopologyFile() throws Exception {
    LocalFileSystemUploader uploader = new LocalFileSystemUploader();
    String overrideDirectory = "/users/twitter";
    Config config = Config.toLocalMode(
        Config.newBuilder()
            .putAll(getDefaultConfig())
            .put(LocalFileSystemKey.FILE_SYSTEM_DIRECTORY.value(), overrideDirectory)
            .build());

    uploader.initialize(config);

    Assert.assertEquals(
        Paths.get(uploader.getTopologyFile()).getParent().toString(),
        overrideDirectory
    );
  }
}
