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

import java.nio.file.Paths;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.heron.common.basics.PackageType;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.common.Misc;

public class LocalFileSystemConfigTest {

  private Config getDefaultConfig() {
    return Config.newBuilder()
        .put(Keys.cluster(), "cluster")
        .put(Keys.role(), "role")
        .put(Keys.topologyName(), "topology")
        .put(Keys.topologyPackageType(), PackageType.TAR)
        .put(Keys.topologyPackageFile(), "/tmp/something.tar.gz")
        .put(LocalFileSystemKeys.fileSystemDirectory(),
            LocalFileSystemDefaults.fileSystemDirectory())
        .build();
  }

  @Test
  public void testDefaultConfig() throws Exception {
    Config config = Config.expand(getDefaultConfig());

    Assert.assertEquals(
        LocalFileSystemContext.fileSystemDirectory(config),
        Misc.substitute(config, LocalFileSystemDefaults.fileSystemDirectory())
    );
  }

  @Test
  public void testOverrideConfig() throws Exception {
    String overrideDirectory = "/users/twitter";

    Config config = Config.expand(
        Config.newBuilder()
            .putAll(getDefaultConfig())
            .put(LocalFileSystemKeys.fileSystemDirectory(), overrideDirectory)
            .build());

    Assert.assertEquals(
        LocalFileSystemContext.fileSystemDirectory(config),
        overrideDirectory
    );
  }

  @Test
  public void testTopologyDirectory() throws Exception {
    Config config = Config.expand(getDefaultConfig());
    LocalFileSystemUploader uploader = new LocalFileSystemUploader();
    uploader.initialize(config);

    Assert.assertEquals(
        uploader.getTopologyDirectory(),
        Misc.substitute(config, LocalFileSystemDefaults.fileSystemDirectory())
    );
  }

  @Test
  public void testTopologyFile() throws Exception {
    Config config = Config.expand(getDefaultConfig());
    LocalFileSystemUploader uploader = new LocalFileSystemUploader();
    uploader.initialize(config);

    Assert.assertEquals(
        Paths.get(uploader.getTopologyFile()).getParent().toString(),
        LocalFileSystemContext.fileSystemDirectory(config)
    );
  }

  @Test
  public void testOverrideTopologyDirectory() throws Exception {
    LocalFileSystemUploader uploader = new LocalFileSystemUploader();
    String overrideDirectory = "/users/twitter";

    Config config = Config.expand(
        Config.newBuilder()
            .putAll(getDefaultConfig())
            .put(LocalFileSystemKeys.fileSystemDirectory(), overrideDirectory)
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
    Config config = Config.expand(
        Config.newBuilder()
            .putAll(getDefaultConfig())
            .put(LocalFileSystemKeys.fileSystemDirectory(), overrideDirectory)
            .build());

    uploader.initialize(config);

    Assert.assertEquals(
        Paths.get(uploader.getTopologyFile()).getParent().toString(),
        overrideDirectory
    );
  }
}
