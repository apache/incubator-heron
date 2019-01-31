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

import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Key;

public class LocalFileSystemContextTest {

  @Test
  public void testGetDefaultFileSystemDirectory() {
    Config config = Config.newBuilder()
        .put(Key.CLUSTER, "cluster")
        .put(Key.ROLE, "role")
        .put(Key.TOPOLOGY_NAME, "topology")
        .build();

    String actualFileSystemDirectory = LocalFileSystemContext.getFileSystemDirectory(config);

    // get default file system directory
    String defaultFileSystemDirectory = Paths.get("${HOME}",
        ".herondata", "repository", "${CLUSTER}", "${ROLE}", "${TOPOLOGY}").toString();

    Assert.assertEquals(defaultFileSystemDirectory, actualFileSystemDirectory);
  }

  @Test
  public void testGetFileSystemDirectory() {
    String fileSystemDirectory = "${HOME}/.herondata/topologies/${CLUSTER}";

    Config config = Config.newBuilder()
        .put(Key.CLUSTER, "cluster")
        .put(Key.ROLE, "role")
        .put(LocalFileSystemKey.FILE_SYSTEM_DIRECTORY.value(), fileSystemDirectory)
        .build();

    // get actual file system directory set by the user
    String actualFileSystemDirectory = LocalFileSystemContext.getFileSystemDirectory(config);

    String expectedFileSystemDirectory = Paths.get("${HOME}",
        ".herondata", "topologies", "${CLUSTER}").toString();

    Assert.assertEquals(expectedFileSystemDirectory, actualFileSystemDirectory);
  }

}
