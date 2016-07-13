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

package com.twitter.heron.spi.utils;

import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;


public class UploaderUtilsTest {

  @Test
  public void testGenerateFilename() throws Exception {
    int expectedUniqueFilename = 10000;
    String topologyName = "topologyName";
    String role = "role";
    String tag = "";
    int version = -1;
    Set<String> filenames = new HashSet<>();
    for (int i = 0; i < expectedUniqueFilename; i++) {
      filenames.add(UploaderUtils.generateFilename(
          topologyName, role, tag, version, ""));
    }

    // All filenames should be unique
    Assert.assertEquals(expectedUniqueFilename, filenames.size());
  }

  @Test
  public void testFilenameFormat() throws Exception {
    String topologyName = "topologyName";
    String role = "role";
    String filename = UploaderUtils.generateFilename(topologyName, role);

    Assert.assertTrue(filename.endsWith(UploaderUtils.DEFAULT_FILENAME_EXTENSION));

    String tag = "";
    int version = -1;
    String extension = ".extension";
    String customizedFilename =
        UploaderUtils.generateFilename(topologyName, role, tag, version, extension);
    Assert.assertTrue(customizedFilename.endsWith(extension));
  }
}
