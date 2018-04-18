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

package org.apache.heron.spi.utils;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.fail;

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

  @Test
  public void testCopyToOutputStream() throws Exception {
    String fileContent = "temp file test content";
    String prefix = "myTestFile";
    String suffix = ".tmp";
    File tempFile = null;
    try {
      // create temp file
      tempFile = File.createTempFile(prefix, suffix);

      // write content to temp file
      writeContentToFile(tempFile.getAbsolutePath(), fileContent);

      // copy file content to output stream
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      UploaderUtils.copyToOutputStream(tempFile.getAbsolutePath(), out);
      Assert.assertEquals(fileContent, new String(out.toByteArray()));
    } finally {
      if (tempFile != null) {
        tempFile.deleteOnExit();
      }
    }
  }

  @Test(expected = FileNotFoundException.class)
  public void testCopyToOutputStreamWithInvalidFile() throws Exception {
    UploaderUtils.copyToOutputStream("invalid_file_name", new ByteArrayOutputStream());
  }

  private void writeContentToFile(String fileName, String content) {
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(fileName))) {
      bw.write(content);
    } catch (IOException e) {
      fail("Unexpected IOException has been thrown so unit test fails. Error message: "
          + e.getMessage());
    }
  }
}
