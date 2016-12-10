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

package com.twitter.heron.common.basics;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * FileUtils Tester.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(FileUtils.class)
public class FileUtilsTest {

  @Before
  public void before() throws Exception {
  }

  @After
  public void after() throws Exception {
  }

  /**
   * Method: deleteFile(String filename)
   */
  @Test
  public void testDeleteFile() throws Exception {
    PowerMockito.mockStatic(Files.class);

    Assert.assertTrue(FileUtils.deleteFile(""));
  }

  /**
   * Method: copyFile(String source, String target)
   */
  @Test
  public void testCopyFile() throws Exception {
    PowerMockito.mockStatic(Files.class);

    Assert.assertTrue(FileUtils.copyFile("", ""));
  }

  /**
   * Method: writeToFile(String filename, byte[] contents)
   */
  @Test
  public void testWriteToFile() throws Exception {
    String currentWorkingDir = Paths.get("").toAbsolutePath().normalize().toString();
    Assert.assertFalse(FileUtils.writeToFile(currentWorkingDir, new byte[]{}, false));

    PowerMockito.mockStatic(Files.class);
    String randomString = UUID.randomUUID().toString();
    Assert.assertTrue(FileUtils.writeToFile(randomString, null, false));

    // We can overwrite it
    Assert.assertTrue(FileUtils.writeToFile(randomString, null, true));
  }

  /**
   * Method: readFromFile(String filename)
   */
  @Test
  public void testReadFromFile() throws Exception {
    String toRead = "abc";
    PowerMockito.mockStatic(Files.class);
    PowerMockito.when(Files.readAllBytes(Matchers.any(Path.class))).thenReturn(toRead.getBytes());

    Assert.assertEquals(new String(FileUtils.readFromFile("")), toRead);
  }

  /**
   * Method: createDirectory(String directory)
   */
  @Test
  public void testCreateDirectory() throws Exception {
    String currentWorkingDir = Paths.get("").toAbsolutePath().normalize().toString();
    Assert.assertFalse(FileUtils.createDirectory(currentWorkingDir));

    String newDir = String.format("%s/%s", currentWorkingDir, UUID.randomUUID().toString());
    Assert.assertTrue(FileUtils.createDirectory(newDir));

    Assert.assertTrue(FileUtils.deleteFile(newDir));
  }

  /**
   * Method: isDirectoryExists(String directory)
   */
  @Test
  public void testIsDirectoryExists() throws Exception {
    PowerMockito.mockStatic(Files.class);
    PowerMockito.when(Files.isDirectory(Matchers.any(Path.class))).thenReturn(true);

    Assert.assertTrue(FileUtils.isDirectoryExists(""));

    PowerMockito.when(Files.isDirectory(Matchers.any(Path.class))).thenReturn(false);

    Assert.assertFalse(FileUtils.isDirectoryExists(""));
  }



  /**
   * Method: getBaseName(String file)
   */
  @Test
  public void testGetBaseName() throws Exception {
    String filename = "a/b";
    Assert.assertEquals("b", FileUtils.getBaseName(filename));

    filename = "b";
    Assert.assertEquals("b", FileUtils.getBaseName(filename));
  }
}
