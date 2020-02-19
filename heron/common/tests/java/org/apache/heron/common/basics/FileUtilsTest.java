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

package org.apache.heron.common.basics;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Mockito.times;

/**
 * FileUtils Tester.
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("jdk.internal.reflect.*")
@PrepareForTest(FileUtils.class)
public class FileUtilsTest {

  /**
   * Method: deleteFile(String filename)
   */
  @Test
  public void testDeleteFile() {
    PowerMockito.mockStatic(Files.class);

    Assert.assertTrue(FileUtils.deleteFile(""));
  }

  /**
   * Method: copyFile(String source, String target)
   */
  @Test
  public void testCopyFile() {
    PowerMockito.mockStatic(Files.class);

    Assert.assertTrue(FileUtils.copyFile("", ""));
  }

  /**
   * Method: writeToFile(String filename, byte[] contents)
   */
  @Test
  public void testWriteToFile() {
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
  public void testReadFromFile() throws IOException {
    String toRead = "abc";
    PowerMockito.mockStatic(Files.class);
    PowerMockito.when(Files.readAllBytes(Matchers.any(Path.class))).thenReturn(toRead.getBytes());

    Assert.assertEquals(new String(FileUtils.readFromFile("")), toRead);
  }

  /**
   * Method: createDirectory(String directory)
   */
  @Test
  public void testCreateDirectory() {
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
  public void testIsDirectoryExists() {
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
  public void testGetBaseName() {
    String filename = "a/b";
    Assert.assertEquals("b", FileUtils.getBaseName(filename));

    filename = "b";
    Assert.assertEquals("b", FileUtils.getBaseName(filename));
  }

  /**
   * Method: deleteDir(File file)
   */
  @Test
  public void testDeleteDirWithFile() throws IOException {
    // Test delete a file
    Path file = Files.createTempFile("testDeleteFile", "txt");
    Assert.assertEquals(true, FileUtils.deleteDir(file.toFile(), true));
    Assert.assertFalse(file.toFile().exists());
  }

  /**
   * Method: deleteDir(File dir)
   */
  @Test
  public void testDeleteDirWithDirs() throws IOException {
    // Test delete dirs recursively,
    //  parent/ -- child1/ -- child3/
    //          |
    //          -- child2/
    Path parent = Files.createTempDirectory("testDeleteDir");
    Path child1 = Files.createTempDirectory(parent, "child1");
    Path child2 = Files.createTempDirectory(parent, "child2");
    Path child3 = Files.createTempDirectory(child1, "child3");

    PowerMockito.spy(FileUtils.class);

    FileUtils.deleteDir(parent.toFile(), true);

    PowerMockito.verifyStatic(times(4));

    Assert.assertFalse(parent.toFile().exists());
    Assert.assertFalse(child1.toFile().exists());
    Assert.assertFalse(child2.toFile().exists());
    Assert.assertFalse(child3.toFile().exists());
  }

  /**
   * Method: cleanDir(String dir)
   */
  @Test
  public void testCleanDir() throws IOException {
    // Test clean dirs,
    //  parent/ -- child1/ -- child3/
    //          |
    //          -- child2/
    Path parent = Files.createTempDirectory("testDeleteDir");
    Path child1 = Files.createTempDirectory(parent, "child1");
    Path child2 = Files.createTempDirectory(parent, "child2");
    Path child3 = Files.createTempDirectory(child1, "child3");

    PowerMockito.spy(FileUtils.class);

    FileUtils.cleanDir(parent.toString());

    Assert.assertTrue(parent.toFile().exists());
    Assert.assertFalse(child1.toFile().exists());
    Assert.assertFalse(child2.toFile().exists());
    Assert.assertFalse(child3.toFile().exists());
  }

}
