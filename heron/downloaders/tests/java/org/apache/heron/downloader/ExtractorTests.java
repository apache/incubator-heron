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

package org.apache.heron.downloader;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.heron.common.basics.FileUtils;

import static org.junit.Assert.assertTrue;

public class ExtractorTests {

  private String tempDirectory;
  private String workingDirectory;

  @Before
  public void before() throws IOException {
    tempDirectory =
        Files.createTempDirectory("temp").toFile().getAbsolutePath();
    workingDirectory =
        Files.createTempDirectory("working").toFile().getAbsolutePath();
  }

  @Test
  public void testFlatExtract() throws Exception {
    final Path file1 = Paths.get(tempDirectory, "file1");
    final Path file2 = Paths.get(tempDirectory, "file2");

    Files.createFile(file1);
    Files.createFile(file2);

    assertTrue(Files.exists(file1));
    assertTrue(Files.exists(file2));

    // build tar.gz with file1 and file2
    final Path tar = Paths.get(workingDirectory, "topology.tar.gz");
    createTarGz(tar.toFile().getAbsolutePath(),
        tempDirectory);
    assertTrue(Files.exists(tar));

    Extractor.extract(new FileInputStream(tar.toFile()), Paths.get(workingDirectory));

    assertTrue(Files.exists(Paths.get(workingDirectory, "file1")));
    assertTrue(Files.exists(Paths.get(workingDirectory, "file2")));
  }

  @Test
  public void testNestedOneLevelExtract() throws Exception {
    final Path file1 = Paths.get(tempDirectory, "file");
    final Path nestedFile = Paths.get(tempDirectory, "dir", "file");

    Files.createFile(file1);
    nestedFile.getParent().toFile().mkdir();
    Files.createFile(nestedFile);

    // build tar.gz with file and dir/file
    final Path tar = Paths.get(workingDirectory, "topology.tar.gz");
    createTarGz(tar.toFile().getAbsolutePath(),
        tempDirectory);
    assertTrue(Files.exists(tar));

    Extractor.extract(new FileInputStream(tar.toFile()), Paths.get(workingDirectory));

    assertTrue(Files.exists(Paths.get(workingDirectory, "file")));
    assertTrue(Files.exists(Paths.get(workingDirectory, "dir", "file")));
  }

  @Test
  public void testNestedTwoLevelsExtract() throws Exception {
    final Path file1 = Paths.get(tempDirectory, "file1");
    final Path nestedFile = Paths.get(tempDirectory, "dir", "file");
    final Path nestedFile2 = Paths.get(tempDirectory, "dir", "dir2", "file");

    Files.createFile(file1);
    nestedFile.getParent().toFile().mkdir();
    Files.createFile(nestedFile);
    nestedFile2.getParent().toFile().mkdir();
    Files.createFile(nestedFile2);

    // build tar.gz with file1 and dir/file dir/dir2/file
    final Path tar = Paths.get(workingDirectory, "topology.tar.gz");
    createTarGz(tar.toFile().getAbsolutePath(),
        tempDirectory);
    assertTrue(Files.exists(tar));

    Extractor.extract(new FileInputStream(tar.toFile()), Paths.get(workingDirectory));

    assertTrue(Files.exists(Paths.get(workingDirectory, "file1")));
    assertTrue(Files.exists(Paths.get(workingDirectory, "dir", "file")));
    assertTrue(Files.exists(Paths.get(workingDirectory, "dir", "dir2", "file")));
  }

  @After
  public void after() {
    cleanAndDeleteDirectory(tempDirectory);
    cleanAndDeleteDirectory(workingDirectory);
  }

  private static void cleanAndDeleteDirectory(String directory) {
    if (directory != null) {
      FileUtils.cleanDir(directory);
      FileUtils.deleteDir(directory);
    }
  }

  private void createTarGz(String name, String directory) throws Exception {
    final String command = String.format("tar -C %s -czvf %s .", directory, name);
    final Process p = Runtime.getRuntime().exec(command);
    p.waitFor(10, TimeUnit.SECONDS);
  }
}
