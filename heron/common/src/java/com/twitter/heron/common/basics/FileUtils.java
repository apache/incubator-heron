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

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Utilities related to File.
 */
public final class FileUtils {
  private static final Logger LOG = Logger.getLogger(FileUtils.class.getName());

  private FileUtils() {
  }

  public static boolean deleteFile(String filename) {
    Path file = new File(filename).toPath();
    try {
      Files.delete(file);
    } catch (NoSuchFileException x) {
      LOG.severe("file does not exist: " + file);
      return false;
    } catch (DirectoryNotEmptyException x) {
      LOG.severe("Path is an not empty directory: " + file);
      return false;
    } catch (IOException x) {
      // File permission problems are caught here.
      LOG.log(Level.SEVERE, "Failed to delete file due to unexpected exception:", x);
      return false;
    }

    return true;
  }

  public static boolean copyFile(String source, String target) {
    Path sourcePath = new File(source).toPath();
    Path targetPath = new File(target).toPath();
    try {
      Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to copy file from " + source + " to target: " + target, e);
      return false;
    }

    return true;
  }

  public static boolean writeToFile(String filename, byte[] contents, boolean overwrite) {
    // default Files behavior is to overwrite. If we specify no overwrite then CREATE_NEW fails
    // if the file exist. This operation is atomic.
    OpenOption[] options = overwrite
        ? new OpenOption[]{} : new OpenOption[]{StandardOpenOption.CREATE_NEW};

    try {
      Files.write(new File(filename).toPath(), contents, options);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to write content to file. ", e);
      return false;
    }

    return true;
  }

  public static byte[] readFromFile(String filename) {
    Path path = new File(filename).toPath();
    byte[] res;
    try {
      res = Files.readAllBytes(path);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to read from file. ", e);
      res = new byte[0];
    }

    return res;
  }

  public static boolean createDirectory(String directory) {
    return new File(directory).mkdirs();
  }

  public static boolean isDirectoryExists(String directory) {
    return Files.isDirectory(new File(directory).toPath());
  }

  public static boolean isFileExists(String file) {
    return Files.exists(new File(file).toPath());
  }

  public static boolean hasChildren(String file) {
    return isDirectoryExists(file) && new File(file).list().length > 0;
  }

  public static String getBaseName(String file) {
    return new File(file).getName();
  }

}
