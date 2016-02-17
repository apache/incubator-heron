package com.twitter.heron.common.basics;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Utilities related to File.
 */
public class FileUtility {
  private static final Logger LOG = Logger.getLogger(FileUtility.class.getName());

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

  public static boolean writeToFile(String filename, byte[] contents) {
    File f = new File(filename);
    if (f.exists()) {
      LOG.severe("File exists. Topology exists: " + filename);
      return false;
    }

    try {
      Files.write(new File(filename).toPath(), contents);
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

  public static boolean isOriginalPackageJar(String packageFilename) {
    return packageFilename.endsWith(".jar");
  }

  public static String getBaseName(String file) {
    return new File(file).getName();
  }
}
