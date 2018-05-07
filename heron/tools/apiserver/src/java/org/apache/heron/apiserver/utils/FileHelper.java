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

package org.apache.heron.apiserver.utils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;

public final class FileHelper {

  private static final Logger LOG = LoggerFactory.getLogger(FileHelper.class);

  public static boolean copy(InputStream in, Path to) {
    try {
      Files.copy(in, to, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException ioe) {
      LOG.error("Failed to copy file to {}", to, ioe);
      return false;
    }
    return true;
  }

  public static boolean copy(Path from, Path to) {
    try {
      Files.copy(from, to, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException ioe) {
      LOG.error("Failed to copy file from {} to {}", from, to, ioe);
      return false;
    }
    return true;
  }

  public static File[] getChildren(String path) {
    final File file = new File(path);
    return file.isDirectory() ? file.listFiles() : new File[] {};
  }

  public static boolean copyDirectory(Path from, Path to) {
    try {
      Files.walkFileTree(from, new CopyDirectoryVisitor(from, to));
    } catch (IOException ioe) {
      LOG.error("Failed to copy directory from {} to {}", from, to, ioe);
      return false;
    }
    return true;
  }

  public static boolean createTarGz(File archive, File... files) {
    try (
        FileOutputStream fileOutputStream = new FileOutputStream(archive);
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
        GzipCompressorOutputStream gzipOuputStream =
            new GzipCompressorOutputStream(bufferedOutputStream);
        TarArchiveOutputStream archiveOutputStream = new TarArchiveOutputStream(gzipOuputStream)
    ) {
      for (File file : files) {
        addFileToArchive(archiveOutputStream, file, "");
      }
      archiveOutputStream.finish();
    } catch (IOException ioe) {
      LOG.error("Failed to create archive {} file.", archive, ioe);
      return false;
    }
    return true;
  }

  private static void addFileToArchive(TarArchiveOutputStream archiveOutputStream, File file,
                                       String base) throws IOException {
    final File absoluteFile = file.getAbsoluteFile();
    final String entryName = base + file.getName();
    final TarArchiveEntry tarArchiveEntry = new TarArchiveEntry(file, entryName);
    archiveOutputStream.putArchiveEntry(tarArchiveEntry);

    if (absoluteFile.isFile()) {
      Files.copy(file.toPath(), archiveOutputStream);
      archiveOutputStream.closeArchiveEntry();
    } else {
      archiveOutputStream.closeArchiveEntry();
      if (absoluteFile.listFiles() != null) {
        for (File f : absoluteFile.listFiles()) {
          addFileToArchive(archiveOutputStream, f, entryName + "/");
        }
      }
    }
  }

  // save uploaded file to new location
  public static void writeToFile(InputStream uploadedInputStream,
                                 String uploadedFileLocation) throws IOException {
    File file = new File(uploadedFileLocation);
    file.getParentFile().mkdirs();

    int read = 0;
    byte[] bytes = new byte[1024];

    try (OutputStream out = new FileOutputStream(file)) {
      while ((read = uploadedInputStream.read(bytes)) != -1) {
        out.write(bytes, 0, read);
      }
      out.flush();
    }
  }

  private static final class CopyDirectoryVisitor extends SimpleFileVisitor<Path> {
    private final Path fromPath;
    private final Path toPath;
    private final StandardCopyOption copyOption = StandardCopyOption.REPLACE_EXISTING;

    private CopyDirectoryVisitor(Path from, Path to) {
      fromPath = from;
      toPath = to;
    }

    @Override
    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
        throws IOException {
      Path targetPath = toPath.resolve(fromPath.relativize(dir));
      if (!Files.exists(targetPath)) {
        Files.createDirectory(targetPath);
      }
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
      Files.copy(file, toPath.resolve(fromPath.relativize(file)), copyOption);
      return FileVisitResult.CONTINUE;
    }
  }

  private FileHelper() {
  }
}
