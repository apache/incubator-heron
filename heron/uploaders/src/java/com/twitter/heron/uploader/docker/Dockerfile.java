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
package com.twitter.heron.uploader.docker;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * A Simple API for programatically writing Dockerfiles
 * This is not intended as a fully speced out API that matches that provides
 * all the features of docker, rather it's intended as a way to separate our code from the FileSystem
 * to enable Unit Testing
 */
class Dockerfile {

  /**
   * Simple builder that roughly matches the bits of the DockerUploader Spec
   */
  static class DockerfileBuilder {

    private final File directory;
    private final List<String> commands = new ArrayList<>();

    private DockerfileBuilder(File directory) {
      this.directory = directory;
    }

    /**
     * DockerUploader FROM directive
     *
     * @param image the base image
     */
    DockerfileBuilder FROM(String image) {
      commands.add("FROM " + image);
      return this;
    }

    /**
     * DockerUploader ADD directive
     *
     * @param file     the source file
     * @param location the target location
     */
    DockerfileBuilder ADD(String file, String location) {
      commands.add("ADD " + file + "\t" + location);
      return this;
    }

    /**
     * Write the Dockerfile, if a Dockerfile already exists overwrite
     *
     * @throws IOException if unable to write or overwrite
     */
    void write() throws IOException {
      File file = new File(directory, "Dockerfile");
      if (file.exists()) {
        if (!file.delete()) {
          throw new IOException("Unable to delete existing Dockerfile");
        }
      }
      if (!file.createNewFile()) {
        throw new IOException("Unable to create Dockerfile");
      }
      try (FileOutputStream outputStream = new FileOutputStream(file)) {
        try (PrintWriter writer = new PrintWriter(outputStream)) {
          for (String command : commands) {
            writer.println(command);
          }
          writer.flush();
        }
        outputStream.flush();
      }
    }

  }

  /**
   * Create a new DockerfileBuilder for a docker file in the given directory
   * The file will be called Dockerfile
   *
   * @param directory the directory in which to create the docker file
   * @return a new DockerfileBuilder for the given directory
   */
  DockerfileBuilder newDockerfile(File directory) {
    return new DockerfileBuilder(directory);
  }

}
