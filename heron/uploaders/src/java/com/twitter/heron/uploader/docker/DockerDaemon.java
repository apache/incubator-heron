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

import com.twitter.heron.spi.utils.ShellUtils;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Provides a wrapper around the DockerUploader command line client
 * This is not intended as a full DockerUploader API but rather an abstraction layer to
 * allow us to Test the uploader
 */
class DockerDaemon {

  private static final Logger LOG = Logger.getLogger(DockerDaemon.class.getName());

  boolean build(File workingDirectory, String tag) {
    String[] dockerCommand = {"docker", "build", "--no-cache=true", "--pull=true", "-t", tag,
        workingDirectory.getAbsolutePath()};
    StringBuilder stdLog = new StringBuilder();
    LOG.info("Creating DockerUploader Image with Tag " + tag);
    if (0 != ShellUtils.runProcess(true, dockerCommand, stdLog, stdLog)) {
      LOG.log(Level.SEVERE, stdLog.toString());
      return false;
    }
    return true;
  }

  boolean push(String tag) {
    String[] dockerPublish = {"docker", "push", tag};
    StringBuilder stdLog = new StringBuilder();
    LOG.info("pushing docker image with tag " + tag);
    if (0 != ShellUtils.runProcess(true, dockerPublish, stdLog, stdLog)) {
      LOG.log(Level.SEVERE, stdLog.toString());
      return false;
    }
    return true;
  }

}
