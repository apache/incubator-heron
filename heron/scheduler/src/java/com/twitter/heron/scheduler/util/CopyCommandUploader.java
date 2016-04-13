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

package com.twitter.heron.scheduler.util;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.scheduler.context.LaunchContext;
import com.twitter.heron.spi.uploader.IUploader;

/**
 * A source-file parameterized topology copy utility in use with heron-cli2.
 * 
 * For example:
 * 1. Copy the generated topology.tar.gz to path /devops/jobs/
 *
 *   --config-property heron.uploader.copy.command='\"cp %s /devops/jobs/\"''
 *
 * 2. Copy the generated topology.tar.gz to AWS S3 s3://twitter.com/devops/jobs/
 *
 *   --config-property heron.uploader.copy.command='\"aws s3 cp %s s3://twitter.com/devops/jobs/\"'
 *
 * 3. Use curl to upload the generated topology.tar.gz to twitter artifactory
 *
 *   --config-property heron.uploader.copy.command='\"curl -0 -v -X PUT --data-binary @%s https://artifactory.twitter.biz/libs-releases-local/com/twitter/devops/jobs/\"'
 */
public class CopyCommandUploader implements IUploader {
  private volatile LaunchContext context;

  @Override
  public void initialize(LaunchContext context) {
    this.context = context;
  }

  private String getUploaderCommand() {
    return context.getPropertyWithException(Constants.HERON_UPLOADER_COPY_COMMAND);
  }

  @Override
  public boolean uploadPackage(String topologyPackage) {
    String fileName = Paths.get(topologyPackage).getFileName().toString();
    String copyCommand = String.format(getUploaderCommand(), topologyPackage);
    return 0 == ShellUtility.runProcess(context.isVerbose(), copyCommand, null, null);
  }

  @Override
  public void undo() {
    // do nothing
  }
}
