package com.twitter.heron.scheduler.util;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import com.twitter.heron.scheduler.api.Constants;
import com.twitter.heron.scheduler.api.IUploader;
import com.twitter.heron.scheduler.api.context.LaunchContext;

/**
 * A source-file parameterized shell copy utility.
 * 
 * For example:
 *   --config-property heron.uploader.copy.command="cp %s /devops/jobs/"
 * OR
 *   --config-property heron.uploader.copy.command="aws s3 cp %s s3://twitter.com/devops/jobs/"
 * OR
 *   --config-property ="curl -0 -v -X PUT --data-binary @%s https://artifactory.twitter.biz/libs-releases-local/com/twitter/devops/jobs/"
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
