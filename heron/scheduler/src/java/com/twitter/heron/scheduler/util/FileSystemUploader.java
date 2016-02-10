package com.twitter.heron.scheduler.util;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.scheduler.context.LaunchContext;
import com.twitter.heron.spi.uploader.IUploader;

public class FileSystemUploader implements IUploader {
  private volatile LaunchContext context;

  @Override
  public void initialize(LaunchContext context) {
    this.context = context;
  }

  private String getUploaderFileSystemPath() {
    return context.getPropertyWithException(Constants.HERON_UPLOADER_FILE_SYSTEM_PATH);
  }

  private String getUserTopologyFilePath() {
    String dc = context.getPropertyWithException(Constants.DC);
    String role = context.getPropertyWithException(Constants.ROLE);
    String environ = context.getPropertyWithException(Constants.ENVIRON);
    String toplogyName = context.getTopology().getName();
    return String.format("%s/%s/%s/%s/%s", getUploaderFileSystemPath(), dc, role, environ, toplogyName);
  }

  @Override
  public boolean uploadPackage(String topologyPackage) {
    String fileName = Paths.get(topologyPackage).getFileName().toString();
    Path filePath = Paths.get(getUserTopologyFilePath(), fileName);
    File parent = filePath.getParent().toFile();
    if (!parent.exists()) parent.mkdirs();

    String copyCmdline = String.format("cp %s %s", topologyPackage, filePath);
    return 0 == ShellUtility.runProcess(context.isVerbose(), copyCmdline, null, null);
  }

  @Override
  public void undo() {
    // do nothing
  }
}
