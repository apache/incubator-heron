package com.twitter.heron.uploaders.localfs;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.ShellUtility;
import com.twitter.heron.spi.newuploader.IUploader;

public class LocalFileSystemUploader implements IUploader {
  protected String cluster;
  protected String role;
  protected String environ;
  protected String topologyName;
  protected boolean verbose;

  protected String fileSystemPath;

  @Override
  public void initialize(Context context) {
    this.cluster = context.getStringValue(Keys.Config.CLUSTER);
    this.role = context.getStringValue(Keys.Config.ROLE);
    this.environ = context.getStringValue(Keys.Config.ENVIRON);
    this.verbose = context.getBooleanValue(Keys.Config.VERBOSE);
    this.topologyName = context.getStringValue(Keys.Config.TOPOLOGY_NAME);
    this.fileSystemPath = context.getStringValue(LocalFileSystemKeys.Config.FILE_SYSTEM_PATH);
  }

  private String getUploaderFileSystemPath() {
    return this.fileSystemPath;
  }

  private String getUserTopologyFilePath() {
    return String.format("%s/%s/%s/%s/%s", getUploaderFileSystemPath(), cluster, role, environ, topologyName);
  }

  @Override
  public boolean uploadPackage(String topologyPackage) {
    String fileName = Paths.get(topologyPackage).getFileName().toString();
    Path filePath = Paths.get(getUserTopologyFilePath(), fileName);
    File parent = filePath.getParent().toFile();
    if (!parent.exists()) parent.mkdirs();

    String copyCmdline = String.format("cp %s %s", topologyPackage, filePath);
    return 0 == ShellUtility.runProcess(verbose, copyCmdline, null, null);
  }

  @Override
  public Context getContext() {
    Context.Builder builder = new Context.Builder();
    return builder.build(); 
  }

  @Override
  public boolean undo() {
    return true;
  }

  @Override
  public void cleanup() {
  }
}
