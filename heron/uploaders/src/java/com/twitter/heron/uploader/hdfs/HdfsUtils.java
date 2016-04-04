package com.twitter.heron.uploader.hdfs;

import com.twitter.heron.spi.common.ShellUtils;

public class HdfsUtils {
  private HdfsUtils() {

  }

  public static boolean isFileExists(String configDir, String fileURI, boolean isVerbose) {
    String command = String.format("hadoop --config %s fs -test -e %s", configDir, fileURI);
    return (0 == ShellUtils.runProcess(isVerbose, command, null, null));
  }

  public static boolean createDir(String configDir, String dir, boolean isVerbose) {
    String command = String.format("hadoop --config %s fs -mkdir -p %s", configDir, dir);
    return (0 == ShellUtils.runProcess(isVerbose, command, null, null));
  }

  public static boolean copyFromLocal(
      String configDir, String source, String target, boolean isVerbose) {
    String command = String.format("hadoop --config %s fs -copyFromLocal -f %s %s",
        configDir, source, target);
    return (0 == ShellUtils.runProcess(isVerbose, command, null, null));
  }

  public static boolean remove(String configDir, String fileURI, boolean isVerbose) {
    String command = String.format("hadoop --config %s fs -rm %s", configDir, fileURI);
    return (0 == ShellUtils.runProcess(isVerbose, command, null, null));
  }
}
