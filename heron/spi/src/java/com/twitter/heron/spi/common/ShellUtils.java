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

package com.twitter.heron.spi.common;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handle shell process.
 */
public final class ShellUtils {

  private static final Logger LOG = Logger.getLogger(ShellUtils.class.getName());

  private ShellUtils() {
  }

  public static String inputstreamToString(InputStream is) {
    char[] buffer = new char[2048];
    StringBuilder builder = new StringBuilder();
    try (Reader reader = new InputStreamReader(is, "UTF-8")) {
      while (true) {
        int readSize = reader.read(buffer, 0, buffer.length);
        if (0 > readSize) {
          break;
        }
        builder.append(buffer, 0, readSize);
        buffer = new char[2048];
      }
    } catch (IOException ioe) {
      LOG.log(Level.SEVERE, "Failed to read stream ", ioe);
    }
    return builder.toString();
  }

  public static int runProcess(
      boolean verbose, String[] cmdline, StringBuilder stdout, StringBuilder stderr) {
    return runSyncProcess(verbose, false, cmdline, stdout, stderr, null);
  }

  public static int runProcess(
      boolean verbose, String cmdline, StringBuilder stdout, StringBuilder stderr) {
    return runSyncProcess(verbose, false, splitTokens(cmdline), stdout, stderr, null);
  }

  public static int runSyncProcess(
      boolean verbose, boolean isInheritIO, String cmdline, StringBuilder stdout,
      StringBuilder stderr, File workingDirectory) {
    return runSyncProcess(
        verbose, isInheritIO, splitTokens(cmdline), stdout, stderr, workingDirectory);
  }

  public static int runSyncProcess(
      boolean verbose, boolean isInheritIO, String[] cmdline, StringBuilder stdout,
      StringBuilder stderr, File workingDirectory) {
    return runSyncProcessWithEnvs(verbose, isInheritIO, cmdline, stdout, stderr, workingDirectory,
        new HashMap<String, String>());
  }

  public static int runSyncProcessWithEnvs(boolean verbose, boolean isInheritIO, String[] cmdline,
                                           StringBuilder stdout,
                                           StringBuilder stderr, File workingDirectory,
                                           Map<String, String> envs) {
    // TODO(nbhagat): Update stdout and stderr

    StringBuilder pStdOut = stdout;
    StringBuilder pStdErr = stderr;
    try {
      if (verbose) {
        LOG.info("$> " + Arrays.toString(cmdline));
      }
      Process process = getProcessBuilder(isInheritIO, cmdline, workingDirectory,
          envs).start();

      int exitValue = process.waitFor();
      if (pStdOut == null) {
        pStdOut = new StringBuilder();
      }
      if (pStdErr == null) {
        pStdErr = new StringBuilder();
      }
      pStdOut.append(inputstreamToString(process.getInputStream()));
      pStdErr.append(inputstreamToString(process.getErrorStream()));
      if (verbose) {
        LOG.info(pStdOut.toString());
        LOG.info(pStdErr.toString());
      }
      return exitValue;
    } catch (IOException | InterruptedException e) {
      LOG.severe("Failed to check status of packer " + e);
    }
    return -1;
  }

  public static Process runASyncProcess(
      boolean verbose, String command, File workingDirectory) {
    return runASyncProcess(verbose, splitTokens(command), workingDirectory);
  }

  public static Process runASyncProcess(
      boolean verbose, String[] command, File workingDirectory) {
    return runASyncProcessWithEnvs(verbose, command, workingDirectory,
        new HashMap<String, String>());
  }

  public static Process runASyncProcessWithEnvs(
      boolean verbose, String[] command, File workingDirectory, Map<String, String> envs) {
    if (verbose) {
      LOG.info("$> " + Arrays.toString(command));
    }

    // For AsyncProcess, we will never inherit IO, since parent process will not
    // be guaranteed alive when children processing trying to flush to
    // parent processes's IO.
    ProcessBuilder pb = getProcessBuilder(false, command, workingDirectory, envs);
    Process process = null;
    try {
      process = pb.start();
    } catch (IOException e) {
      LOG.severe("Failed to run Async Process " + e);
    }

    return process;
  }

  // TODO(nbhagat): Tokenize using DefaultConfig configOverride parser to handle case when
  // argument contains space.
  protected static String[] splitTokens(String command) {
    if (command.length() == 0) {
      throw new IllegalArgumentException("Empty command");
    }

    StringTokenizer st = new StringTokenizer(command);
    String[] cmdarray = new String[st.countTokens()];
    for (int i = 0; st.hasMoreTokens(); i++) {
      cmdarray[i] = st.nextToken();
    }
    return cmdarray;
  }

  protected static ProcessBuilder getProcessBuilder(
      boolean isInheritIO, String[] command, File workingDirectory, Map<String, String> envs) {
    ProcessBuilder pb = new ProcessBuilder(command)
        .directory(workingDirectory);
    if (isInheritIO) {
      pb.inheritIO();
    }

    Map<String, String> env = pb.environment();
    for (String envKey : envs.keySet()) {
      env.put(envKey, envs.get(envKey));
    }

    return pb;
  }

  public static Process establishSSHTunnelProcess(
      String tunnelHost, int tunnelPort, String destHost, int destPort, boolean verbose) {
    if (destHost == null
        || destHost.isEmpty()
        || "localhost".equals(destHost)
        || "127.0.0.1".equals(destHost)) {
      throw new RuntimeException("Trying to open tunnel to localhost.");
    }
    return ShellUtils.runASyncProcess(verbose,
        new String[]{
            "ssh", String.format("-NL%d:%s:%d", tunnelPort, destHost, destPort), tunnelHost},
        new File(".")
    );
  }

  /**
   * Copy a URL package to a target folder
   *
   * @param uri the URI to download core release package
   * @param destination the target filename to download the release package to
   * @param isVerbose display verbose output or not
   * @return true if successful
   */
  public static boolean curlPackage(
      String uri, String destination, boolean isVerbose, boolean isInheritIO) {

    // get the directory containing the target file
    File parentDirectory = Paths.get(destination).getParent().toFile();

    // using curl copy the url to the target file
    String cmd = String.format("curl %s -o %s", uri, destination);
    int ret = runSyncProcess(isVerbose, isInheritIO,
        cmd, new StringBuilder(), new StringBuilder(), parentDirectory);

    return ret == 0;
  }

  /**
   * Extract a tar package to a target folder
   *
   * @param packageName the tar package
   * @param targetFolder the target folder
   * @param isVerbose display verbose output or not
   * @return true if untar successfully
   */
  public static boolean extractPackage(
      String packageName, String targetFolder, boolean isVerbose, boolean isInheritIO) {
    String cmd = String.format("tar -xvf %s", packageName);

    int ret = runSyncProcess(isVerbose, isInheritIO,
        cmd, new StringBuilder(), new StringBuilder(), new File(targetFolder));

    return ret == 0;
  }

}
