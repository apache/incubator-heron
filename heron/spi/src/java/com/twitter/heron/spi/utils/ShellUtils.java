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

package com.twitter.heron.spi.utils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;

/**
 * Handle shell process.
 */
public final class ShellUtils {

  private static final Logger LOG = Logger.getLogger(ShellUtils.class.getName());

  private ShellUtils() {
  }

  @VisibleForTesting
  static String inputstreamToString(InputStream is) {
    char[] buffer = new char[2048];
    StringBuilder builder = new StringBuilder();
    try (Reader reader = new InputStreamReader(is, StandardCharsets.UTF_8)) {
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

  public static int runProcess(String[] cmdline, StringBuilder stdout, StringBuilder stderr) {
    return runSyncProcess(false, false, cmdline, stdout, stderr, null);
  }

  public static int runProcess(
      boolean verbose, String cmdline, StringBuilder stdout, StringBuilder stderr) {
    return runSyncProcess(verbose, false, splitTokens(cmdline), stdout, stderr, null);
  }

  public static int runSyncProcess(
      boolean verbose, boolean isInheritIO, String[] cmdline, StringBuilder stdout,
      StringBuilder stderr, File workingDirectory) {
    return runSyncProcess(isInheritIO, cmdline, stdout, stderr, workingDirectory,
        new HashMap<String, String>());
  }

  /**
   * Start a daemon thread to read data from "input" to "out".
   */
  private static Thread asyncProcessStream(final InputStream input, final StringBuilder out) {
    Thread thread = new Thread() {
      @Override
      public void run() {
        try {
          out.append(inputstreamToString(input));
        } finally {
          try {
            input.close();
          } catch (IOException e) {
            LOG.log(Level.WARNING, "Failed to close the input stream", e);
          }
        }
      }
    };
    thread.setDaemon(true);
    thread.start();
    return thread;
  }

  /**
   * run sync process
   */
  private static int runSyncProcess(
      boolean isInheritIO, String[] cmdline, StringBuilder stdout,
      StringBuilder stderr, File workingDirectory, Map<String, String> envs) {
    final StringBuilder pStdOut = stdout == null ? new StringBuilder() : stdout;
    final StringBuilder pStdErr = stderr == null ? new StringBuilder() : stderr;

    // Log the command for debugging
    LOG.log(Level.FINE, "Process command: `$ {0}`", Arrays.toString(cmdline));
    ProcessBuilder pb = getProcessBuilder(isInheritIO, cmdline, workingDirectory, envs);

    Process process;
    try {
      process = pb.start();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to run Sync Process ", e);
      return -1;
    }

    // Launching threads to consume stdout and stderr before "waitFor". Otherwise, output from the
    // "process" can exhaust the available buffer for the output or error stream because neither
    // stream is read while waiting for the process to complete. If either buffer becomes full, it
    // can block the "process" as well, preventing all progress for both the "process" and the
    // current thread.
    Thread stdoutThread = asyncProcessStream(process.getInputStream(), pStdOut);
    Thread stderrThread = asyncProcessStream(process.getErrorStream(), pStdErr);

    int exitValue;

    try {
      exitValue = process.waitFor();
      // Make sure `pStdOut` and `pStdErr` get the buffered data
      stdoutThread.join();
      stderrThread.join();
    } catch (InterruptedException e) {
      // The current thread is interrupted, so try to interrupt reading threads and kill
      // the process to return quickly.
      stdoutThread.interrupt();
      stderrThread.interrupt();
      process.destroy();
      LOG.log(Level.SEVERE, "Running Sync Process was interrupted", e);
      // Reset the interrupt status to allow other codes noticing it.
      Thread.currentThread().interrupt();
      return -1;
    }

    String stdoutString = pStdOut.toString();
    String stderrString = pStdErr.toString();
    if (!stdoutString.isEmpty()) {
      LOG.log(Level.FINE, "\nSTDOUT:\n {0}", stdoutString);
    }

    if (!stderrString.isEmpty()) {
      LOG.log(Level.FINE, "\nSTDERR:\n {0}", stderrString);
    }

    return exitValue;
  }

  public static Process runASyncProcess(
      String[] command, File workingDirectory, String logFileUuid) {
    return runASyncProcess(
        command, workingDirectory, new HashMap<String, String>(), logFileUuid, true);
  }

  public static Process runASyncProcess(
      boolean verbose, String[] command, File workingDirectory) {
    return runASyncProcess(command, workingDirectory, new HashMap<String, String>(), null, true);
  }

  public static Process runASyncProcess(
      boolean verbose, String[] command, File workingDirectory, Map<String, String> envs) {
    return runASyncProcess(command, workingDirectory, envs, null, true);
  }

  public static Process runASyncProcess(String command) {
    return runASyncProcess(splitTokens(command), new File("."),
        new HashMap<String, String>(), null, false);
  }

  private static Process runASyncProcess(String[] command, File workingDirectory,
      Map<String, String> envs, String logFileUuid, boolean logStderr) {
    LOG.log(Level.FINE, "$> {0}", Arrays.toString(command));

    // the log file can help people to find out what happened between pb.start()
    // and the async process started
    String commandFileName = Paths.get(command[0]).getFileName().toString();
    String uuid = logFileUuid;
    if (uuid == null) {
      uuid = UUID.randomUUID().toString().substring(0, 8) + "-started";
    }

    // For AsyncProcess, we will never inherit IO, since parent process will not
    // be guaranteed alive when children processing trying to flush to
    // parent processes's IO.
    ProcessBuilder pb = getProcessBuilder(false, command, workingDirectory, envs);
    pb.redirectErrorStream(true);

    if (logStderr) {
      String logFilePath = String.format("%s/%s-%s.stderr",
          workingDirectory, commandFileName, uuid);
      pb.redirectOutput(ProcessBuilder.Redirect.appendTo(new File(logFilePath)));
    }

    Process process = null;
    try {
      process = pb.start();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to run Async Process ", e);
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

  static Process establishSSHTunnelProcess(
      String tunnelHost, int tunnelPort, String destHost, int destPort) {
    if (destHost == null
        || destHost.isEmpty()
        || "localhost".equals(destHost)
        || "127.0.0.1".equals(destHost)) {
      throw new RuntimeException("Trying to open tunnel to localhost.");
    }
    return ShellUtils.runASyncProcess(
        String.format("ssh -NL%d:%s:%d %s", tunnelPort, destHost, destPort, tunnelHost));
  }

  static Process establishSocksProxyProcess(String proxyHost, int proxyPort) {
    return ShellUtils.runASyncProcess(String.format("ssh -ND %d %s", proxyPort, proxyHost));
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
        splitTokens(cmd), new StringBuilder(), new StringBuilder(), parentDirectory);

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
        splitTokens(cmd), new StringBuilder(), new StringBuilder(), new File(targetFolder));

    return ret == 0;
  }

}
