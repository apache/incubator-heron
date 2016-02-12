package com.twitter.heron.spi.utils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handle shell process.
 */
public class ShellUtility {

  private static final Logger LOG = Logger.getLogger(ShellUtility.class.getName());

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
    // TODO(nbhagat): Update stdout and stderr
    try {
      if (verbose) {
        LOG.info("$> " + Arrays.toString(cmdline));
      }
      Process process = getProcessBuilder(isInheritIO, cmdline, workingDirectory).start();

      int exitValue = process.waitFor();
      if (stdout == null) {
        stdout = new StringBuilder();
      }
      if (stderr == null) {
        stderr = new StringBuilder();
      }
      stdout.append(inputstreamToString(process.getInputStream()));
      stderr.append(inputstreamToString(process.getErrorStream()));
      if (verbose) {
        LOG.info(stdout.toString());
        LOG.info(stderr.toString());
      }
      return exitValue;
    } catch (IOException | InterruptedException e) {
      LOG.severe("Failed to check status of packer " + e);
    }
    return -1;
  }

  public static Process runASyncProcess(
      boolean verbose, boolean isInheritIO, String command, File workingDirectory) {
    return runASyncProcess(verbose, isInheritIO, splitTokens(command), workingDirectory);
  }

  public static Process runASyncProcess(
      boolean verbose, boolean isInheritIO, String[] command, File workingDirectory) {
    if (verbose) {
      LOG.info("$> " +  Arrays.toString(command));
    }

    ProcessBuilder pb = getProcessBuilder(isInheritIO, command, workingDirectory);
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
    if (command.length() == 0)
      throw new IllegalArgumentException("Empty command");

    StringTokenizer st = new StringTokenizer(command);
    String[] cmdarray = new String[st.countTokens()];
    for (int i = 0; st.hasMoreTokens(); i++) {
      cmdarray[i] = st.nextToken();
    }
    return cmdarray;
  }

  protected static ProcessBuilder getProcessBuilder(
      boolean isInheritIO, String[] command, File workingDirectory) {
    ProcessBuilder pb = new ProcessBuilder(command)
        .directory(workingDirectory);
    if (isInheritIO) {
      pb.inheritIO();
    }

    return pb;
  }

  public static Process setupTunnel(
      boolean verbose, String tunnelHost, int tunnelPort, String destHost, int destPort) {
    if (destHost == null
        || destHost.isEmpty()
        || destHost.equals("localhost")
        || destHost.equals("127.0.0.1")) {
      throw new RuntimeException("Trying to open tunnel to localhost.");
    }
    return ShellUtility.runASyncProcess(verbose, false,
        new String[] {
            "ssh", String.format("-NL%d:%s:%d", tunnelPort, destHost, destPort), tunnelHost},
        new File(".")
    );
  }
}
