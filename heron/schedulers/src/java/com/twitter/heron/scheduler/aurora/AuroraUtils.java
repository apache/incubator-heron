package com.twitter.heron.scheduler.aurora;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.twitter.heron.spi.common.ShellUtils;

/**
 * This file defines Utils methods used by Aurora
 */
public class AuroraUtils {
  private static final Logger LOG = Logger.getLogger(AuroraUtils.class.getName());

  // Create an aurora job
  public static boolean createAuroraJob(String jobName, String cluster, String role, String env,
                                        String auroraFilename, Map<String, String> bindings,
                                        boolean isVerbose) {
    List<String> auroraCmd = Arrays.asList("aurora", "job", "create", "--wait-until", "RUNNING");

    for (Map.Entry<String, String> binding : bindings.entrySet()) {
      auroraCmd.add("--bind");
      auroraCmd.add(String.format("%s=%s", binding.getKey(), binding.getValue()));
    }

    String jobSpec = String.format("%s/%s/%s/%s", cluster, role, env, jobName);
    auroraCmd.add(jobSpec);
    auroraCmd.add(auroraFilename);

    if (isVerbose) {
      auroraCmd.add("--verbose");
    }

    return 0 == ShellUtils.runProcess(
        isVerbose, (String[]) auroraCmd.toArray(), new StringBuilder(), new StringBuilder());
  }

  // Kill an aurora job
  public static boolean killAuroraJob(String jobName, String cluster, String role, String env,
                                      boolean isVerbose) {
    List<String> auroraCmd = Arrays.asList("aurora", "job", "killall");
    String jobSpec = String.format("%s/%s/%s/%s", cluster, role, env, jobName);
    auroraCmd.add(jobSpec);

    appendAuroraCommandOptions(auroraCmd, isVerbose);

    return 0 == ShellUtils.runProcess(
        isVerbose, (String[]) auroraCmd.toArray(), new StringBuilder(), new StringBuilder());
  }

  // Restart an aurora job
  public static boolean restartAuroraJob(String jobName, String cluster, String role, String env,
                                         int containerId, boolean isVerbose) {
    List<String> auroraCmd = Arrays.asList("aurora", "job", "restart");
    String jobSpec = String.format("%s/%s/%s/%s", cluster, role, env, jobName);
    if (containerId != -1) {
      jobSpec = String.format("%s/%s", jobSpec, "" + containerId);
    }
    auroraCmd.add(jobSpec);

    appendAuroraCommandOptions(auroraCmd, isVerbose);

    return 0 == ShellUtils.runProcess(
        isVerbose, (String[]) auroraCmd.toArray(), new StringBuilder(), new StringBuilder());
  }

  // Static method to append verbose and batching options if needed
  public static void appendAuroraCommandOptions(List<String> auroraCmd,
                                                boolean isVerbose) {
    if (isVerbose) {
      auroraCmd.add("--verbose");
    }
    auroraCmd.add("--no-batching");

    return;
  }
}
