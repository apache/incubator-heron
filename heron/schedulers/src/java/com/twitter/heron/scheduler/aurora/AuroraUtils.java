package com.twitter.heron.scheduler.aurora;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.logging.Logger;

import com.twitter.heron.spi.common.ShellUtils;

public class AuroraUtils {
  private static final Logger LOG = Logger.getLogger(AuroraUtils.class.getName());

  public static boolean createAuroraJob(String jobName, String cluster, String role, String env,
                                        String auroraFilename, Map<String, String> bindings) {
    ArrayList<String> auroraCmd = new ArrayList<>(Arrays.asList(
        "aurora", "job", "create", "--verbose",
        "--wait-until", "RUNNING"));

    for (Map.Entry<String, String> binding : bindings.entrySet()) {
      auroraCmd.add("--bind");
      auroraCmd.add(String.format("%s=%s", binding.getKey(), binding.getValue()));
    }

    auroraCmd.add(String.format("%s/%s/%s/%s",
        cluster,
        role,
        env,
        jobName));
    auroraCmd.add(auroraFilename);

    String[] cmdline = auroraCmd.toArray(new String[auroraCmd.size()]);
    LOG.info("cmdline=" + Arrays.toString(cmdline));
    return 0 == ShellUtils.runProcess(true, cmdline, new StringBuilder(), new StringBuilder());
  }
}
