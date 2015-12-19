package com.twitter.heron.scheduler.service;

import java.nio.charset.Charset;
import java.util.logging.Logger;

import javax.xml.bind.DatatypeConverter;

import com.twitter.heron.scheduler.api.IConfigLoader;
import com.twitter.heron.scheduler.api.IRuntimeManager;
import com.twitter.heron.scheduler.api.context.RuntimeManagerContext;
import com.twitter.heron.scheduler.util.Factory;

public class RuntimeManagerMain {
  private static final Logger LOG = Logger.getLogger(RuntimeManagerMain.class.getName());

  public static void main(String[] args) throws Exception {
    IRuntimeManager.Command command = Factory.makeCommand(args[0]);
    String topologyName = args[1];
    String commandConfigLoader = args[2];
    String commandConfigOverrideEncoded = args[3];

    // TODO(mfu): move to common cli
    String commandConfigFile = "";
    if (args.length == 5) {
      commandConfigFile = args[4];
    }

    String configOverride = new String(
        DatatypeConverter.parseBase64Binary(commandConfigOverrideEncoded), Charset.forName("UTF-8"));

    if (!manageTopology(
        command, topologyName, commandConfigLoader, commandConfigFile, configOverride)) {
      LOG.severe(String.format("Failed to %s topology", command));
      Runtime.getRuntime().exit(1);
    }

    LOG.info(String.format("Topology %s successfully", command));
  }

  public static boolean manageTopology(IRuntimeManager.Command command,
                                       String topologyName,
                                       String commandConfigLoader,
                                       String commandConfigFile,
                                       String configOverride)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    // Make the config
    IConfigLoader commandConfig = Factory.makeConfigLoader(commandConfigLoader);

    LOG.info("Config to override in RuntimeManager: " + configOverride);
    if (!commandConfig.load(commandConfigFile, configOverride)) {
      throw new RuntimeException("Failed to load config. File: " + commandConfigFile
          + " Override: " + configOverride);
    }

    RuntimeManagerContext context = new RuntimeManagerContext(commandConfig, topologyName);
    context.start();

    IRuntimeManager runtimeManager = Factory.makeRuntimeManager(commandConfig.getRuntimeManagerClass());
    RuntimeManagerRunner runtimeManagerRunner = new RuntimeManagerRunner(command, runtimeManager, context);

    boolean ret = runtimeManagerRunner.call();
    context.close();
    return ret;
  }
}
