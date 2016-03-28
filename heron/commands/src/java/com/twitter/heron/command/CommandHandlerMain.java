package com.twitter.heron.command;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.spi.common.ClusterConfig;
import com.twitter.heron.spi.common.ClusterDefaults;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;

import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.HelpFormatter;

public class CommandHandlerMain {
  private static final Logger LOG = Logger.getLogger(CommandHandlerMain.class.getName());

  public static void main(String[] args)
      throws ClassNotFoundException, IllegalAccessException,
      InstantiationException, IOException, ParseException {

    // parse the help options first.
    Options options = CommandHandlerOptions.constructOptions();
    Options helpOptions = CommandHandlerOptions.constructHelpOptions();
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(helpOptions, args, true);;

    if(cmd.hasOption("h")) {
      CommandHandlerOptions.usage(options);
      System.exit(0);
    }

    // Now parse the required options
    try {
      cmd = parser.parse(options, args);
    } catch(ParseException e) {
      LOG.severe("Error parsing command line options: " + e.getMessage());
      CommandHandlerOptions.usage(options);
      System.exit(1);
    }

    // get the command to be executed
    String command = cmd.getOptionValue("command");

    // load the static config
    Config config = CommandHandlerConfig.loadConfig(cmd);
    LOG.info("Static config loaded successfully ");
    LOG.info(config.toString());

    // create an instance of state manager
    String statemgrClass = Context.stateManagerClass(config);
    IStateManager statemgr = (IStateManager) Class.forName(statemgrClass).newInstance();

    try {
      // initialize the statemgr
      statemgr.initialize(config);

      // build the runtime config 
      Config runtime = Config.newBuilder()
          .put(Keys.schedulerStateManagerAdaptor(), new SchedulerStateManagerAdaptor(statemgr))
          .build();

      // instantiate the command handler
      CommandHandler commandHandler = CommandHandlerFactory.makeCommand(command, config, runtime);

      // execute preconditions for command execution
      commandHandler.beforeExecution();

      commandHandler.execute();

      // execute post conditions for command execution
      commandHandler.afterExecution();

    } catch (Exception e) { 
      e.printStackTrace();
      LOG.severe("Unable to execute command " + command);
      System.exit(1);

    } finally {
      // close the state manager
      statemgr.close();
    }

    System.exit(0);
  }
}
