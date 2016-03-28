package com.twitter.heron.command;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.util.concurrent.TimeUnit;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.Runtime;

import com.twitter.heron.proto.system.ExecutionEnvironment;

import com.twitter.heron.command.tableview.Board;
import com.twitter.heron.command.tableview.Block;
import com.twitter.heron.command.tableview.Table;

public class ListTopologiesHandler extends CommandHandler {
  private static final Logger LOG = Logger.getLogger(ListTopologiesHandler.class.getName());

  /**
   * Construct the command handler with static and runtime config
   */
  ListTopologiesHandler(Config config, Config runtime) {
    super(config, runtime);
  }

  /**
   * Execute any conditions before the command execution
   */
  public boolean beforeExecution() {
    return true;
  }

  /**
   * Execute any cleanup after the command execution
   */
  public boolean afterExecution() {
    return true;
  }

  /** 
   * Execute the command
   */
  public boolean execute() throws Exception {

    // get the state manager instance
    SchedulerStateManagerAdaptor stateManager = Runtime.schedulerStateManagerAdaptor(runtime);

    // get the execution states of all topologies
    List<ExecutionEnvironment.ExecutionState> executionStates;
    executionStates = stateManager.getAllExecutionStates().get(5, TimeUnit.SECONDS);

    // if no topologies are found, return right away
    if (executionStates.isEmpty()) {
      System.out.println("No topologies found"); 
      return true;
    }

    // print the header
    System.out.format("%-12.12s %-12.12s %-12.12s %-15.15s %-15.15s %s\n", 
        "CLUSTER", "ROLE", "ENVIRON", "USER", "TIME", "NAME");

    long now = System.currentTimeMillis()/1000;
    for (ExecutionEnvironment.ExecutionState es: executionStates) {

      // calculate the difference in time in seconds
      long delta = now - es.getSubmissionTime();

      // calculate (and subtract) whole days
      long days = (long)Math.floor(delta / 86400);
      delta -= days * 86400;

      // calculate (and subtract) whole hours
      long hours = ((long)Math.floor(delta / 3600)) % 24;
      delta -= hours * 3600;

      // calculate (and subtract) whole minutes
      long minutes = ((long)Math.floor(delta / 60)) % 60;
      delta -= minutes * 60;

      // what's left is seconds
      long seconds = delta % 60;  // in theory the modulus is not required

      String timeString = String.format("%03d:%02d:%02d:%02d", days, hours, minutes, seconds);      
 
      System.out.format("%-12.12s %-12.12s %-12.12s %-15.15s %-15.15s %s\n", 
          es.getCluster(), es.getRole(), es.getEnviron(), 
          es.getSubmissionUser(), timeString, es.getTopologyName());
    }

    return true;
  }
}
