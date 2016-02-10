package com.twitter.heron.scheduler.service;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Logger;

import javax.xml.bind.DatatypeConverter;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.scheduler.Scheduler;

import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.packing.IPackingAlgorithm;
import com.twitter.heron.spi.scheduler.IConfigLoader;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.scheduler.context.LaunchContext;
import com.twitter.heron.spi.util.Factory;

import com.twitter.heron.scheduler.service.server.SchedulerServer;
import com.twitter.heron.scheduler.util.TopologyUtility;

/**
 * Main class of scheduler.
 */
public class SchedulerMain {
  private static final Logger LOG = Logger.getLogger(SchedulerMain.class.getName());

  private static final ExecutorService service = Executors.newCachedThreadPool(new ThreadFactory() {
    private long count = 0;

    @Override
    public Thread newThread(Runnable r) {
      Thread t = new Thread(r, "scheduler-" + (++count));
      t.setDaemon(false);
      return t;
    }
  });

  /**
   * To be launched with topology name (args[0]), scheduler class (args[1]).
   * args[2] will be config loader class and args[3] will be config overrides.
   * and optionally scheduler config file (args[4]).
   */
  public static void main(String[] args) throws
      ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {
    String topologyName = args[0];
    String schedulerClass = args[1];
    String schedulerConfigLoader = args[2];
    String schedulerConfigOverrides = args[3].replace("&equals;", "=");

    int schedulerServerPort = Integer.parseInt(args[4]);

    // TODO(mfu): move to common cli
    String schedulerConfigFile = "";
    if (args.length == 6) {
      schedulerConfigFile = args[5];
    }

    String configOverride = new String(
        DatatypeConverter.parseBase64Binary(schedulerConfigOverrides), Charset.forName("UTF-8"));

    // The topology def file should locate in root of working directory
    String topologyDefnFile = TopologyUtility.lookUpTopologyDefnFile(".", topologyName);
    runScheduler(schedulerClass, schedulerConfigLoader, schedulerConfigFile,
        schedulerServerPort, configOverride, TopologyUtility.getTopology(topologyDefnFile));
  }

  private static HealthCheckRunner makeHealthCheckRunner(IScheduler scheduler) {
    return new HealthCheckRunner(scheduler);
  }

  public static void runScheduler(String schedulerClass,
                                  String schedulerConfigLoader,
                                  String schedulerConfigFile,
                                  int schedulerServerPort,
                                  String schedulerConfigOverrides,
                                  TopologyAPI.Topology topology) throws
      ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {
    final IConfigLoader schedulerConfig = Factory.makeConfigLoader(schedulerConfigLoader);

    LOG.info("Config to override in Scheduler: " + schedulerConfigOverrides);
    schedulerConfig.load(schedulerConfigFile, schedulerConfigOverrides);

    final LaunchContext context = new LaunchContext(schedulerConfig, topology);
    context.start();

    final IScheduler scheduler = Factory.makeScheduler(schedulerClass);

    // Start user generated routines.
    scheduler.initialize(context);
    // Start REST endpoint.
    SchedulerServer server = runServer(scheduler, context, schedulerServerPort);
    // Write the Scheduler location to state manager.
    setSchedulerLocation(context, server);

    // Make the packing plan
    IPackingAlgorithm packingAlgorithm =
        Factory.makePackingAlgorithm(schedulerConfig.getPackingAlgorithmClass());
    PackingPlan packing = packingAlgorithm.pack(context);

    scheduler.schedule(packing);

    service.execute(makeHealthCheckRunner(scheduler));  // Run health-check continuously.

    // We would close the context when we receive a Kill Request
  }

  public static SchedulerServer runServer(IScheduler scheduler, LaunchContext context, int port)
      throws IOException {
    final SchedulerServer schedulerServer = new SchedulerServer(scheduler, context, port, true);
    schedulerServer.start();

    return schedulerServer;
  }

  public static void setSchedulerLocation(LaunchContext context,
                                          SchedulerServer schedulerServer) {
    // Set scheduler location to host:port by default. Overwrite scheduler location if behind DNS.
    Scheduler.SchedulerLocation location = Scheduler.SchedulerLocation.newBuilder()
        .setTopologyName(context.getTopologyName())
        .setHttpEndpoint(String.format("%s:%d", schedulerServer.getHost(), schedulerServer.getPort()))
        .build();
    LOG.info("Setting SchedulerLocation: " + location);
    context.getStateManagerAdaptor().setSchedulerLocation(location);
  }
}
