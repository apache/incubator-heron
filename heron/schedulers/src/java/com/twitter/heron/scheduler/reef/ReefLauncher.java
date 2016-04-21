package com.twitter.heron.scheduler.reef;

import com.twitter.heron.scheduler.reef.HeronMasterDriver.HeronExecutorLauncher;
import com.twitter.heron.scheduler.reef.HeronMasterDriver.HeronSchedulerLauncher;
import com.twitter.heron.scheduler.reef.HeronMasterDriver.HeronExecutorContainerBuilder;
import com.twitter.heron.scheduler.reef.HeronMasterDriver.HeronExecutorContainerErrorHandler;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.scheduler.ILauncher;
import org.apache.reef.client.ClientConfiguration;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.REEF;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.InjectionException;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Launches Heron Scheduler on a REEF on YARN cluster. The launcher will start a master (driver/AM) container for each
 * topology first. The master container will start worker containers subsequently. This launcher is responsible for
 * copying scheduler dependencies, topology package and heron-core libraries in file cache.
 */
@Unit
public class ReefLauncher implements ILauncher {
  private static final Logger LOG = Logger.getLogger(ReefLauncher.class.getName());
  private String topologyName;
  private String topologyPackageLocation;
  private String topologyJar;
  private String coreReleasePackage;
  private String cluster;
  private String role;
  private String env;
  private ArrayList<String> libJars = new ArrayList<>();

  @Override
  public void initialize(Config config, Config runtime) {
    topologyName = Context.topologyName(config);
    topologyJar = Context.topologyJarFile(config);
    topologyPackageLocation = Context.topologyPackageFile(config);
    cluster = Context.cluster(config);
    role = Context.role(config);
    env = Context.environ(config);

    try {
      Class<?> packingClass = Class.forName(Context.packingClass(config));
      Class<?> stateMgrClass = Class.forName(Context.stateManagerClass(config));

      // In addition to jar for REEF's driver implementation, jar for packing and state manager classes is also needed
      // on the server classpath. Upload these libraries in the global cache.
      libJars.add(HeronMasterDriver.class.getProtectionDomain().getCodeSource().getLocation().getFile());
      libJars.add(packingClass.getProtectionDomain().getCodeSource().getLocation().getFile());
      libJars.add(stateMgrClass.getProtectionDomain().getCodeSource().getLocation().getFile());

      coreReleasePackage = new URI(Context.corePackageUri(config)).getPath();
    } catch (URISyntaxException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    LOG.log(Level.INFO, "Initializing topology: {0}, core: {1}", new Object[]{topologyName, coreReleasePackage});
  }

  @Override
  public boolean launch(PackingPlan packing) {
    Configuration runtimeREEFConf = getRuntimeConf();
    Configuration reefDriverConf = getHMDriverConf();
    Configuration reefClientConf = getClientConf();

    try {
      final Injector tangInjector = Tang.Factory.getTang().newInjector(runtimeREEFConf, reefClientConf);
      final REEF reef = tangInjector.getInstance(REEF.class);
      final ReefClientSideHandlers client = tangInjector.getInstance(ReefClientSideHandlers.class);
      client.initialize(topologyName);

      reef.submit(reefDriverConf);
      client.waitForJobToStart();
    } catch (InjectionException | InterruptedException e) {
      LOG.log(Level.WARNING, "Failed to launch REEF app", e);
      return false;
    }

    return true;
  }

  /**
   * Creates configuration required by driver code to successfully construct heron's configuration and launch heron's
   * scheduler
   */
  private Configuration getHMDriverConf() {
    String topologyPackageName = new File(topologyPackageLocation).getName();
    String corePackageName = new File(coreReleasePackage).getName();
    return HeronDriverConfiguration.CONF.setMultiple(DriverConfiguration.GLOBAL_LIBRARIES, libJars)
            .set(DriverConfiguration.DRIVER_IDENTIFIER, topologyName)
            .set(DriverConfiguration.ON_DRIVER_STARTED, HeronSchedulerLauncher.class)
            .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, HeronExecutorContainerBuilder.class)
            .set(DriverConfiguration.ON_EVALUATOR_FAILED, HeronExecutorContainerErrorHandler.class)
            .set(DriverConfiguration.ON_CONTEXT_ACTIVE, HeronExecutorLauncher.class)
            .set(DriverConfiguration.GLOBAL_FILES, topologyPackageLocation)
            .set(DriverConfiguration.GLOBAL_FILES, coreReleasePackage)
            .set(HeronDriverConfiguration.TOPOLOGY_NAME, topologyName)
            .set(HeronDriverConfiguration.TOPOLOGY_JAR, topologyJar)
            .set(HeronDriverConfiguration.TOPOLOGY_PACKAGE_NAME, topologyPackageName)
            .set(HeronDriverConfiguration.HERON_CORE_PACKAGE_NAME, corePackageName)
            .set(HeronDriverConfiguration.ROLE, role)
            .set(HeronDriverConfiguration.ENV, env)
            .set(HeronDriverConfiguration.CLUSTER, cluster)
            .set(HeronDriverConfiguration.HTTP_PORT, 0)
            .build();
  }

  @Override
  public void close() {
    //TODO
  }

  /**
   * Specifies YARN as the runtime for REEF cluster. Override this method to use a different runtime.
   */
  Configuration getRuntimeConf() {
    return YarnClientConfiguration.CONF.build();
  }

  /**
   * @return Builds and returns configuration needed by REEF client to launch topology as a REEF job and track it.
   */
  private Configuration getClientConf() {
    return ClientConfiguration.CONF
            .set(ClientConfiguration.ON_JOB_RUNNING, ReefClientSideHandlers.RunningJobHandler.class)
            .set(ClientConfiguration.ON_JOB_FAILED, ReefClientSideHandlers.FailedJobHandler.class)
            .set(ClientConfiguration.ON_RUNTIME_ERROR, ReefClientSideHandlers.RuntimeErrorHandler.class)
            .build();
  }
}
