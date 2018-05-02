/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.scheduler.yarn;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;

import org.apache.heron.scheduler.yarn.HeronMasterDriver.ContainerAllocationHandler;
import org.apache.heron.scheduler.yarn.HeronMasterDriver.FailedContainerHandler;
import org.apache.heron.scheduler.yarn.HeronMasterDriver.HeronSchedulerLauncher;
import org.apache.heron.scheduler.yarn.HeronMasterDriver.HeronWorkerLauncher;
import org.apache.heron.scheduler.yarn.HeronMasterDriver.HeronWorkerStartHandler;
import org.apache.heron.scheduler.yarn.HeronMasterDriver.HeronWorkerTaskCompletedErrorHandler;
import org.apache.heron.scheduler.yarn.HeronMasterDriver.HeronWorkerTaskFailureHandler;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.scheduler.ILauncher;
import org.apache.reef.client.ClientConfiguration;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.REEF;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.runtime.yarn.client.YarnDriverConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.InjectionException;

/**
 * Launches Heron Scheduler on a YARN using REEF. The launcher will start a master (driver/AM)
 * container for each topology first. The master container will start worker containers
 * subsequently. This launcher is responsible for copying scheduler dependencies, topology package
 * and heron-core libraries in file cache.
 */
@Unit
public class YarnLauncher implements ILauncher {
  private static final Logger LOG = Logger.getLogger(YarnLauncher.class.getName());
  private String topologyName;
  private String topologyPackageLocation;
  private String topologyJar;
  private String coreReleasePackage;
  private String cluster;
  private String role;
  private String env;
  private String queue;
  private int driverMemory;
  private ArrayList<String> libJars = new ArrayList<>();

  @Override
  public void initialize(Config config, Config runtime) {
    topologyName = Context.topologyName(config);
    topologyJar = Context.topologyBinaryFile(config);
    topologyPackageLocation = Context.topologyPackageFile(config);
    cluster = Context.cluster(config);
    role = Context.role(config);
    env = Context.environ(config);
    queue = YarnContext.heronYarnQueue(config);
    driverMemory = YarnContext.heronDriverMemoryMb(config);

    try {
      // In addition to jar for REEF's driver implementation, jar for packing and state manager
      // classes is also needed on the server classpath. Upload these libraries in the global cache.
      // TODO Although these jars will be available in heron-core/lib directory when core package
      // TODO is extracted. So copying these jars can be skipped if these jars can be put in
      // TODO classpath of SchedulerMain.
      addLibraryToClasspathSet(this.getClass().getName());
      addLibraryToClasspathSet(Context.packingClass(config));
      addLibraryToClasspathSet(Context.stateManagerClass(config));

      coreReleasePackage = new URI(Context.corePackageUri(config)).getPath();
    } catch (URISyntaxException | ClassNotFoundException e) {
      throw new RuntimeException("Core package URI is OR packing/state manager is missing", e);
    }

    LOG.log(Level.INFO,
        "Initializing topology: {0}, core: {1}",
        new Object[]{topologyName, coreReleasePackage});
  }

  @VisibleForTesting
  void addLibraryToClasspathSet(String className) throws ClassNotFoundException {
    Class<?> referenceClass = Class.forName(className);
    libJars.add(referenceClass.getProtectionDomain().getCodeSource().getLocation().getFile());
  }

  @Override
  public boolean launch(PackingPlan packing) {
    Configuration reefRuntimeConf = getRuntimeConf();
    Configuration reefDriverConf = getHMDriverConf();
    Configuration reefClientConf = getClientConf();

    boolean ret;
    try {
      final Injector injector = Tang.Factory.getTang().newInjector(reefRuntimeConf, reefClientConf);
      final REEF reef = injector.getInstance(REEF.class);
      final ReefClientSideHandlers client = injector.getInstance(ReefClientSideHandlers.class);

      reef.submit(reefDriverConf);

      ret = client.waitForSchedulerJobResponse();
    } catch (InjectionException | InterruptedException e) {
      LOG.log(Level.WARNING, "Failed to launch REEF app", e);
      return false;
    }

    return ret;
  }

  /**
   * Creates configuration required by driver code to successfully construct heron's configuration
   * and launch heron's scheduler
   */
  @VisibleForTesting
  Configuration getHMDriverConf() {
    String topologyPackageName = new File(topologyPackageLocation).getName();
    String corePackageName = new File(coreReleasePackage).getName();

    // topologyName and other configurations are required by Heron Driver/Scheduler to load
    // configuration files. Using REEF configuration model is better than depending on external
    // persistence.
    // TODO: https://github.com/apache/incubator-heron/issues/952: explore sharing across topologies
    return HeronDriverConfiguration.CONF
        .setMultiple(DriverConfiguration.GLOBAL_LIBRARIES, libJars)
        .set(DriverConfiguration.DRIVER_IDENTIFIER, topologyName)
        .set(DriverConfiguration.ON_DRIVER_STARTED, HeronSchedulerLauncher.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, ContainerAllocationHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_FAILED, FailedContainerHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, HeronWorkerLauncher.class)
        .set(DriverConfiguration.ON_TASK_RUNNING, HeronWorkerStartHandler.class)
        .set(DriverConfiguration.ON_TASK_COMPLETED, HeronWorkerTaskCompletedErrorHandler.class)
        .set(DriverConfiguration.ON_TASK_FAILED, HeronWorkerTaskFailureHandler.class)
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
        .set(HeronDriverConfiguration.VERBOSE, false)
        .set(YarnDriverConfiguration.QUEUE, queue)
        .set(DriverConfiguration.DRIVER_MEMORY, driverMemory)
        .build();
  }

  @Override
  public void close() {
    //TODO
  }

  /**
   * Specifies YARN as the runtime for REEF cluster. Override this method to use a different
   * runtime.
   */
  Configuration getRuntimeConf() {
    return YarnClientConfiguration.CONF.build();
  }

  /**
   * Builds and returns configuration needed by REEF client to launch topology as a REEF job and
   * track it.
   */
  private Configuration getClientConf() {
    return HeronClientConfiguration.CONF
        .set(ClientConfiguration.ON_JOB_RUNNING, ReefClientSideHandlers.RunningJobHandler.class)
        .set(ClientConfiguration.ON_JOB_FAILED, ReefClientSideHandlers.FailedJobHandler.class)
        .set(ClientConfiguration.ON_RUNTIME_ERROR, ReefClientSideHandlers.RuntimeErrorHandler.class)
        .set(HeronClientConfiguration.TOPOLOGY_NAME, topologyName)
        .build();
  }
}
