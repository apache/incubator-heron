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

package com.twitter.heron.scheduler.service;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.logging.Logger;

import javax.xml.bind.DatatypeConverter;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.scheduler.util.TopologyUtility;
import com.twitter.heron.spi.packing.IPackingAlgorithm;
import com.twitter.heron.spi.scheduler.IConfigLoader;
import com.twitter.heron.spi.scheduler.ILauncher;
import com.twitter.heron.spi.scheduler.context.LaunchContext;
import com.twitter.heron.spi.uploader.IUploader;
import com.twitter.heron.spi.util.Factory;

/**
 * Calls Uploader to upload topology package, and Launcher to launch Scheduler.
 * TODO(nbhagat): Use commons cli to parse command line.
 * TODO(nbhagat): Make all argument passed to CLI available here.
 * args[0] = Topology Package location. Topology package generated from heron-cli will be a
 * tar file containing topology, topology definition and all dependencies.
 * args[1] = ConfigLoader class.
 * args[2] = Scheduler config file.
 * args[3] = Config override string encoded as base64 string.
 * args[4] = topology definition file.
 * args[5] = Heron internals config file location.
 * args[6] = Original package.
 */
public class SubmitterMain {
  private static final Logger LOG = Logger.getLogger(SubmitterMain.class.getName());
  private static IConfigLoader submitterConfig;
  private static String heronInternalsFile;
  private static String originalPackageFile;

  public static IConfigLoader getConfig() {
    return submitterConfig;
  }

  public static String getHeronInternalsConfigFile() {
    return heronInternalsFile;
  }

  public static String getOriginalPackageFile() {
    return originalPackageFile;
  }

  public static void main(String[] args) throws
      ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {

    String topologyPackage = args[0];
    String submitterConfigLoader = args[1];
    String submitterConfigFile = args[2];
    String submitterConfigOverrideEncoded = args[3];
    String topologyDefnFile = args[4];
    heronInternalsFile = args[5];
    originalPackageFile = args[6];
    String configOverride = new String(
        DatatypeConverter.parseBase64Binary(submitterConfigOverrideEncoded), Charset.forName("UTF-8"));
    if (!submitTopology(
        topologyPackage, submitterConfigLoader, submitterConfigFile, configOverride,
        TopologyUtility.getTopology(topologyDefnFile))) {
      Runtime.getRuntime().exit(1);
    }
  }

  public static boolean submitTopology(String topologyPackage,
                                       String submitterConfigLoader,
                                       String submitterConfigFile,
                                       String configOverride,
                                       TopologyAPI.Topology topology) throws
      ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {
    submitterConfig = Factory.makeConfigLoader(submitterConfigLoader);

    LOG.info("Config to override in Submitter: " + configOverride);
    if (!submitterConfig.load(submitterConfigFile, configOverride)) {
      throw new RuntimeException("Failed to load config. File: " + submitterConfigFile
          + " Override: " + configOverride);
    }

    LaunchContext context = new LaunchContext(submitterConfig, topology);
    context.start();

    IPackingAlgorithm packingAlgorithm = Factory.makePackingAlgorithm(submitterConfig.getPackingAlgorithmClass());

    // Uploader task.
    IUploader uploader = Factory.makeUploader(submitterConfig.getUploaderClass());

    UploadRunner uploadRunner = new UploadRunner(uploader, context, topologyPackage);
    boolean result = uploadRunner.call();

    if (!result) {
      LOG.severe("Failed to upload package. Exiting");
      return false;
    }

    // Launcher task.
    ILauncher launcher = Factory.makeLauncher(submitterConfig.getLauncherClass());

    LaunchRunner launchRunner = new LaunchRunner(launcher, context, packingAlgorithm);
    result = launchRunner.call();

    // Close it
    context.close();

    if (!result) {
      LOG.severe("Failed to launch topology. Attempting to roll back upload.");
      uploader.undo();
      return false;
    }
    return true;
  }
}
