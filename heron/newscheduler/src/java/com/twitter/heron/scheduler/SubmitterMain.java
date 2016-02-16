package com.twitter.heron.scheduler;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.logging.Logger;

import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.IPacking;
import com.twitter.heron.spi.uploader.IUploader;

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

  public static void main(String[] args) throws
      ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {
    String cluster = args[0];
    String role = args[1];
    String environ = args[2];
    String configPath = args[3];
    String configOverrideEncoded = args[4];

    String topologyPackage = args[5];
    String topologyDefnFile = args[6];
    String heronInternalsFile = args[7];
    String originalPackageFile = args[8];
    String pkg_type = FileUtils.isOriginalPackageJar(
        FileUtility.getBaseName(originalPackageFile)) ? "jar" : "tar" ; 

    // First load the defaults, then the config from files to override it 
    Context.Builder cb1 = Context.newBuilder()
       .putAll(ClusterDefaults.getDefaults())
       .putAll(ClusterConfig.loadConfig(cluster, configPath));
 
    // Add config parameters from the command line
    Context.Builder cb2 = Context.newBuilder()
       .put(Keys.Config.CLUSTER, cluster)
       .put(Keys.Config.ROLE, role)
       .put(Keys.Config.ENVIRON, environ)

    Context.Builder cb3 = Context.newBuilder()
       .put(Keys.Config.INTERNALS_CONFIG_FILE, heronInternalsFile)
       .put(Keys.Config.TOPOLOGY_DEFINITION_FILE, topologyDefnFile)
       .put(Keys.Config.TOPOLOGY_JAR_FILE, originalPackageFile)
       .put(Keys.Config.TOPOLOGY_PKG_TYPE, pkg_type)

    // TODO - Karthik override any parameters from the command line

    System.out.println(cb.build()); 
  }
}
