//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package com.twitter.heron.apiserver;

import java.io.IOException;
import java.net.BindException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.heron.apiserver.resources.HeronResource;
import com.twitter.heron.apiserver.utils.ConfigUtils;
import com.twitter.heron.apiserver.utils.Logging;
import com.twitter.heron.spi.common.Config;

public final class Runtime {

  private static final Logger LOG = LoggerFactory.getLogger(Runtime.class);

  private static final String API_BASE_PATH = "/api/v1/*";

  private enum Flag {
    Help("h"),
    BaseTemplate("base-template"),
    Cluster("cluster"),
    ConfigPath("config-path"),
    Port("port"),
    Property("D"),
    ReleaseFile("release-file"),
    Verbose("verbose");

    final String name;

    Flag(String name) {
      this.name = name;
    }
  }

  private static Options createOptions() {
    final Option cluster = Option.builder()
        .desc("Cluster in which to deploy topologies")
        .longOpt(Flag.Cluster.name)
        .hasArg()
        .argName(Flag.Cluster.name)
        .required()
        .build();

    final Option baseTemplate = Option.builder()
        .desc("Base configuration to use for deloying topologies")
        .longOpt(Flag.BaseTemplate.name)
        .hasArg()
        .argName(Flag.BaseTemplate.name)
        .required(false)
        .build();

    final Option config = Option.builder()
        .desc("Path to the base configuration for deploying topologies")
        .longOpt(Flag.ConfigPath.name)
        .hasArg()
        .argName(Flag.ConfigPath.name)
        .required(false)
        .build();

    final Option port = Option.builder()
        .desc("Port to bind to")
        .longOpt(Flag.Port.name)
        .hasArg()
        .argName(Flag.Port.name)
        .required(false)
        .build();

    final Option property = Option.builder(Flag.Property.name)
        .argName("property=value")
        .numberOfArgs(2)
        .valueSeparator()
        .desc("use value for given property")
        .build();

    final Option release = Option.builder()
        .desc("Path to the release file")
        .longOpt(Flag.ReleaseFile.name)
        .hasArg()
        .argName(Flag.ReleaseFile.name)
        .required(false)
        .build();

    final Option verbose = Option.builder()
        .desc("Verbose mode. Increases logging level to show debug messages.")
        .longOpt(Flag.Verbose.name)
        .hasArg(false)
        .argName(Flag.Verbose.name)
        .required(false)
        .build();

    return new Options()
        .addOption(baseTemplate)
        .addOption(cluster)
        .addOption(config)
        .addOption(port)
        .addOption(release)
        .addOption(property)
        .addOption(verbose);
  }

  private static Options constructHelpOptions() {
    Option help = Option.builder(Flag.Help.name)
        .desc("List all options and their description")
        .longOpt("help")
        .hasArg(false)
        .required(false)
        .build();

    return new Options()
        .addOption(help);
  }

  // Print usage options
  private static void usage(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("heron-apiserver", options);
  }

  private static String getConfigurationDirectory(String toolsHome, CommandLine cmd) {
    if (cmd.hasOption(Flag.ConfigPath.name)) {
      return cmd.getOptionValue(Flag.ConfigPath.name);
    } else if (cmd.hasOption(Flag.BaseTemplate.name)) {
      return Paths.get(toolsHome, Constants.DEFAULT_HERON_CONFIG_DIRECTORY,
          cmd.getOptionValue(Flag.BaseTemplate.name)).toFile().getAbsolutePath();
    }
    return Paths.get(toolsHome, Constants.DEFAULT_HERON_CONFIG_DIRECTORY,
        cmd.getOptionValue(Flag.Cluster.name)).toFile().getAbsolutePath();
  }

  private static String getHeronDirectory(CommandLine cmd) {
    final String cluster = cmd.getOptionValue(Flag.Cluster.name);
    return "local".equalsIgnoreCase(cluster)
        ? Constants.DEFAULT_HERON_LOCAL : Constants.DEFAULT_HERON_CLUSTER;
  }

  private static String getReleaseFile(String toolsHome, CommandLine cmd) {
    if (cmd.hasOption(Flag.ReleaseFile.name)) {
      return cmd.getOptionValue(Flag.ReleaseFile.name);
    }
    return Paths.get(toolsHome, Constants.DEFAULT_HERON_RELEASE_FILE)
        .toFile().getAbsolutePath();
  }

  private static int getPort(CommandLine cmd) {
    if (cmd.hasOption(Flag.Port.name)) {
      return Integer.valueOf(cmd.getOptionValue(Flag.Port.name));
    }

    return Constants.DEFAULT_PORT;
  }

  private static String loadOverrides(CommandLine cmd) throws IOException {
    return ConfigUtils.createOverrideConfiguration(
        cmd.getOptionProperties(Flag.Property.name));
  }

  private static String getToolsHome() throws URISyntaxException {
    final String jarLocation =
        Runtime.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
    return Paths.get(jarLocation).getParent().getParent().toFile().getAbsolutePath();
  }

  private static Boolean isVerbose(CommandLine cmd) {
    return cmd.hasOption(Flag.Verbose.name) ? Boolean.TRUE : Boolean.FALSE;
  }

  @SuppressWarnings({"IllegalCatch", "RegexpSinglelineJava"})
  public static void main(String[] args) throws Exception {
    final Options options = createOptions();
    final Options helpOptions = constructHelpOptions();

    CommandLineParser parser = new DefaultParser();

    // parse the help options first.
    CommandLine cmd = parser.parse(helpOptions, args, true);
    if (cmd.hasOption(Flag.Help.name)) {
      usage(options);
      return;
    }

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException pe) {
      System.err.println(pe.getMessage());
      usage(options);
      return;
    }

    final boolean verbose = isVerbose(cmd);
    // set and configure logging level
    Logging.setVerbose(verbose);
    Logging.configure(verbose);

    LOG.debug("apiserver overrides:\n {}", cmd.getOptionProperties(Flag.Property.name));

    final String toolsHome = getToolsHome();

    // read command line flags
    final String cluster = cmd.getOptionValue(Flag.Cluster.name);
    final String heronConfigurationDirectory = getConfigurationDirectory(toolsHome, cmd);
    final String heronDirectory = getHeronDirectory(cmd);
    final String releaseFile = getReleaseFile(toolsHome, cmd);
    final String configurationOverrides = loadOverrides(cmd);
    final int port = getPort(cmd);

    final Config baseConfiguration =
        ConfigUtils.getBaseConfiguration(heronDirectory,
            heronConfigurationDirectory,
            releaseFile,
            configurationOverrides);

    final ResourceConfig config = new ResourceConfig(Resources.get());
    final Server server = new Server(port);

    final ServletContextHandler contextHandler =
        new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    contextHandler.setContextPath("/");

    LOG.info("using configuration path: {}", heronConfigurationDirectory);

    contextHandler.setAttribute(HeronResource.ATTRIBUTE_CLUSTER, cluster);
    contextHandler.setAttribute(HeronResource.ATTRIBUTE_CONFIGURATION, baseConfiguration);
    contextHandler.setAttribute(HeronResource.ATTRIBUTE_CONFIGURATION_DIRECTORY,
        heronConfigurationDirectory);
    contextHandler.setAttribute(HeronResource.ATTRIBUTE_CONFIGURATION_OVERRIDE_PATH,
        configurationOverrides);

    server.setHandler(contextHandler);

    final ServletHolder apiServlet =
        new ServletHolder(new ServletContainer(config));

    contextHandler.addServlet(apiServlet, API_BASE_PATH);



    try {
      server.start();

      LOG.info("Heron apiserver started at {}", server.getURI());

      server.join();
    } catch (Exception ex) {
      final String message = getErrorMessage(server, port, ex);
      LOG.error(message);
      System.err.println(message);
      System.exit(1);
    } finally {
      server.destroy();
    }
  }

  private static String getErrorMessage(Server server, int port, Exception ex) {
    if (ex instanceof BindException) {
      final URI uri = server.getURI();
      return String.format("%s http://%s:%d", ex.getMessage(), uri.getHost(), port);
    }

    return ex.getMessage();
  }

  private Runtime() {
  }
}
