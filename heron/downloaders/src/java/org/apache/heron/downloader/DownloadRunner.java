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

package org.apache.heron.downloader;

import java.io.File;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.heron.api.utils.Slf4jUtils;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.ConfigLoader;

public final class DownloadRunner {

  public enum DownloaderMode {
    cluster,
    local
  }

  private enum CliArgs {
    HERON_HOME("heron_home"),
    CONFIG_PATH("config_path"),
    MODE("mode"),
    TOPOLOGY_PACKAGE_URI("topology_package_uri"),
    EXTRACT_DESTINATION("extract_destination");

    private String text;

    CliArgs(String name) {
      this.text = name;
    }
  }

  // Print usage options
  private static void usage(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(DownloadRunner.class.getSimpleName(), options);
  }

  // construct command line help options
  private static Options constructHelpOptions() {
    Options options = new Options();
    Option help = Option.builder("h")
        .desc("List all options and their description")
        .longOpt("help")
        .build();

    options.addOption(help);
    return options;
  }

  // Construct all required command line options
  private static Options constructCliOptions() {
    Options options = new Options();

    Option packageUri = Option.builder("u")
        .desc("Uri indicating from where to download the file")
        .longOpt(CliArgs.TOPOLOGY_PACKAGE_URI.text)
        .hasArgs()
        .argName(CliArgs.TOPOLOGY_PACKAGE_URI.text)
        .build();

    Option destination = Option.builder("f")
        .desc("Destination to store the downloaded file")
        .longOpt(CliArgs.EXTRACT_DESTINATION.text)
        .hasArgs()
        .argName(CliArgs.EXTRACT_DESTINATION.text)
        .build();

    Option heronHome = Option.builder("d")
        .desc("Directory where heron is installed")
        .longOpt(CliArgs.HERON_HOME.text)
        .hasArgs()
        .argName("heron home dir")
        .build();

    Option configFile = Option.builder("p")
        .desc("Path of the config files")
        .longOpt(CliArgs.CONFIG_PATH.text)
        .hasArgs()
        .argName("config path")
        .build();

    // candidates:
    // local: download to client local machine
    // cluster: download into the container in the cloud
    Option mode = Option.builder("m")
        .desc("download mode, cluster or local")
        .longOpt(CliArgs.MODE.text)
        .hasArg()
        .argName("download mode")
        .build();

    options.addOption(packageUri);
    options.addOption(destination);
    options.addOption(heronHome);
    options.addOption(configFile);
    options.addOption(mode);

    return options;
  }


  // takes topology package URI and extracts it to a directory
  public static void main(String[] args) throws Exception {
    Slf4jUtils.installSLF4JBridge();
    CommandLineParser parser = new DefaultParser();
    Options slaManagerCliOptions = constructCliOptions();

    // parse the help options first.
    Options helpOptions = constructHelpOptions();
    CommandLine cmd = parser.parse(helpOptions, args, true);
    if (cmd.hasOption("h")) {
      usage(slaManagerCliOptions);
      return;
    }

    try {
      cmd = parser.parse(slaManagerCliOptions, args);
    } catch (ParseException e) {
      usage(slaManagerCliOptions);
      throw new RuntimeException("Error parsing command line options: ", e);
    }

    DownloaderMode mode = DownloaderMode.cluster;
    if (cmd.hasOption(CliArgs.MODE.text)) {
      mode = DownloaderMode.valueOf(cmd.getOptionValue(CliArgs.MODE.text, null));
    }

    Config config;
    switch (mode) {
      case cluster:
        config = Config.toClusterMode(Config.newBuilder()
            .putAll(ConfigLoader.loadClusterConfig())
            .build());
        break;

      case local:
        if (!cmd.hasOption(CliArgs.HERON_HOME.text) || !cmd.hasOption(CliArgs.CONFIG_PATH.text)) {
          throw new IllegalArgumentException("Missing heron_home or config_path argument");
        }
        String heronHome = cmd.getOptionValue(CliArgs.HERON_HOME.text, null);
        String configPath = cmd.getOptionValue(CliArgs.CONFIG_PATH.text, null);
        config = Config.toLocalMode(Config.newBuilder()
            .putAll(ConfigLoader.loadConfig(heronHome, configPath, null, null))
            .build());
        break;

      default:
        throw new IllegalArgumentException(
            "Invalid mode: " + cmd.getOptionValue(CliArgs.MODE.text));
    }

    String uri = cmd.getOptionValue(CliArgs.TOPOLOGY_PACKAGE_URI.text, null);
    String destination = cmd.getOptionValue(CliArgs.EXTRACT_DESTINATION.text, null);
    // make it compatible with old param format
    if (uri == null && destination == null) {
      String[] leftOverArgs = cmd.getArgs();
      if (leftOverArgs.length != 2) {
        System.err.println("Usage: downloader <topology-package-uri> <extract-destination>");
        return;
      }
      uri = leftOverArgs[0];
      destination = leftOverArgs[1];
    }

    final URI topologyLocation = new URI(uri);
    final Path topologyDestination = Paths.get(destination);

    final File file = topologyDestination.toFile();
    if (!file.exists()) {
      file.mkdirs();
    }

    Class clazz = Registry.UriToClass(config, topologyLocation);
    final Downloader downloader = Registry.getDownloader(clazz, topologyLocation);
    downloader.download(topologyLocation, topologyDestination);
  }

  private DownloadRunner() {
  }
}
