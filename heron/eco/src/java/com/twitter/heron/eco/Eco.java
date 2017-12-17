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
package com.twitter.heron.eco;

import java.io.File;
import java.io.FileInputStream;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.twitter.heron.eco.definition.EcoTopologyDefinition;
import com.twitter.heron.eco.helper.EcoParser;


public class Eco {

  private static final Logger LOG = Logger.getLogger(Eco.class.getName());

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    Options options = constructOptions();

    CommandLineParser parser = new DefaultParser();

    CommandLine cmd;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      throw new RuntimeException("Error parsing command line options: ", e);
    }

    FileInputStream fin = new FileInputStream(new File(cmd.getOptionValue("eco-config-file")));

    EcoTopologyDefinition topologyDef = EcoParser.parseFromInputStream(fin);

    LOG.info("Printing topology config to String: " + topologyDef.toString());

  }

  private static Options constructOptions() {
    Options options = new Options();
    Option ecoConfig = Option.builder("eco")
        .desc("Yaml config file for specifying topology definitions")
        .longOpt("eco-config-file")
        .hasArgs()
        .argName("eco-config-file")
        .required()
        .build();
    options.addOption(ecoConfig);
    return options;
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
}
