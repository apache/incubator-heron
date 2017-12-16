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
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.yaml.snakeyaml.Yaml;

import com.twitter.heron.common.basics.SysUtils;


public class Eco {

  private static final Logger LOG = Logger.getLogger(Eco.class.getName());

  public static void main(String[] args) throws Exception {
    Options options = constructOptions();

    CommandLineParser parser = new DefaultParser();



    CommandLine cmd;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      throw new RuntimeException("Error parsing command line options: ", e);
    }

    Eco eco = new Eco(cmd.getOptionValue("eco-config-file"));


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

  private String fileName;
  private Map<Object, Object> ecoProperties;

  @SuppressWarnings("unchecked")
  public Eco(String fileName) throws FileNotFoundException {
    FileInputStream fin = new FileInputStream(new File(fileName));
    try {
      this.fileName = fileName;
      Yaml yaml = new Yaml();
      Map<Object, Object> ecoProperties = (Map<Object, Object>) yaml.load(fin);
      if (ecoProperties == null) {
        throw new RuntimeException("Could not open eco config file");
      } else {
        LOG.info("YAML FILE: " + ecoProperties.toString());
        this.ecoProperties = ecoProperties;
      }
    } finally {
      SysUtils.closeIgnoringExceptions(fin);
    }

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
