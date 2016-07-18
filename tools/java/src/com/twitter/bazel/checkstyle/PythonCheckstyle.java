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
package com.twitter.bazel.checkstyle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Logger;

import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
import com.google.devtools.build.lib.actions.extra.ExtraActionInfo;
import com.google.devtools.build.lib.actions.extra.SpawnInfo;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Verifies that the python source styles conform to the python styles in pylint.
 * Usage: java com.twitter.bazel.checkstyle.PythonCheckstyle -f &lt;extra_action_file&gt; -p &lt;pylint_file&gt;
 * <p>
 * To test:
 * $ bazel build --config=darwin --experimental_action_listener=tools/python:compile_python heron/cli/src/python/...
 */
public final class PythonCheckstyle {
  public static final Logger LOG = Logger.getLogger(PythonCheckstyle.class.getName());
  private static final String CLASSNAME = PythonCheckstyle.class.getCanonicalName();
  private static final String PYLINT_RCFILE = "tools/python/checkstyle.ini";

  private PythonCheckstyle() {
  }

  public static void main(String[] args) throws IOException {
    CommandLineParser parser = new DefaultParser();

    // create the Options
    Options options = new Options();
    options.addOption(Option.builder("f")
            .required(true).hasArg()
            .longOpt("extra_action_file")
            .desc("bazel extra action protobuf file")
            .build());
    options.addOption(Option.builder("p")
            .required(true).hasArg()
            .longOpt("pylint_file")
            .desc("Executable pylint file to invoke")
            .build());

    try {
      // parse the command line arguments
      CommandLine line = parser.parse(options, args);

      String extraActionFile = line.getOptionValue("f");
      String pylintFile = line.getOptionValue("p");

      Collection<String> sourceFiles = getSourceFiles(extraActionFile);
      if (sourceFiles.size() == 0) {
        LOG.info("No python files found by checkstyle");
        return;
      }

      LOG.info(sourceFiles.size() + " python files found by checkstyle");

      // Create and run the command
      List<String> commandBuilder = new ArrayList<>();
      commandBuilder.add(pylintFile);
      commandBuilder.add("--rcfile=" + PYLINT_RCFILE);
      commandBuilder.add("--reports=n");
      commandBuilder.add("--disable=I");
      commandBuilder.addAll(sourceFiles);
      runLinter(commandBuilder);

    } catch (ParseException exp) {
      LOG.severe(String.format("Invalid input to %s: %s", CLASSNAME, exp.getMessage()));
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("java " + CLASSNAME, options);
    }
  }

  private static void runLinter(List<String> command) throws IOException {
    LOG.finer("checkstyle command: " + command);

    ProcessBuilder processBuilder = new ProcessBuilder(command);
    processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
    processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
    Process pylint = processBuilder.start();

    try {
      pylint.waitFor();
    } catch (InterruptedException e) {
      throw new RuntimeException("python checkstyle command was interrupted: " + command, e);
    }

    if (pylint.exitValue() == 30) {
      LOG.warning("python checkstyle detected bad styles.");
      // SUPPRESS CHECKSTYLE RegexpSinglelineJava
      System.exit(1);
    }

    if (pylint.exitValue() != 0) {
      throw new RuntimeException(
              "python checkstyle command failed with status " + pylint.exitValue());
    }
  }

  @SuppressWarnings("unchecked")
  private static Collection<String> getSourceFiles(String extraActionFile) {

    ExtraActionInfo info = ExtraActionUtils.getExtraActionInfo(extraActionFile);
    SpawnInfo spawnInfo = info.getExtension(SpawnInfo.spawnInfo);

    return Collections2.filter(spawnInfo.getInputFileList(),
        Predicates.and(
            Predicates.containsPattern(".*/src/.+\\.py[c]{0,1}$"),
            Predicates.not(Predicates.containsPattern("third_party/"))
        )
    );
  }
}
