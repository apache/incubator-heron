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

package org.apache.bazel.cppcheck;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Logger;

import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
import com.google.devtools.build.lib.actions.extra.CppCompileInfo;
import com.google.devtools.build.lib.actions.extra.ExtraActionInfo;

import org.apache.bazel.checkstyle.ExtraActionUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Runs cppcheck static analysis on cpp files
 */
public final class CppCheck {
  public static final Logger LOG = Logger.getLogger(CppCheck.class.getName());
  private static final String CLASSNAME = CppCheck.class.getCanonicalName();

  private CppCheck() {
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
    options.addOption(Option.builder("c")
            .required(true).hasArg()
            .longOpt("cppcheck_file")
            .desc("Executable cppcheck file to invoke")
            .build());

    try {
      // parse the command line arguments
      CommandLine line = parser.parse(options, args);

      String extraActionFile = line.getOptionValue("f");
      String cppcheckFile = line.getOptionValue("c");

      Collection<String> sourceFiles = getSourceFiles(extraActionFile);
      if (sourceFiles.size() == 0) {
        LOG.fine("No cpp files found by checkstyle");
        return;
      }

      LOG.fine(sourceFiles.size() + " cpp files found by checkstyle");

      // Create and run the command
      List<String> commandBuilder = new ArrayList<>();
      commandBuilder.add(cppcheckFile);
      commandBuilder.add("--std=c++11");
      commandBuilder.add("--language=c++");
      commandBuilder.add("--error-exitcode=1"); // exit with 1 on error
      // use googletest cfg so that TEST_F is not considered syntax error
      commandBuilder.add("--library=googletest");
      commandBuilder.addAll(sourceFiles);
      runChecker(commandBuilder);

    } catch (ParseException exp) {
      LOG.severe(String.format("Invalid input to %s: %s", CLASSNAME, exp.getMessage()));
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("java " + CLASSNAME, options);
    }
  }

  private static void runChecker(List<String> command) throws IOException {
    LOG.fine("checkstyle command: " + command);

    ProcessBuilder processBuilder = new ProcessBuilder(command);
    processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
    processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
    Process cppcheck = processBuilder.start();

    try {
      cppcheck.waitFor();
    } catch (InterruptedException e) {
      throw new RuntimeException("cppcheck command was interrupted: " + command, e);
    }

    if (cppcheck.exitValue() == 1) {
      LOG.warning("cppcheck detected bad cpp files.");
      throw new RuntimeException("cppcheck detected bad cpp files.");
    }

    if (cppcheck.exitValue() != 0) {
      throw new RuntimeException(
              "cppcheck command failed with status " + cppcheck.exitValue());
    }
  }

  @SuppressWarnings("unchecked")
  private static Collection<String> getSourceFiles(String extraActionFile) {

    ExtraActionInfo info = ExtraActionUtils.getExtraActionInfo(extraActionFile);
    CppCompileInfo cppInfo = info.getExtension(CppCompileInfo.cppCompileInfo);

    return Collections2.filter(
            cppInfo.getSourcesAndHeadersList(),
            Predicates.and(
              Predicates.not(Predicates.containsPattern("external/")),
              Predicates.not(Predicates.containsPattern("third_party/")),
              Predicates.not(Predicates.containsPattern("config/heron-config.h")),
              Predicates.not(Predicates.containsPattern(".*cppmap")),
              Predicates.not(Predicates.containsPattern(".*srcjar")),
              Predicates.not(Predicates.containsPattern(".*pb.h$")),
              Predicates.not(Predicates.containsPattern(".*cc_wrapper.sh$")),
              Predicates.not(Predicates.containsPattern(".*pb.cc$"))
            )
    );
  }
}
