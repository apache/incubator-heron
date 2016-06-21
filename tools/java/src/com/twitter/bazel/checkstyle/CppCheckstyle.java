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
import com.google.devtools.build.lib.actions.extra.CppCompileInfo;
import com.google.devtools.build.lib.actions.extra.ExtraActionInfo;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Verifies that the c, cpp and header source styles conform to the google styles in cpplint.
 * Usage: java com.twitter.bazel.checkstyle.CppCheckstyle -f &lt;extra_action_file&gt; -c &lt;cpplint_file&gt;
 * <p>
 * To test:
 * $ bazel build --config=darwin --experimental_action_listener=tools/cpp:compile_cpp heron/stmgr/src/cpp:grouping-cxx
 */
public final class CppCheckstyle {
  public static final Logger LOG = Logger.getLogger(CppCheckstyle.class.getName());
  private static final String CLASSNAME = CppCheckstyle.class.getCanonicalName();

  private CppCheckstyle() {
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
            .longOpt("cpplint_file")
            .desc("Executable cpplint file to invoke")
            .build());

    try {
      // parse the command line arguments
      CommandLine line = parser.parse(options, args);

      String extraActionFile = line.getOptionValue("f");
      String cpplintFile = line.getOptionValue("c");

      Collection<String> sourceFiles = getSourceFiles(extraActionFile);
      if (sourceFiles.size() == 0) {
        LOG.fine("No cpp files found by checkstyle");
        return;
      }

      LOG.fine(sourceFiles.size() + " cpp files found by checkstyle");

      // Create and run the command
      List<String> commandBuilder = new ArrayList<>();
      commandBuilder.add(cpplintFile);
      commandBuilder.add("--linelength=100");
      // TODO: https://github.com/twitter/heron/issues/466,
      // Remove "runtime/references" when we fix all non-const references in our codebase.
      // TODO: https://github.com/twitter/heron/issues/467,
      // Remove "runtime/threadsafe_fn" when we fix all non-threadsafe libc functions
      commandBuilder.add("--filter=-build/header_guard,-runtime/references,-runtime/threadsafe_fn");
      commandBuilder.addAll(sourceFiles);
      runLinter(commandBuilder);

    } catch (ParseException exp) {
      LOG.severe(String.format("Invalid input to %s: %s", CLASSNAME, exp.getMessage()));
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("java " + CLASSNAME, options);
    }
  }

  private static void runLinter(List<String> command) throws IOException {
    LOG.fine("checkstyle command: " + command);

    ProcessBuilder processBuilder = new ProcessBuilder(command);
    processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
    processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
    Process cpplint = processBuilder.start();

    try {
      cpplint.waitFor();
    } catch (InterruptedException e) {
      throw new RuntimeException("cpp checkstyle command was interrupted: " + command, e);
    }

    if (cpplint.exitValue() == 1) {
      LOG.warning("cpp checkstyle detected bad styles.");
      // SUPPRESS CHECKSTYLE RegexpSinglelineJava
      System.exit(1);
    }

    if (cpplint.exitValue() != 0) {
      throw new RuntimeException(
              "cpp checkstyle command failed with status " + cpplint.exitValue());
    }
  }

  @SuppressWarnings("unchecked")
  private static Collection<String> getSourceFiles(String extraActionFile) {

    ExtraActionInfo info = ExtraActionUtils.getExtraActionInfo(extraActionFile);
    CppCompileInfo cppInfo = info.getExtension(CppCompileInfo.cppCompileInfo);

    return Collections2.filter(
            cppInfo.getSourcesAndHeadersList(),
            Predicates.and(
                    Predicates.not(Predicates.containsPattern("third_party/")),
                    Predicates.not(Predicates.containsPattern("config/heron-config.h")),
                    Predicates.not(Predicates.containsPattern(".*pb.h$")),
                    Predicates.not(Predicates.containsPattern(".*cc_wrapper.sh$")),
                    Predicates.not(Predicates.containsPattern(".*pb.cc$"))
            )
    );
  }
}
