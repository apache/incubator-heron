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
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import com.google.common.base.Joiner;
import com.google.devtools.build.lib.actions.extra.ExtraActionInfo;
import com.google.devtools.build.lib.actions.extra.JavaCompileInfo;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.ArrayUtils;

/**
 * Verifies that the java classes styles conform to the styles in the config.
 * Usage: java com.twitter.bazel.checkstyle.JavaCheckstyle -f &lt;extra_action_file&gt; -c &lt;checkstyle_config&gt;
 * <p>
 * To test:
 * $ bazel build --config=darwin heron/spi/src/java:heron-spi --experimental_action_listener=tools/java:compile_java
 */
public final class JavaCheckstyle {
  public static final Logger LOG = Logger.getLogger(JavaCheckstyle.class.getName());
  private static final String CLASSNAME = JavaCheckstyle.class.getCanonicalName();

  private JavaCheckstyle() {
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
        .longOpt("checkstyle_config_file")
        .desc("checkstyle config file")
        .build());

    try {
      // parse the command line arguments
      CommandLine line = parser.parse(options, args);

      String extraActionFile = line.getOptionValue("f");
      String configFile = line.getOptionValue("c");

      String[] sourceFiles = getSourceFiles(extraActionFile);
      if (sourceFiles.length == 0) {
        LOG.fine("No java files found by checkstyle");
        return;
      }

      LOG.fine(sourceFiles.length + " java files found by checkstyle");

      String[] checkstyleArgs = (String[]) ArrayUtils.addAll(
          new String[]{"-c", configFile}, sourceFiles);

      LOG.fine("checkstyle args: " + Joiner.on(" ").join(checkstyleArgs));
      com.puppycrawl.tools.checkstyle.Main.main(checkstyleArgs);
    } catch (ParseException exp) {
      LOG.severe(String.format("Invalid input to %s: %s", CLASSNAME, exp.getMessage()));
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("java " + CLASSNAME, options);
    }
  }

  private static String[] getSourceFiles(String extraActionFile) {

    ExtraActionInfo info = ExtraActionUtils.getExtraActionInfo(extraActionFile);
    JavaCompileInfo jInfo = info.getExtension(JavaCompileInfo.javaCompileInfo);

    // Filter out files under heron/storm directory due to license issues
    List<String> sourceFiles = new LinkedList<>();
    for (String file : jInfo.getSourceFileList()) {
      if (!file.startsWith("heron/storm/src/java")) {
        sourceFiles.add(file);
      }
    }

    return sourceFiles.toArray(new String[sourceFiles.size()]);
  }
}
