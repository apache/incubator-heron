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

package org.apache.bazel.checkstyle;

import java.io.IOException;
import java.util.Collection;
import java.util.logging.Logger;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
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
 * Usage: java org.apache.bazel.checkstyle.JavaCheckstyle -f &lt;extra_action_file&gt; -c &lt;checkstyle_config&gt;
 * <p>
 * To test:
 * $ bazel build heron/spi/src/java:heron-spi --experimental_action_listener=tools/java:compile_java
 */
public final class JavaCheckstyle {
  public static final Logger LOG = Logger.getLogger(JavaCheckstyle.class.getName());
  private static final String CLASSNAME = JavaCheckstyle.class.getCanonicalName();
  private static final Predicate APACHE_STYLE_FILES = Predicates.or(
      Predicates.containsPattern("storm-compatibility-examples.src.java"),
      Predicates.containsPattern("storm-compatibility.src.java"),
      Predicates.containsPattern("tools/test/LcovMerger"),
      Predicates.containsPattern("contrib")
  );
  private static final Predicate NO_CHECK_FILES = Predicates.or(
      Predicates.containsPattern("external") // from external/ directory for bazel
  );

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
    options.addOption(Option.builder("hc")
        .required(true).hasArg()
        .longOpt("heron_checkstyle_config_file")
        .desc("checkstyle config file")
        .build());
    options.addOption(Option.builder("ac")
        .required(true).hasArg()
        .longOpt("apache_checkstyle_config_file")
        .desc("checkstyle config file for imported source files")
        .build());

    try {
      // parse the command line arguments
      CommandLine line = parser.parse(options, args);

      String extraActionFile = line.getOptionValue("f");
      String configFile = line.getOptionValue("hc");
      String apacheConfigFile = line.getOptionValue("ac");

      // check heron source file style
      String[] heronSourceFiles = getHeronSourceFiles(extraActionFile);
      checkStyle(heronSourceFiles, configFile);

      // check other apache source file style
      String[] apacheSourceFiles = getApacheSourceFiles(extraActionFile);
      checkStyle(apacheSourceFiles, apacheConfigFile);

    } catch (ParseException exp) {
      LOG.severe(String.format("Invalid input to %s: %s", CLASSNAME, exp.getMessage()));
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("java " + CLASSNAME, options);
    }
  }

  private static void checkStyle(String[] files, String config) throws IOException {
    if (files.length == 0) {
      LOG.fine("No java files found by checkstyle");
      return;
    }

    LOG.fine(files.length + " java files found by checkstyle");

    String[] checkstyleArgs = (String[]) ArrayUtils.addAll(
        new String[]{"-c", config}, files);

    LOG.fine("checkstyle args: " + Joiner.on(" ").join(checkstyleArgs));
    com.puppycrawl.tools.checkstyle.Main.main(checkstyleArgs);
  }

  @SuppressWarnings("unchecked")
  private static String[] getHeronSourceFiles(String extraActionFile) {
    return getSourceFiles(extraActionFile, Predicates.not(
      Predicates.or(APACHE_STYLE_FILES, NO_CHECK_FILES)
    ));
  }

  @SuppressWarnings("unchecked")
  private static String[] getApacheSourceFiles(String extraActionFile) {
    return getSourceFiles(extraActionFile, APACHE_STYLE_FILES);
  }

  private static String[] getSourceFiles(String extraActionFile,
                                         Predicate<CharSequence> predicate) {
    ExtraActionInfo info = ExtraActionUtils.getExtraActionInfo(extraActionFile);
    JavaCompileInfo jInfo = info.getExtension(JavaCompileInfo.javaCompileInfo);

    Collection<String> sourceFiles = Collections2.filter(jInfo.getSourceFileList(), predicate);

    return sourceFiles.toArray(new String[sourceFiles.size()]);
  }
}
