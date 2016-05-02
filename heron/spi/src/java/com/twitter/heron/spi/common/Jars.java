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

package com.twitter.heron.spi.common;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Jars {
  private static final Logger LOG = Logger.getLogger(Jars.class.getName());

  // scheduler jar search pattern
  private static Pattern schedulerJarPattern =
      Pattern.compile("heron-.*-scheduler.*.jar|heron-scheduler*.jar");

  // metrics manager search pattern
  private static Pattern metricsManagerPattern =
      Pattern.compile("heron-.*-metricsmgr.*.jar|heron-metricsmgr*.jar");

  private Jars() {
  }

  /**
   * Given a directory, get the list of jars matching the pattern.
   *
   * @param pattern the pattern to match for jar files
   * @param directory the directory to search
   * @return list of jars
   */
  public static List<String> getJars(final Pattern pattern, String directory) {

    // create the filename filter
    FilenameFilter fileNameFilter = new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        Matcher m = pattern.matcher(name);
        return m.matches();
      }
    };

    // find the list of files
    File[] paths = new File(directory).listFiles(fileNameFilter);

    // return it is as a list of files
    ArrayList<String> files = new ArrayList<>();
    for (File path : paths) {
      files.add(path.toString());
    }

    return files;
  }

  /**
   * Get the class path from the list of jars that match the pattern
   * in the given directory
   *
   * @param pattern the pattern to match for jar files
   * @param directory the directory to search
   * @return class path of jars
   */
  public static String getClassPath(Pattern pattern, String directory) {
    List<String> jars = getJars(pattern, directory);

    StringBuilder sb = new StringBuilder();
    for (String s : jars) {
      sb.append(s).append(':');
    }
    sb.deleteCharAt(sb.length() - 1); // delete last colon

    return sb.toString();
  }

  /**
   * Given a directory, get the list of scheduler jars. A jar belongs to
   * scheduler, if it has the either the pattern "heron-scheduler*.jar" or
   * "heron-*-scheduler*.jar"
   *
   * @param directory the directory to search
   * @return list of scheduler jars
   */
  public static List<String> getSchedulerJars(String directory) {
    return getJars(schedulerJarPattern, directory);
  }

  /**
   * Get the scheduler class path from the list of scheduler jars in a
   * directory.
   *
   * @param directory the directory to search
   * @return class of scheduler jars
   */
  public static String getSchedulerClassPath(String directory) {
    return getClassPath(schedulerJarPattern, directory);
  }

  /**
   * Given a directory, get the list of metricsmgr jars. A jar belongs to
   * metrics, if it has the either the pattern "heron-metricsmgr*.jar" or
   * "heron-*-metricsmgr*.jar"
   *
   * @param directory the directory to search
   * @return list of metrics manager jars
   */
  public static List<String> getMetricsManagerJars(String directory) {
    return getJars(metricsManagerPattern, directory);
  }

  /**
   * Get the metrics mgr class path from the list of metrics manager jars in a
   * directory.
   *
   * @param directory the directory to search
   * @return class path of metrics manager jars
   */
  public static String getMetricsManagerClassPath(String directory) {
    return getClassPath(metricsManagerPattern, directory);
  }
}
