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
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;

public final class Misc {

  private static final Logger LOG = Logger.getLogger(Misc.class.getName());

  // Pattern to match an URL - just looks for double forward slashes //
  private static final Pattern URL_PATTERN = Pattern.compile("(.+)://(.+)");

  private Misc() {
  }

  /**
   * Given a string, check if it is a URL - URL, according to our definition is
   * the presence of two consecutive forward slashes //
   *
   * @param pathString string representing a path
   * @return true if the pathString is a URL, else false
   */
  @VisibleForTesting
  static boolean isURL(String pathString) {
    Matcher m = URL_PATTERN.matcher(pathString);
    return m.matches();
  }

  /**
   * Given a static config map, substitute occurrences of ${HERON_*} variables
   * in the provided URL
   *
   * @param config a static map config object of key value pairs
   * @param pathString string representing a path including ${HERON_*} variables
   * @return String string that represents the modified path
   */
  private static String substituteURL(Config config, String pathString) {
    Matcher m = URL_PATTERN.matcher(pathString);
    if (m.matches()) {
      StringBuilder sb = new StringBuilder();
      sb.append(m.group(1)).append(":").append("//").append(substitute(config, m.group(2)));
      return sb.toString();
    }
    return pathString;
  }

  private static final Map<String, Key> SUBS = new HashMap<>();
  static {
    SUBS.put("${HERON_HOME}", Key.HERON_HOME);
    SUBS.put("${HERON_BIN}", Key.HERON_BIN);
    SUBS.put("${HERON_CONF}", Key.HERON_CONF);
    SUBS.put("${HERON_LIB}", Key.HERON_LIB);
    SUBS.put("${HERON_DIST}", Key.HERON_DIST);
    SUBS.put("${CLUSTER}", Key.CLUSTER);
    SUBS.put("${ROLE}", Key.ROLE);
    SUBS.put("${TOPOLOGY}", Key.TOPOLOGY_NAME);
    SUBS.put("${ENVIRON}", Key.ENVIRON);
  }

  /**
   * Given a static config map, substitute occurrences of ${HERON_*} variables
   * in the provided path string
   *
   * @param config a static map config object of key value pairs
   * @param pathString string representing a path including ${HERON_*} variables
   * @return String string that represents the modified path
   */
  public static String substitute(Config config, String pathString) {

    // trim the leading and trailing spaces
    String trimmedPath = pathString.trim();

    if (isURL(trimmedPath)) {
      return substituteURL(config, trimmedPath);
    }

    // get platform independent file separator
    String fileSeparator = Matcher.quoteReplacement(System.getProperty("file.separator"));

    // split the trimmed path into a list of components
    List<String> fixedList = Arrays.asList(trimmedPath.split(fileSeparator));
    List<String> list = new LinkedList<>(fixedList);

    // substitute various variables
    for (int i = 0; i < list.size(); i++) {
      String elem = list.get(i);

      if ("${HOME}".equals(elem) || "~".equals(elem)) {
        list.set(i, System.getProperty("user.home"));

      } else if ("${JAVA_HOME}".equals(elem)) {
        String javaPath = System.getenv("JAVA_HOME");
        if (javaPath != null) {
          list.set(i, javaPath);
        }
      } else if (SUBS.containsKey(elem)) {
        Key key = SUBS.get(elem);
        String value = config.getStringValue(key);
        if (value == null) {
          throw new IllegalArgumentException(String.format("Config value %s contains substitution "
              + "token %s but the corresponding config setting %s not found",
              pathString, elem, key.value()));
        }
        list.set(i, value);
      }
    }

    return combinePaths(list);
  }

  /**
   * Given a list of strings, concatenate them to form a file system
   * path
   *
   * @param paths a list of strings to be included in the path
   * @return String string that gives the file system path
   */
  private static String combinePaths(List<String> paths) {
    File file = new File(paths.get(0));

    for (int i = 1; i < paths.size(); i++) {
      file = new File(file, paths.get(i));
    }

    return file.getPath();
  }
}
