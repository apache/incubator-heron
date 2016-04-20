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
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.filechooser.FileSystemView;

public class Misc {

    private static final Logger LOG = Logger.getLogger(Misc.class.getName());

    // Pattern to match an URL - just looks for double forward slashes //
    private static Pattern urlPattern = Pattern.compile("(.+)://(.+)");

    /**
     * Given a string representing heron home, substitute occurrences of
     * ${HERON_HOME} in the provided path.
     *
     * @param heronHome, string representing a path to heron home
     * @param pathString, string representing a path including ${HERON_HOME}
     * @return String, string that represents the modified path
     */
    public static String substitute(String heronHome, String pathString) {
        Config config = Config.newBuilder()
                .put(Keys.heronHome(), heronHome)
                .build();
        return substitute(config, pathString);
    }

    /**
     * Given strings representing heron home and heron conf, substitute occurrences of
     * ${HERON_HOME} and ${HERON_CONF} in the provided path.
     *
     * @param heronHome, string representing a path heron home
     * @param configPath, string representing a path to heron conf
     * @param pathString, string representing a path including ${HERON_HOME}/${HERON_CONF}
     * @return String, string that represents the modified path
     */
    public static String substitute(String heronHome, String configPath, String pathString) {
        Config config = Config.newBuilder()
                .put(Keys.heronHome(), heronHome)
                .put(Keys.heronConf(), configPath)
                .build();
        return substitute(config, pathString);
    }

    /**
     * Given a string representing heron sandbox home, substitute occurrences of
     * ${HERON_SANDBOX_HOME} in the provided path.
     *
     * @param heronSandboxHome, string representing a path to heron sandbox home
     * @param pathString, string representing a path including ${HERON_SANDBOX_HOME}
     * @return String, string that represents the modified path
     */
    public static String substituteSandbox(String heronSandboxHome, String pathString) {
        Config config = Config.newBuilder()
                .put(Keys.heronSandboxHome(), heronSandboxHome)
                .build();
        return substitute(config, pathString);
    }

    /**
     * Given strings representing heron home and heron conf, substitute occurrences of
     * ${HERON_SANDBOX_HOME} and ${HERON_SANDBOX_CONF} in the provided path.
     *
     * @param heronSandboxHome, string representing a path heron sandbox home
     * @param configPath, string representing a path to heron conf
     * @param pathString, string representing a path including ${HERON_SANDBOX_HOME}/${HERON_SANDBOX_CONF}
     * @return String, string that represents the modified path
     */
    public static String substituteSandbox(String heronSandboxHome, String configPath, String pathString) {
        Config config = Config.newBuilder()
                .put(Keys.heronSandboxHome(), heronSandboxHome)
                .put(Keys.heronSandboxConf(), configPath)
                .build();
        return substitute(config, pathString);
    }

    /**
     * Given a string, check if it is a URL - URL, according to our definition is
     * the presence of two consecutive forward slashes //
     *
     * @param pathString, string representing a path
     * @return true, if the pathString is a URL, else false
     */
    protected static final boolean isURL(String pathString) {
        Matcher m = urlPattern.matcher(pathString);
        return m.matches();
    }

    /**
     * Given a static config map, substitute occurrences of ${HERON_*} variables
     * in the provided URL
     *
     * @param config, a static map config object of key value pairs
     * @param pathString, string representing a path including ${HERON_*} variables
     * @return String, string that represents the modified path
     */
    private static String substituteURL(Config config, String pathString) {
        Matcher m = urlPattern.matcher(pathString);
        if (m.matches()) {
            StringBuilder sb = new StringBuilder();
            sb.append(m.group(1)).append(":").append("//").append(substitute(config, m.group(2)));
            return sb.toString();
        }
        return pathString;
    }

    /**
     * Given a static config map, substitute occurrences of ${HERON_*} variables
     * in the provided path string
     *
     * @param config, a static map config object of key value pairs
     * @param pathString, string representing a path including ${HERON_*} variables
     * @return String, string that represents the modified path
     */
    public static String substitute(Config config, String pathString) {

        // trim the leading and trailing spaces
        String trimmedPath = pathString.trim();

        if (isURL(trimmedPath))
            return substituteURL(config, trimmedPath);

        // get platform independent file separator
        String fileSeparator = Matcher.quoteReplacement(System.getProperty("file.separator"));

        // split the trimmed path into a list of components
        List<String> fixedList = Arrays.asList(trimmedPath.split(fileSeparator));
        List<String> list = new LinkedList<String>(fixedList);

        // get the home path
        String homePath = FileSystemView.getFileSystemView().getHomeDirectory().getAbsolutePath();

        // substitute various variables
        for (int i = 0; i < list.size(); i++) {
            String elem = list.get(i);

            if (elem.equals("${HOME}")) {
                list.set(i, homePath);

            } else if (elem.equals("~")) {
                list.set(i, homePath);

            } else if (elem.equals("${JAVA_HOME}")) {
                String javaPath = System.getenv("JAVA_HOME");
                if (javaPath != null) list.set(i, javaPath);

            } else if (elem.equals("${HERON_HOME}")) {
                list.set(i, Context.heronHome(config));

            } else if (elem.equals("${HERON_BIN}")) {
                list.set(i, Context.heronBin(config));

            } else if (elem.equals("${HERON_CONF}")) {
                list.set(i, Context.heronConf(config));

            } else if (elem.equals("${HERON_LIB}")) {
                list.set(i, Context.heronLib(config));

            } else if (elem.equals("${HERON_DIST}")) {
                list.set(i, Context.heronDist(config));

            } else if (elem.equals("${HERON_SANDBOX_HOME}")) {
                list.set(i, Context.heronSandboxHome(config));

            } else if (elem.equals("${HERON_SANDBOX_BIN}")) {
                list.set(i, Context.heronSandboxBin(config));

            } else if (elem.equals("${HERON_SANDBOX_CONF}")) {
                list.set(i, Context.heronSandboxConf(config));

            } else if (elem.equals("${HERON_SANDBOX_LIB}")) {
                list.set(i, Context.heronSandboxLib(config));

            } else if (elem.equals("${CLUSTER}")) {
                list.set(i, Context.cluster(config));

            } else if (elem.equals("${ROLE}")) {
                list.set(i, Context.role(config));

            } else if (elem.equals("${TOPOLOGY}")) {
                list.set(i, Context.topologyName(config));
            }
        }

        return combinePaths(list);
    }

    /**
     * Given a list of strings, concatenate them to form a file system
     * path
     *
     * @param paths, a list of strings to be included in the path
     * @return String, string that gives the file system path
     */
    protected static String combinePaths(List<String> paths) {
        File file = new File(paths.get(0));

        for (int i = 1; i < paths.size(); i++) {
            file = new File(file, paths.get(i));
        }

        return file.getPath();
    }
}
