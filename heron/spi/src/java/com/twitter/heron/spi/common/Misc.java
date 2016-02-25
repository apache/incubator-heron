package com.twitter.heron.spi.common;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import java.util.Arrays;
import java.util.List;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.Collections;
import javax.swing.filechooser.FileSystemView;

public class Misc {

  private static final Logger LOG = Logger.getLogger(Misc.class.getName());

  public static String substitute(String heronHome, String pathString) {
    Config config = Config.newBuilder()
      .put(Keys.HERON_HOME, heronHome)
      .build();

    return substitute(config, pathString);
  }

  public static String substitute(Config config, String pathString) {

    // trim the leading and trailing spaces
    String trimmedPath = pathString.trim();

    // get platform independent file separator
    String fileSeparator = Matcher.quoteReplacement(System.getProperty("file.separator"));

    // split the trimmed path into a list of components
    List<String> fixedList = Arrays.asList(trimmedPath.split(fileSeparator));
    List<String> list = new LinkedList<String>(fixedList);
    list.removeAll(Collections.singleton(""));

    // get the home path
    String hp = FileSystemView.getFileSystemView().getHomeDirectory().getAbsolutePath();
    String homePath = hp.substring(1);

    // If ${HOME} or ~ is specified, substitute them with home path
    for (int i = 0 ; i < list.size(); i++) {
      String elem = list.get(i);

      if (elem.equals("${HOME}")) {
        list.set(i, homePath);
      } else if (elem.equals("~")) {
        list.set(i, homePath);
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
      } else if (elem.equals("${CLUSTER}")) {
        list.set(i, Context.cluster(config));
      } else if (elem.equals("${ROLE}")) {
        list.set(i, Context.role(config));
      } else if (elem.equals("${TOPOLOGY}")) {
        list.set(i, Context.topologyName(config));
      }
    }

    StringBuilder sb = new StringBuilder();
    for (String s: list) {
      sb.append(System.getProperty("file.separator"));
      sb.append(s);
    }
    return sb.toString();
  }
}
