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

package org.apache.heron.dlog;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.distributedlog.AppendOnlyStreamWriter;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.heron.common.basics.Pair;

/**
 * A utility program to copy data between filesystem files and dlog streams.
 */
public final class Util {

  private Util() {
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage : Util <src> <target>");
      System.err.println("");
      System.err.println("NOTE: <src>/<target> can be either a file path or a dlog stream");
      Runtime.getRuntime().exit(-1);
      return;
    }

    String srcPath = args[0];
    String destPath = args[1];

    Namespace srcNs = null;
    Namespace destNs = null;
    InputStream is = null;
    OutputStream os = null;

    try {

      if (srcPath.startsWith("distributedlog")) {
        URI srcUri = URI.create(srcPath);
        Pair<URI, String> parentAndName = getParentURI(srcUri);
        srcNs = openNamespace(parentAndName.first);
        is = openInputStream(srcNs, parentAndName.second);
      } else {
        is = new FileInputStream(destPath);
      }

      if (destPath.startsWith("distributedlog")) {
        URI destUri = URI.create(srcPath);
        Pair<URI, String> parentAndName = getParentURI(destUri);
        destNs = openNamespace(parentAndName.first);
        os = openOutputStream(destNs, parentAndName.second);
      } else {
        os = new FileOutputStream(destPath);
      }

      copyStream(is, os);
    } finally {
      if (null != is) {
        is.close();
      }
      if (null != os) {
        os.close();
      }
      if (null != srcNs) {
        srcNs.close();
      }
      if (null != destNs) {
        destNs.close();
      }
    }
  }

  static void copyStream(InputStream in, OutputStream out) throws IOException {
    int read = 0;
    byte[] bytes = new byte[128 * 1024];
    while ((read = in.read(bytes)) >= 0) {
      if (0 == read) {
        continue;
      }
      out.write(bytes, 0, read);
    }
    out.flush();
    out.close();
  }

  private static Pair<URI, String> getParentURI(URI uri) throws URISyntaxException {
    String path = uri.getPath();
    File pathFile = new File(path);
    String logName = pathFile.getName();
    String parentName = pathFile.getParent();
    return Pair.create(
        new URI(
          uri.getScheme(),
          uri.getAuthority(),
          parentName,
          uri.getQuery(),
          uri.getFragment()
        ), logName);
  }

  private static Namespace openNamespace(URI uri) throws IOException {
    DistributedLogConfiguration distributedLogConfiguration = new DistributedLogConfiguration();
    distributedLogConfiguration.addProperty("bkc.allowShadedLedgerManagerFactoryClass", true);
    return NamespaceBuilder.newBuilder()
        .uri(uri)
        .clientId("dlog-util")
        .conf(distributedLogConfiguration)
        .build();
  }

  private static OutputStream openOutputStream(Namespace namespace, String pkgName)
      throws IOException {
    DistributedLogManager dlm = namespace.openLog(pkgName);
    AppendOnlyStreamWriter writer = dlm.getAppendOnlyStreamWriter();
    return new DLOutputStream(dlm, writer);
  }

  private static InputStream openInputStream(Namespace ns, String logName)
      throws Exception {
    DistributedLogManager dlm = ns.openLog(logName);
    return new DLInputStream(dlm);
  }

}
