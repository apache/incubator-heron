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

package org.apache.heron.downloader;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.util.function.Supplier;

import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.heron.dlog.DLInputStream;

public class DLDownloader implements Downloader {

  static final DistributedLogConfiguration CONF = new DistributedLogConfiguration()
      .setUseDaemonThread(true);                        // use daemon thread

  static InputStream openInputStream(Namespace ns, String logName)
      throws Exception {
    DistributedLogManager dlm = ns.openLog(logName);
    return new DLInputStream(dlm);
  }

  private final NamespaceBuilder builder;

  public DLDownloader() {
    this(() -> NamespaceBuilder.newBuilder());
  }

  public DLDownloader(Supplier<NamespaceBuilder> builderSupplier) {
    this.builder = builderSupplier.get();
  }

  @Override
  public void download(URI uri, Path destination) throws Exception {
    String path = uri.getPath();
    File pathFile = new File(path);
    String logName = pathFile.getName();
    String parentName = pathFile.getParent();
    URI parentUri = new URI(
        uri.getScheme(),
        uri.getAuthority(),
        parentName,
        uri.getQuery(),
        uri.getFragment());

    CONF.addProperty("bkc.allowShadedLedgerManagerFactoryClass", true);

    Namespace ns = builder
        .clientId("heron-downloader")
        .conf(CONF)
        .uri(parentUri)
        .build();
    try {
      // open input stream
      InputStream in = openInputStream(ns, logName);
      Extractor.extract(in, destination);
    } finally {
      ns.close();
    }
  }
}
