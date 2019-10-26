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

import java.net.URI;
import java.net.URL;
import java.nio.file.Path;

/**
 * Used to download files via heron_downloader that has a URI prefix of "file://"
 * E.g. ./heron_downloader file:///foo/bar /path/location
 */
public class FileDownloader implements Downloader {
  @Override
  public void download(URI uri, Path destination) throws Exception {
    final URL url = uri.toURL();
    Extractor.extract(url.openStream(), destination);
  }
}
