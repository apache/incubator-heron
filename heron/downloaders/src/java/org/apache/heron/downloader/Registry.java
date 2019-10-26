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

import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.Map;

import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Key;

final class Registry {

  private Registry() { }

  public static Class<? extends Downloader> UriToClass(Config config, URI uri) throws Exception {
    final String scheme = uri.getScheme().toLowerCase();
    Map<String, Object> yamlConfig = (Map<String, Object>) config.get(Key.DOWNLOADER_PROTOCOLS);
    if (!yamlConfig.containsKey(scheme)) {
      throw new RuntimeException(
          String.format("Unable to create downloader unsupported uri %s", uri.toString()));
    }
    Class clazz = Class.forName((String) yamlConfig.get(scheme));
    return clazz;
  }

  public static Downloader getDownloader(
      Class<? extends Downloader> downloaderClass, URI uri) throws Exception {
    try {
      return downloaderClass.getConstructor().newInstance();
    } catch (InstantiationException | IllegalAccessException
        | InvocationTargetException | NoSuchMethodException e) {
      final String message =
            String.format("Unable to create downloader for uri %s", uri.toString());
      throw new Exception(message, e);
    }
  }
}
