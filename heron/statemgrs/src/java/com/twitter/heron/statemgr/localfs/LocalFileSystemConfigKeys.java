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

package com.twitter.heron.statemgr.localfs;

import java.util.Map;

import com.twitter.heron.spi.common.Resource;

final class LocalFileSystemConfigKeys {

  // holds the mapping of keys to their corresponding key strings
  private static Map<String, Object> keys;

  // load the resource for config keys
  static {
    try {
      keys = Resource.load(
          "com.twitter.heron.statemgr.localfs.LocalFileSystemKeys",
          LocalFileSystemConstants.KEYS_YAML);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Unable to load the local file system uploader keys class " + e);
    }
  }

  private LocalFileSystemConfigKeys() {
  }

  /*
   * Get the key string value for the given key
   *
   * @param key, the key
   * @return String, the key string value for the key
   */
  public static String get(String key) {
    return (String) keys.get(key);
  }
}
