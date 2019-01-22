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

package org.apache.heron.uploader.localfs;

/**
 * Keys specific to the local FS uploader
 */
public enum LocalFileSystemKey {
  //key to specify the local file system directory for the uploader
  FILE_SYSTEM_DIRECTORY("heron.uploader.localfs.file.system.directory",
      "${HOME}/.herondata/repository/${CLUSTER}/${ROLE}/${TOPOLOGY}");

  private final String value;
  private final Object defaultValue;

  LocalFileSystemKey(String value) {
    this.value = value;
    this.defaultValue = null;
  }

  LocalFileSystemKey(String value, String defaultValue) {
    this.value = value;
    this.defaultValue = defaultValue;
  }

  public String value() {
    return value;
  }

  public String getDefaultString() {
    return (String) defaultValue;
  }
}
