//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package com.twitter.heron.statemgr.localfs;

/**
 * Keys specific to the local FS state manager
 */
public enum LocalFileSystemKey {
  // config key to initialize file directory hierarchy
  IS_INITIALIZE_FILE_TREE("heron.statemgr.localfs.is.initialize.file.tree");

  private String value;

  LocalFileSystemKey(String value) {
    this.value = value;
  }

  public String value() {
    return value;
  }
}
