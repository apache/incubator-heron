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

package com.twitter.heron.ckptmgr;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

import com.twitter.heron.common.basics.SysUtils;

public class CheckpointManagerConfig {
  public static final String CONFIG_KEY_STATEFUL_BACKEND = "stateful-backend";
  public static final String CONFIG_KEY_CLASSNAME = "class";
  public static final String CONFIG_KEY_CLASSPATH = "classpath";

  private final Map<String, Object> backendConfig = new HashMap<>();

  @SuppressWarnings("unchecked")
  public CheckpointManagerConfig(String filename) throws FileNotFoundException {
    FileInputStream fin = new FileInputStream(new File(filename));
    try {
      Yaml yaml = new Yaml();
      Map<String, Object> ret = (Map<String, Object>) yaml.load(fin);

      if (ret == null) {
        throw new RuntimeException("Could not parse stateful backend config file");
      } else {
        String backend = (String) ret.get(CONFIG_KEY_STATEFUL_BACKEND);
        backendConfig.putAll((Map<String, Object>) ret.get(backend));
      }
    } finally {
      SysUtils.closeIgnoringExceptions(fin);
    }
  }

  public void setConfig(String key, Object value) {
    backendConfig.put(key, value);
  }

  @Override
  public String toString() {
    return backendConfig.toString();
  }

  public Object get(String key) {
    return backendConfig.get(key);
  }

  public Map<String, Object> getBackendConfig() {
    return backendConfig;
  }
}
