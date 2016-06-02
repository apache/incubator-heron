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

package com.twitter.heron.spouts.kafka.common;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.twitter.heron.api.tuple.Values;

@SuppressWarnings("serial")
public class StringKeyValueScheme extends StringScheme implements KeyValueScheme {

  @Override
  public List<Object> deserializeKeyAndValue(byte[] key, byte[] value) {
    if (key == null) {
      return deserialize(value);
    }
    String keyString = StringScheme.deserializeString(key);
    String valueString = StringScheme.deserializeString(value);
    Map<String, String> kvMap = new HashMap<>();
    kvMap.put(keyString, valueString);
    return new Values(kvMap);
  }

  @Override
  public List<Object> deserialize(byte[] byteBuffer) {
    return null;
  }
}
